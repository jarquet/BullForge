from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import pytest

from services.model_service.meta.regime import RegimeBuilder


def _spy_df(n_bars=200):
    base = datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc)
    closes = [400.0 + 0.01 * i for i in range(n_bars)]  # gentle uptrend
    return pd.DataFrame({
        "timestamp": [base + timedelta(minutes=i) for i in range(n_bars)],
        "close": closes,
    }).set_index("timestamp")


class TestRegimeBuilder:
    def test_all_fields_populated_at_recent_timestamp(self):
        spy = _spy_df()
        rb = RegimeBuilder(spy_bars=spy, max_staleness_hours=48)
        ts = spy.index[-1]
        regime = rb.regime_at(ts)
        for key in ["spy_trend_1m", "spy_trend_1h", "spy_trend_1d",
                    "spy_momentum", "vol_regime"]:
            assert key in regime
            assert not pd.isna(regime[key])

    def test_uses_last_known_when_ts_after_last_bar(self):
        spy = _spy_df()
        rb = RegimeBuilder(spy_bars=spy, max_staleness_hours=48)
        # Query 30 minutes after the last bar — within staleness
        ts = spy.index[-1] + timedelta(minutes=30)
        regime = rb.regime_at(ts)
        assert not pd.isna(regime["spy_trend_1m"])

    def test_returns_zeros_when_stale_beyond_cap(self):
        spy = _spy_df()
        rb = RegimeBuilder(spy_bars=spy, max_staleness_hours=1)
        ts = spy.index[-1] + timedelta(hours=5)
        regime = rb.regime_at(ts)
        for key in ["spy_trend_1m", "spy_trend_1h", "spy_trend_1d",
                    "spy_momentum", "vol_regime"]:
            assert regime[key] == 0.0

    def test_returns_zeros_when_spy_empty(self):
        rb = RegimeBuilder(spy_bars=pd.DataFrame(columns=["close"]),
                           max_staleness_hours=48)
        ts = datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc)
        regime = rb.regime_at(ts)
        assert all(v == 0.0 for v in regime.values())


class TestAppendBar:
    """RegimeBuilder.append_bar keeps the in-memory frame fresh from
    market:ticks SPY events. Without this, model_service loads SPY once
    at startup and after meta_regime_max_staleness_hours (default 48h)
    regime_at(now) silently returns zeros for every regime feature —
    corrupting meta_features.feature_vector rows for everything past
    day 2 of the deploy.
    """

    def test_append_bar_rescues_stale_regime(self):
        """End-to-end: stale dataframe -> regime_at returns zeros.
        Append a fresh bar -> regime_at now returns live values."""
        spy = _spy_df()
        rb = RegimeBuilder(spy_bars=spy, max_staleness_hours=48)
        # Walk forward 60 hours past the last bar — well beyond 48h cap.
        now = spy.index[-1] + timedelta(hours=60)
        stale = rb.regime_at(now)
        assert all(v == 0.0 for v in stale.values()), (
            "precondition: regime should be zeroed once stale beyond cap"
        )

        # Append a fresh SPY bar at 'now' and confirm regime unzeros.
        rb.append_bar(now, close=420.0)
        fresh = rb.regime_at(now)
        # spy_trend_1m must be a real (non-zero) pct change vs. the
        # most recent prior bar (close=400.01 + ...) — the gentle
        # uptrend in _spy_df leaves the prior close well below 420.0.
        assert fresh["spy_trend_1m"] != 0.0
        assert fresh["spy_trend_1h"] != 0.0
        assert fresh["spy_trend_1d"] != 0.0

    def test_append_bar_rejects_naive_datetime(self):
        rb = RegimeBuilder(spy_bars=_spy_df(), max_staleness_hours=48)
        naive = datetime(2026, 4, 30, 18, 0)  # no tzinfo
        with pytest.raises(ValueError):
            rb.append_bar(naive, close=400.0)

    def test_append_bar_rejects_non_utc_datetime(self):
        from datetime import timezone as tz
        rb = RegimeBuilder(spy_bars=_spy_df(), max_staleness_hours=48)
        non_utc = datetime(2026, 4, 30, 18, 0, tzinfo=tz(timedelta(hours=5)))
        with pytest.raises(ValueError):
            rb.append_bar(non_utc, close=400.0)

    def test_append_bar_idempotent_on_duplicate_timestamp(self):
        """A re-published market:ticks event must not corrupt the index
        with duplicates. We accept either dedup-by-timestamp OR
        no-op-on-existing — both keep len() bounded by unique timestamps.
        """
        spy = _spy_df()
        rb = RegimeBuilder(spy_bars=spy, max_staleness_hours=48)
        ts = spy.index[-1] + timedelta(minutes=1)
        rb.append_bar(ts, close=420.0)
        before_len = len(rb._df)
        # Re-publish the same tick a few times.
        rb.append_bar(ts, close=420.0)
        rb.append_bar(ts, close=421.0)  # different close on same ts
        after_len = len(rb._df)
        assert after_len == before_len, (
            "duplicate timestamps must not grow the frame"
        )
        # Index must remain monotonic increasing & unique for searchsorted.
        assert rb._df.index.is_monotonic_increasing
        assert rb._df.index.is_unique

    def test_append_bar_updates_in_memory_state_for_query(self):
        """Direct check that the new tail row is reflected in regime_at:
        the new (timestamp, close) is what the asof lookup returns."""
        spy = _spy_df()
        rb = RegimeBuilder(spy_bars=spy, max_staleness_hours=48)
        # 30 minutes after the last bar, well within staleness.
        ts_new = spy.index[-1] + timedelta(minutes=30)
        rb.append_bar(ts_new, close=500.0)  # large jump up
        regime = rb.regime_at(ts_new)
        # spy_trend_1m is the pct_change from the previous bar's close
        # to 500.0 — must be strongly positive (prior closes ~402).
        assert regime["spy_trend_1m"] > 0.1

    def test_append_bar_sorts_out_of_order_arrivals(self):
        """Websocket reconnect/replay can deliver a market:ticks event
        with a timestamp earlier than the current tail. Without an explicit
        sort the index goes non-monotonic and regime_at's searchsorted
        silently lands on the wrong row. sort_index() after concat is the
        guard."""
        rb = RegimeBuilder(
            spy_bars=pd.DataFrame(columns=["close"]),
            max_staleness_hours=48,
        )
        t1 = datetime(2026, 4, 30, 14, 1, tzinfo=timezone.utc)
        t0 = datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc)
        rb.append_bar(t1, close=400.5)
        rb.append_bar(t0, close=400.0)
        assert rb._df.index.is_monotonic_increasing
        assert rb._df.index.is_unique

    def test_append_bar_to_empty_frame(self):
        """Cold start: builder constructed with an empty frame must
        accept the first append_bar without exploding."""
        rb = RegimeBuilder(
            spy_bars=pd.DataFrame(columns=["close"]),
            max_staleness_hours=48,
        )
        ts = datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc)
        rb.append_bar(ts, close=400.0)
        assert len(rb._df) == 1
