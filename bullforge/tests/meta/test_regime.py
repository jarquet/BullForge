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
