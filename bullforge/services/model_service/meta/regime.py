from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd


_FIELDS = ("spy_trend_1m", "spy_trend_1h", "spy_trend_1d",
           "spy_momentum", "vol_regime")


class RegimeBuilder:
    """Computes SPY-derived regime context with forward-fill staleness cap.

    `spy_bars` is a DataFrame indexed by UTC timestamp with at least a
    `close` column. Pre-compute `_trend_1m` / `_vol_regime` columns on
    construction so per-timestamp lookups are O(log n).
    """

    def __init__(self, spy_bars: pd.DataFrame, max_staleness_hours: int):
        self._max_stale = timedelta(hours=max_staleness_hours)
        self._df = self._enrich(spy_bars)

    @staticmethod
    def _enrich(spy: pd.DataFrame) -> pd.DataFrame:
        if spy.empty:
            return spy
        df = spy.copy()
        df["trend_1m"] = df["close"].pct_change(1)
        df["momentum"] = df["trend_1m"].ewm(span=12, adjust=False).mean()
        vol60 = df["trend_1m"].rolling(60).std()
        median30d = vol60.rolling(30 * 24 * 60, min_periods=60).median()
        df["vol_regime"] = vol60 / median30d.replace(0.0, np.nan)
        return df

    def regime_at(self, ts: datetime) -> dict[str, float]:
        zeros = {k: 0.0 for k in _FIELDS}
        if self._df.empty:
            return zeros

        # asof — find the most recent SPY bar at or before ts
        idx = self._df.index.searchsorted(ts, side="right") - 1
        if idx < 0:
            return zeros
        bar_ts = self._df.index[idx]
        if ts - bar_ts > self._max_stale:
            return zeros

        row = self._df.iloc[idx]

        def _hr(minutes: int) -> float:
            target = ts - timedelta(minutes=minutes)
            j = self._df.index.searchsorted(target, side="right") - 1
            if j < 0:
                return 0.0
            past = float(self._df.iloc[j]["close"])
            now = float(row["close"])
            return (now - past) / past if past else 0.0

        return {
            "spy_trend_1m": float(row["trend_1m"]) if not pd.isna(row["trend_1m"]) else 0.0,
            "spy_trend_1h": _hr(60),
            "spy_trend_1d": _hr(60 * 24),
            "spy_momentum": float(row["momentum"]) if not pd.isna(row["momentum"]) else 0.0,
            "vol_regime": float(row["vol_regime"]) if not pd.isna(row["vol_regime"]) else 0.0,
        }
