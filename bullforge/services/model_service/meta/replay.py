from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SimResult:
    hit_target: bool
    hit_stop: bool
    exit_idx: int
    realized_return: float


class PairSimulator:
    """Walks bars forward from `entry_idx`, returns first-touch outcome.

    Stop wins on same-bar collisions (conservative; matches typical
    intra-bar stop-fill behavior).
    """

    def simulate(
        self,
        bars: pd.DataFrame,
        *,
        entry_idx: int,
        direction: int,
        target_pct: float,
        stop_pct: float,
        max_hold_bars: int,
    ) -> SimResult:
        if entry_idx + 1 >= len(bars):
            return SimResult(False, False, entry_idx, 0.0)

        entry_price = float(bars.iloc[entry_idx]["close"])
        if direction == 1:
            target_price = entry_price * (1.0 + target_pct)
            stop_price = entry_price * (1.0 - stop_pct)
        else:
            target_price = entry_price * (1.0 - target_pct)
            stop_price = entry_price * (1.0 + stop_pct)

        last_idx = min(entry_idx + max_hold_bars, len(bars) - 1)
        for i in range(entry_idx + 1, last_idx + 1):
            high = float(bars.iloc[i]["high"])
            low = float(bars.iloc[i]["low"])

            if direction == 1:
                hit_stop = low <= stop_price
                hit_target = high >= target_price
            else:
                hit_stop = high >= stop_price
                hit_target = low <= target_price

            if hit_stop:
                return SimResult(False, True, i, -stop_pct)
            if hit_target:
                return SimResult(True, False, i, target_pct)

        # Timeout: exit at last considered bar's close
        exit_close = float(bars.iloc[last_idx]["close"])
        realized = ((exit_close - entry_price) / entry_price) * direction
        return SimResult(False, False, last_idx, float(realized))


def compute_atr_pct(
    bars: pd.DataFrame, *, idx: int, period: int = 14,
) -> float:
    """ATR-period at row `idx`, expressed as fraction of that row's close.

    Returns NaN if insufficient prior bars exist for the rolling mean.
    """
    if idx < period:
        return float("nan")
    sub = bars.iloc[: idx + 1]
    high = sub["high"]
    low = sub["low"]
    close = sub["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean().iloc[-1]
    if pd.isna(atr) or close.iloc[-1] == 0:
        return float("nan")
    return float(atr / close.iloc[-1])
