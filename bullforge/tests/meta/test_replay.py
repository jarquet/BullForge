from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import pytest

from services.model_service.meta.replay import (
    PairSimulator,
    SimResult,
    compute_atr_pct,
)


def _bar_df(closes, highs=None, lows=None, start_min=0):
    """Build a synthetic 1m bars dataframe."""
    n = len(closes)
    if highs is None:
        highs = [c * 1.001 for c in closes]
    if lows is None:
        lows = [c * 0.999 for c in closes]
    base = datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc)
    return pd.DataFrame({
        "timestamp": [base + timedelta(minutes=i + start_min) for i in range(n)],
        "open": closes,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": [1000.0] * n,
    })


class TestPairSimulator:
    def test_long_target_hits_first(self):
        # entry @ 100, target +1% = 101, stop -0.5% = 99.5
        # bar 1 high=101.5 -> target hit
        bars = _bar_df([100, 101, 101], highs=[100.2, 101.5, 101.5], lows=[99.8, 100.5, 100.5])
        sim = PairSimulator()
        res = sim.simulate(bars, entry_idx=0, direction=1,
                           target_pct=0.01, stop_pct=0.005, max_hold_bars=10)
        assert res.hit_target is True
        assert res.hit_stop is False
        assert res.exit_idx == 1
        assert res.realized_return == pytest.approx(0.01)

    def test_long_stop_hits_first(self):
        bars = _bar_df([100, 99, 99], highs=[100.1, 99.2, 99.2], lows=[99.9, 99.4, 99.4])
        sim = PairSimulator()
        res = sim.simulate(bars, entry_idx=0, direction=1,
                           target_pct=0.01, stop_pct=0.005, max_hold_bars=10)
        assert res.hit_target is False
        assert res.hit_stop is True
        assert res.realized_return == pytest.approx(-0.005)

    def test_long_timeout_with_positive_return(self):
        bars = _bar_df([100, 100.2, 100.4], highs=[100.1, 100.3, 100.5], lows=[99.9, 100.1, 100.3])
        sim = PairSimulator()
        res = sim.simulate(bars, entry_idx=0, direction=1,
                           target_pct=0.01, stop_pct=0.005, max_hold_bars=2)
        assert res.hit_target is False
        assert res.hit_stop is False
        assert res.realized_return == pytest.approx((100.4 - 100) / 100, rel=1e-6)
        assert res.exit_idx == 2

    def test_long_both_hit_same_bar_stop_wins(self):
        # bar 1 has both extremes - stop must win (conservative)
        bars = _bar_df([100, 100], highs=[100.1, 102], lows=[99.9, 99])
        sim = PairSimulator()
        res = sim.simulate(bars, entry_idx=0, direction=1,
                           target_pct=0.01, stop_pct=0.005, max_hold_bars=10)
        assert res.hit_stop is True
        assert res.hit_target is False

    def test_short_target_hits_first(self):
        bars = _bar_df([100, 99, 99], highs=[100.1, 99.2, 99.2], lows=[99.9, 98.5, 98.5])
        sim = PairSimulator()
        res = sim.simulate(bars, entry_idx=0, direction=-1,
                           target_pct=0.01, stop_pct=0.005, max_hold_bars=10)
        assert res.hit_target is True
        assert res.realized_return == pytest.approx(0.01)

    def test_short_stop_hits_first(self):
        bars = _bar_df([100, 101, 101], highs=[100.1, 101.5, 101.5], lows=[99.9, 100.5, 100.5])
        sim = PairSimulator()
        res = sim.simulate(bars, entry_idx=0, direction=-1,
                           target_pct=0.01, stop_pct=0.005, max_hold_bars=10)
        assert res.hit_stop is True
        assert res.realized_return == pytest.approx(-0.005)

    def test_insufficient_bars_returns_zero(self):
        bars = _bar_df([100])  # only entry bar, no follow-ups
        sim = PairSimulator()
        res = sim.simulate(bars, entry_idx=0, direction=1,
                           target_pct=0.01, stop_pct=0.005, max_hold_bars=5)
        assert res.hit_target is False
        assert res.hit_stop is False
        assert res.realized_return == 0.0
        assert res.exit_idx == 0


def test_compute_atr_pct():
    # ATR-14 needs at least 15 bars of high/low/close
    closes = list(range(100, 130))  # 30 bars
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    bars = _bar_df(closes, highs=highs, lows=lows)
    atr_pct = compute_atr_pct(bars, idx=20, period=14)
    # high-low spread = 2; close ~120; expected ATR%/close ≈ 2/120 ≈ 0.0167
    assert 0.01 < atr_pct < 0.025


def test_compute_atr_pct_warmup_returns_nan():
    bars = _bar_df([100] * 5)
    atr_pct = compute_atr_pct(bars, idx=2, period=14)
    assert pd.isna(atr_pct)
