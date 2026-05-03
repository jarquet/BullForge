"""Hermetic tests for shared.exclusion.

Mirrors the style of tests/test_trading_window.py — UTC inputs, ET-aware
expectations, no DB. The dates are chosen to match those file's reference
points: 2026-04-11 = Saturday, 2026-04-13 = Monday.

EDT (UTC-4) is in effect for these April dates, so:
    09:30 ET = 13:30 UTC, 10:30 ET = 14:30 UTC, 16:00 ET = 20:00 UTC,
    15:00 ET = 19:00 UTC (= close - 60-min buffer).
"""
from datetime import datetime, timezone

from shared.exclusion import is_evaluation_window_in_exclusion


def _dt(year, month, day, hour, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def test_equity_1m_target_in_open_buffer_excluded():
    # feature 13:25 UTC (09:25 ET), horizon 10m → target 13:35 UTC (09:35 ET).
    # 09:35 ET is inside the 60-min open buffer (window opens 10:30 ET).
    excluded = is_evaluation_window_in_exclusion(
        "SPY", _dt(2026, 4, 13, 13, 25), horizon_minutes=10,
        open_buffer_min=60, close_buffer_min=60,
    )
    assert excluded is True


def test_equity_1m_target_midday_survives():
    # feature 14:50 UTC (10:50 ET), horizon 10m → target 15:00 UTC (11:00 ET).
    excluded = is_evaluation_window_in_exclusion(
        "SPY", _dt(2026, 4, 13, 14, 50), horizon_minutes=10,
        open_buffer_min=60, close_buffer_min=60,
    )
    assert excluded is False


def test_equity_1m_target_in_close_buffer_excluded():
    # feature 19:20 UTC (15:20 ET), horizon 10m → target 19:30 UTC (15:30 ET).
    # 15:30 ET is past the 15:00 ET close-buffer end → excluded.
    excluded = is_evaluation_window_in_exclusion(
        "SPY", _dt(2026, 4, 13, 19, 20), horizon_minutes=10,
        open_buffer_min=60, close_buffer_min=60,
    )
    assert excluded is True


def test_equity_1d_target_on_saturday_excluded():
    # feature Friday 2026-04-10 14:00 UTC (10:00 ET), horizon 1440m → target
    # Saturday 2026-04-11 14:00 UTC. is_trading_allowed rejects weekends
    # naturally → excluded.
    excluded = is_evaluation_window_in_exclusion(
        "SPY", _dt(2026, 4, 10, 14, 0), horizon_minutes=1440,
        open_buffer_min=60, close_buffer_min=60,
    )
    assert excluded is True


def test_equity_1d_target_midweek_survives():
    # feature Monday 17:00 UTC (13:00 ET), horizon 1440m → target Tuesday
    # 17:00 UTC (13:00 ET). Midweek midday, well inside the buffered window.
    excluded = is_evaluation_window_in_exclusion(
        "SPY", _dt(2026, 4, 13, 17, 0), horizon_minutes=1440,
        open_buffer_min=60, close_buffer_min=60,
    )
    assert excluded is False


def test_crypto_never_excluded_weekend():
    # Saturday 03:00 UTC for BTC/USD with any horizon → not excluded.
    excluded = is_evaluation_window_in_exclusion(
        "BTC/USD", _dt(2026, 4, 11, 3, 0), horizon_minutes=10,
        open_buffer_min=60, close_buffer_min=60,
    )
    assert excluded is False


def test_crypto_never_excluded_target_inside_equity_buffer():
    # feature 13:25 UTC (would be inside open buffer for equity), horizon 10m,
    # symbol = ETH/USD → crypto bypasses → not excluded.
    excluded = is_evaluation_window_in_exclusion(
        "ETH/USD", _dt(2026, 4, 13, 13, 25), horizon_minutes=10,
        open_buffer_min=60, close_buffer_min=60,
    )
    assert excluded is False


def test_boundary_target_at_open_plus_buffer_inclusive():
    # feature 14:20 UTC (10:20 ET), horizon 10m → target 14:30 UTC (10:30 ET).
    # 10:30 ET is exactly open + 60min — `<=` semantics in trading_window.py:45
    # mean this is allowed → not excluded.
    excluded = is_evaluation_window_in_exclusion(
        "SPY", _dt(2026, 4, 13, 14, 20), horizon_minutes=10,
        open_buffer_min=60, close_buffer_min=60,
    )
    assert excluded is False


def test_boundary_target_at_close_minus_buffer_excluded():
    # feature 18:50 UTC (14:50 ET), horizon 10m → target 19:00 UTC (15:00 ET).
    # 15:00 ET is exactly close - 60min — `<` semantics in trading_window.py:45
    # mean this is rejected → excluded.
    excluded = is_evaluation_window_in_exclusion(
        "SPY", _dt(2026, 4, 13, 18, 50), horizon_minutes=10,
        open_buffer_min=60, close_buffer_min=60,
    )
    assert excluded is True


def test_zero_buffers_still_exclude_weekends():
    # Crypto-style 24/7 buffers (open=0, close=0) for an equity 1d target
    # on Saturday → still excluded because is_trading_allowed rejects
    # weekends regardless of buffer values.
    excluded = is_evaluation_window_in_exclusion(
        "SPY", _dt(2026, 4, 10, 14, 0), horizon_minutes=1440,
        open_buffer_min=0, close_buffer_min=0,
    )
    assert excluded is True
