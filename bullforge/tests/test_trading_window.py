from datetime import datetime, timezone

import pytest

from shared.trading_window import is_trading_allowed


def _dt(year, month, day, hour, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def test_crypto_always_allowed_weekend():
    ok, reason = is_trading_allowed(
        "BTC/USD", _dt(2026, 4, 12, 3, 0),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is True
    assert "crypto" in reason.lower()


def test_crypto_always_allowed_weekday_midnight():
    ok, _ = is_trading_allowed(
        "ETH/USD", _dt(2026, 4, 15, 0, 0),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is True


def test_equity_rejected_on_weekend():
    ok, reason = is_trading_allowed(
        "SPY", _dt(2026, 4, 11, 16, 0),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is False
    assert "weekend" in reason.lower()


def test_equity_rejected_before_buffer():
    ok, reason = is_trading_allowed(
        "SPY", _dt(2026, 4, 13, 14, 0),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is False
    assert "outside" in reason.lower()


def test_equity_allowed_midday():
    ok, _ = is_trading_allowed(
        "SPY", _dt(2026, 4, 13, 16, 0),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is True


def test_equity_rejected_inside_close_buffer():
    ok, _ = is_trading_allowed(
        "SPY", _dt(2026, 4, 13, 19, 30),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is False


def test_equity_rejected_after_close():
    ok, _ = is_trading_allowed(
        "SPY", _dt(2026, 4, 13, 21, 0),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is False


def test_equity_allowed_at_window_open_boundary():
    ok, _ = is_trading_allowed(
        "SPY", _dt(2026, 4, 13, 14, 30),
        open_buffer_min=60, close_buffer_min=60,
    )
    assert ok is True


def test_equity_allowed_with_zero_buffer():
    ok, _ = is_trading_allowed(
        "SPY", _dt(2026, 4, 13, 13, 30),
        open_buffer_min=0, close_buffer_min=0,
    )
    assert ok is True


def test_equity_naive_datetime_rejected():
    with pytest.raises(ValueError):
        is_trading_allowed(
            "SPY", datetime(2026, 4, 13, 14, 0),
            open_buffer_min=60, close_buffer_min=60,
        )
