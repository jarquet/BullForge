from datetime import datetime, timezone

from shared.market_hours import is_market_open, last_market_close


def _utc(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def test_open_during_edt_trading_hours():
    # Monday 2026-04-20; 14:30 UTC = 10:30 EDT (inside 09:30–16:00 ET window)
    assert is_market_open(_utc("2026-04-20T14:30:00")) is True


def test_open_during_est_trading_hours():
    # Monday 2026-02-02; 16:00 UTC = 11:00 EST
    assert is_market_open(_utc("2026-02-02T16:00:00")) is True


def test_closed_before_open_bell():
    # Monday, 13:00 UTC = 09:00 EDT (30m before open)
    assert is_market_open(_utc("2026-04-20T13:00:00")) is False


def test_closed_after_close_bell():
    # Monday, 20:30 UTC = 16:30 EDT (30m after close)
    assert is_market_open(_utc("2026-04-20T20:30:00")) is False


def test_closed_weekend():
    # Saturday noon UTC
    assert is_market_open(_utc("2026-04-18T12:00:00")) is False


def test_closed_christmas():
    # 2026-12-25 Friday; NYSE closed
    assert is_market_open(_utc("2026-12-25T15:00:00")) is False


def test_last_close_after_today_close():
    # Monday 21:00 UTC — today's close (20:00 UTC EDT) has happened
    close = last_market_close(_utc("2026-04-20T21:00:00"))
    assert close is not None
    assert close.date() == datetime(2026, 4, 20).date()
    assert close.hour == 20  # 16:00 EDT == 20:00 UTC


def test_last_close_during_trading_returns_prior_close():
    # Monday 15:00 UTC mid-session; prior close was Friday 2026-04-17
    close = last_market_close(_utc("2026-04-20T15:00:00"))
    assert close is not None
    assert close.date() == datetime(2026, 4, 17).date()
