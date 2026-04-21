from datetime import datetime, timedelta, timezone

from services.dashboard_service.freshness_api import _bar_color


def _utc(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def test_crypto_green_when_fresh():
    now = _utc("2026-04-20T20:00:00")
    bar = now - timedelta(seconds=60)
    assert _bar_color("BTC/USD", bar, now, market_open_now=False, last_close=None) == "green"


def test_crypto_red_when_stale():
    now = _utc("2026-04-20T20:00:00")
    bar = now - timedelta(seconds=400)
    assert _bar_color("BTC/USD", bar, now, market_open_now=False, last_close=None) == "red"


def test_equity_intraday_green():
    now = _utc("2026-04-20T15:00:00")
    bar = now - timedelta(seconds=60)
    assert _bar_color("SPY", bar, now, market_open_now=True, last_close=None) == "green"


def test_equity_intraday_red_when_stalled():
    now = _utc("2026-04-20T15:00:00")
    bar = now - timedelta(seconds=1000)
    assert _bar_color("SPY", bar, now, market_open_now=True, last_close=None) == "red"


def test_equity_after_close_green_if_bar_near_close():
    # "Now" is 1h after close (21:00 UTC); last bar at 19:59 UTC (1m before close)
    now = _utc("2026-04-20T21:00:00")
    last_close = _utc("2026-04-20T20:00:00")
    bar = _utc("2026-04-20T19:59:00")
    assert _bar_color("SPY", bar, now, market_open_now=False, last_close=last_close) == "green"


def test_equity_post_market_bar_is_green():
    # Bar is after close (post-market tape) — gap clamps to 0 even though bar > close
    now = _utc("2026-04-21T03:00:00")
    last_close = _utc("2026-04-20T20:00:00")
    bar = _utc("2026-04-20T20:30:00")
    assert _bar_color("SPY", bar, now, market_open_now=False, last_close=last_close) == "green"


def test_equity_days_stale_is_red_even_when_market_closed():
    now = _utc("2026-04-20T22:00:00")
    last_close = _utc("2026-04-20T20:00:00")
    bar = _utc("2026-04-15T20:00:00")
    assert _bar_color("SPY", bar, now, market_open_now=False, last_close=last_close) == "red"


def test_equity_closed_yellow_when_gap_between_15m_and_1h():
    # Last bar is 30m before close → gap = 1800s → yellow
    now = _utc("2026-04-20T22:00:00")
    last_close = _utc("2026-04-20T20:00:00")
    bar = _utc("2026-04-20T19:30:00")
    assert _bar_color("SPY", bar, now, market_open_now=False, last_close=last_close) == "yellow"
