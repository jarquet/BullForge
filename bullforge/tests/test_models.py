from datetime import datetime, timezone
from shared.models import Bar, CalendarEvent, CalendarEventType, Order


def test_bar_creation():
    bar = Bar(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        open=420.10,
        high=420.50,
        low=419.80,
        close=420.30,
        volume=15000,
        timeframe="1m",
    )
    assert bar.symbol == "SPY"
    assert bar.timestamp.tzinfo == timezone.utc
    assert bar.open == 420.10
    assert bar.timeframe == "1m"


def test_bar_rejects_naive_timestamp():
    """Bars must have timezone-aware UTC timestamps."""
    import pytest

    with pytest.raises(ValueError, match="UTC"):
        Bar(
            symbol="SPY",
            timestamp=datetime(2026, 4, 10, 15, 30, 0),  # naive
            open=420.10,
            high=420.50,
            low=419.80,
            close=420.30,
            volume=15000,
            timeframe="1m",
        )


def test_bar_accepts_zoneinfo_utc():
    """ZoneInfo("UTC") has a zero UTC offset and must be accepted."""
    from zoneinfo import ZoneInfo
    bar = Bar(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=ZoneInfo("UTC")),
        open=420.10,
        high=420.50,
        low=419.80,
        close=420.30,
        volume=15000,
        timeframe="1m",
    )
    assert bar.symbol == "SPY"


def test_calendar_event_creation():
    event = CalendarEvent(
        event_type=CalendarEventType.EARNINGS,
        symbol="AAPL",
        date=datetime(2026, 4, 24, tzinfo=timezone.utc),
        description="AAPL Q2 2026 Earnings",
    )
    assert event.event_type == CalendarEventType.EARNINGS
    assert event.symbol == "AAPL"


def test_calendar_event_fed_has_no_symbol():
    event = CalendarEvent(
        event_type=CalendarEventType.FED_ANNOUNCEMENT,
        symbol=None,
        date=datetime(2026, 5, 7, tzinfo=timezone.utc),
        description="FOMC Meeting",
    )
    assert event.symbol is None
    assert event.event_type == CalendarEventType.FED_ANNOUNCEMENT


def test_order_accepts_manual_replay_reconcile_source():
    """Locks in the manual_replay value the reconcile_from_alpaca CLI uses.

    Prior bug: the validator was widened in models.py but the DB CHECK
    constraint and the validator drifted out of sync (the script tripped
    a CheckViolationError, then once fixed, a ValueError on Order
    construction). Test pins the validator side so any future narrowing
    of VALID_ORDER_RECONCILE_SOURCES is caught here, not at production.
    """
    order = Order(
        symbol="BTC/USD",
        side="sell",
        quantity=0.0989,
        order_type="market",
        submitted_at=datetime(2026, 5, 5, 1, 8, tzinfo=timezone.utc),
        signal_strength=0.0,
        alpaca_order_id="alp-replay-1",
        status="filled",
        origin="external",
        reconcile_source="manual_replay",
    )
    assert order.reconcile_source == "manual_replay"


def test_order_rejects_unknown_reconcile_source():
    """Counterpart guard: the validator still rejects unknown values."""
    import pytest

    with pytest.raises(ValueError, match="reconcile_source"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=1.0,
            order_type="market",
            submitted_at=datetime(2026, 5, 5, 14, 0, tzinfo=timezone.utc),
            signal_strength=0.0,
            alpaca_order_id="alp-bad-1",
            status="submitted",
            reconcile_source="not_a_real_source",
        )
