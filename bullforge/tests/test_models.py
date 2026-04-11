from datetime import datetime, timezone
from shared.models import Bar, CalendarEvent, CalendarEventType


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
