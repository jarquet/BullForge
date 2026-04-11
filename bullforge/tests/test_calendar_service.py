import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from shared.config import Settings
from shared.models import CalendarEvent, CalendarEventType
from services.data_pipeline.calendar_service import CalendarService


@pytest.fixture
def mock_settings(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test_secret")
    return Settings()


@pytest.mark.asyncio
async def test_is_fed_day_returns_true_on_fed_day(mock_settings):
    mock_db = AsyncMock()
    fed_date = datetime(2026, 5, 7, tzinfo=timezone.utc)
    mock_db.pool.acquire.return_value.__aenter__ = AsyncMock(
        return_value=MagicMock(fetchval=AsyncMock(return_value=1))
    )
    mock_db.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    service = CalendarService(settings=mock_settings, db=mock_db)
    result = await service.is_fed_day(fed_date)
    assert result is True


@pytest.mark.asyncio
async def test_is_fed_day_returns_false_on_normal_day(mock_settings):
    mock_db = AsyncMock()
    mock_db.pool.acquire.return_value.__aenter__ = AsyncMock(
        return_value=MagicMock(fetchval=AsyncMock(return_value=0))
    )
    mock_db.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    service = CalendarService(settings=mock_settings, db=mock_db)
    normal_date = datetime(2026, 5, 8, tzinfo=timezone.utc)
    result = await service.is_fed_day(normal_date)
    assert result is False


@pytest.mark.asyncio
async def test_is_near_earnings_returns_true_within_window(mock_settings):
    mock_db = AsyncMock()
    mock_db.pool.acquire.return_value.__aenter__ = AsyncMock(
        return_value=MagicMock(fetchval=AsyncMock(return_value=1))
    )
    mock_db.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    service = CalendarService(settings=mock_settings, db=mock_db)
    result = await service.is_near_earnings("AAPL", datetime(2026, 4, 23, tzinfo=timezone.utc), days_window=3)
    assert result is True


@pytest.mark.asyncio
async def test_store_calendar_event(mock_settings):
    mock_db = AsyncMock()
    mock_conn = AsyncMock()
    mock_db.pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    service = CalendarService(settings=mock_settings, db=mock_db)
    event = CalendarEvent(
        event_type=CalendarEventType.EARNINGS, symbol="AAPL",
        date=datetime(2026, 4, 24, tzinfo=timezone.utc), description="AAPL Q2 2026 Earnings",
    )
    await service.store_event(event)
    mock_conn.execute.assert_called_once()
