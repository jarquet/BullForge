from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta

from shared.config import Settings
from shared.db import Database
from shared.models import CalendarEvent, CalendarEventType

logger = logging.getLogger(__name__)


class CalendarService:
    def __init__(self, settings: Settings, db: Database):
        self._settings = settings
        self._db = db

    async def store_event(self, event: CalendarEvent) -> None:
        async with await self._db.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO calendar_events (event_type, symbol, event_date, description)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (event_type, symbol, event_date) DO UPDATE
                SET description = $4
                """,
                event.event_type.value, event.symbol, event.date, event.description,
            )
        logger.info("Stored calendar event: %s %s %s", event.event_type.value, event.symbol, event.date)

    async def is_fed_day(self, date: datetime) -> bool:
        start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1)
        async with await self._db.pool.acquire() as conn:
            count = await conn.fetchval(
                """
                SELECT count(*) FROM calendar_events
                WHERE event_type = $1
                AND event_date >= $2 AND event_date < $3
                """,
                CalendarEventType.FED_ANNOUNCEMENT.value, start_of_day, end_of_day,
            )
        return count > 0

    async def is_near_earnings(self, symbol: str, date: datetime, days_window: int = 2) -> bool:
        window_start = date - timedelta(days=days_window)
        window_end = date + timedelta(days=days_window)
        async with await self._db.pool.acquire() as conn:
            count = await conn.fetchval(
                """
                SELECT count(*) FROM calendar_events
                WHERE event_type = $1
                AND symbol = $2
                AND event_date >= $3 AND event_date <= $4
                """,
                CalendarEventType.EARNINGS.value, symbol, window_start, window_end,
            )
        return count > 0

    async def is_market_holiday(self, date: datetime) -> bool:
        start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1)
        async with await self._db.pool.acquire() as conn:
            count = await conn.fetchval(
                """
                SELECT count(*) FROM calendar_events
                WHERE event_type = $1
                AND event_date >= $2 AND event_date < $3
                """,
                CalendarEventType.MARKET_HOLIDAY.value, start_of_day, end_of_day,
            )
        return count > 0
