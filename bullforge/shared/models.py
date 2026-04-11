from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class CalendarEventType(Enum):
    EARNINGS = "earnings"
    FED_ANNOUNCEMENT = "fed_announcement"
    MARKET_HOLIDAY = "market_holiday"


@dataclass(frozen=True)
class Bar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    timeframe: str  # "1m", "1d", etc.

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.tzinfo != timezone.utc:
            raise ValueError(
                f"Bar timestamp must be UTC, got tzinfo={self.timestamp.tzinfo}"
            )


@dataclass(frozen=True)
class CalendarEvent:
    event_type: CalendarEventType
    date: datetime
    description: str
    symbol: Optional[str] = None
