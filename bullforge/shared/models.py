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


@dataclass(frozen=True)
class FeatureRow:
    symbol: str
    timestamp: datetime
    timeframe: str  # "1m", "1d"
    features: dict[str, float]  # {"sma_20": 420.5, "rsi_14": 55.3, ...}

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.tzinfo != timezone.utc:
            raise ValueError(
                f"FeatureRow timestamp must be UTC, got tzinfo={self.timestamp.tzinfo}"
            )
        if not self.features:
            raise ValueError("FeatureRow features must not be empty")


@dataclass(frozen=True)
class Label:
    symbol: str
    timestamp: datetime
    timeframe: str  # "1m", "1d"
    horizon_minutes: int  # how far forward the label looks
    forward_return: float  # actual return over the horizon
    direction: int  # -1 = down, 0 = flat, 1 = up

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.tzinfo != timezone.utc:
            raise ValueError(
                f"Label timestamp must be UTC, got tzinfo={self.timestamp.tzinfo}"
            )
        if self.horizon_minutes <= 0:
            raise ValueError(
                f"horizon_minutes must be positive, got {self.horizon_minutes}"
            )
        if self.direction not in (-1, 0, 1):
            raise ValueError(
                f"Label direction must be -1, 0, or 1, got {self.direction}"
            )


@dataclass(frozen=True)
class ModelMetadata:
    symbol: str
    timeframe: str  # "1m", "1d"
    horizon_minutes: int
    model_type: str  # "gradient_boost", "logistic_regression"
    artifact_path: str
    metrics: dict[str, float]
    feature_columns: list[str]
    trained_at: datetime
    train_samples: int
    test_samples: int
    is_active: bool = True
    id: Optional[int] = None

    def __post_init__(self):
        if self.trained_at.tzinfo is None or self.trained_at.tzinfo != timezone.utc:
            raise ValueError(
                f"ModelMetadata trained_at must be UTC, got tzinfo={self.trained_at.tzinfo}"
            )
        if self.horizon_minutes <= 0:
            raise ValueError(
                f"horizon_minutes must be positive, got {self.horizon_minutes}"
            )
        if not self.feature_columns:
            raise ValueError("feature_columns must not be empty")


@dataclass(frozen=True)
class Prediction:
    symbol: str
    timestamp: datetime
    timeframe: str  # "1m", "1d"
    horizon_minutes: int
    model_id: int
    predicted_direction: int  # -1 = down, 0 = flat, 1 = up
    confidence: float  # probability of predicted class, 0.0–1.0

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.tzinfo != timezone.utc:
            raise ValueError(
                f"Prediction timestamp must be UTC, got tzinfo={self.timestamp.tzinfo}"
            )
        if self.predicted_direction not in (-1, 0, 1):
            raise ValueError(
                f"predicted_direction must be -1, 0, or 1, got {self.predicted_direction}"
            )
        if not (0.0 <= self.confidence <= 1.0):
            raise ValueError(
                f"confidence must be between 0.0 and 1.0, got {self.confidence}"
            )
