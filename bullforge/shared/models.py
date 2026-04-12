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


VALID_ORDER_SIDES = {"buy", "sell"}
VALID_ORDER_TYPES = {"market", "limit"}
VALID_ORDER_STATUSES = {
    "pending", "submitted", "filled", "partially_filled", "cancelled", "rejected",
}


@dataclass(frozen=True)
class TradingSignal:
    symbol: str
    timestamp: datetime
    direction: int  # -1 = sell, 0 = flat/exit, 1 = buy
    strength: float  # 0.0–1.0
    contributing_predictions: int

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.tzinfo != timezone.utc:
            raise ValueError(
                f"TradingSignal timestamp must be UTC, got tzinfo={self.timestamp.tzinfo}"
            )
        if self.direction not in (-1, 0, 1):
            raise ValueError(
                f"direction must be -1, 0, or 1, got {self.direction}"
            )
        if not (0.0 <= self.strength <= 1.0):
            raise ValueError(
                f"strength must be in [0.0, 1.0], got {self.strength}"
            )
        if self.contributing_predictions < 0:
            raise ValueError(
                f"contributing_predictions must be non-negative, got {self.contributing_predictions}"
            )


@dataclass(frozen=True)
class Order:
    symbol: str
    side: str  # "buy" or "sell"
    quantity: float  # float for crypto fractional shares
    order_type: str  # "market" or "limit"
    submitted_at: datetime
    signal_strength: float  # 0.0–1.0, strength of the signal that triggered this
    alpaca_order_id: Optional[str] = None
    status: str = "pending"
    filled_at: Optional[datetime] = None
    filled_price: Optional[float] = None
    filled_quantity: Optional[float] = None
    limit_price: Optional[float] = None
    id: Optional[int] = None

    def __post_init__(self):
        if self.submitted_at.tzinfo is None or self.submitted_at.tzinfo != timezone.utc:
            raise ValueError(
                f"submitted_at must be UTC, got tzinfo={self.submitted_at.tzinfo}"
            )
        if self.side not in VALID_ORDER_SIDES:
            raise ValueError(
                f"side must be one of {VALID_ORDER_SIDES}, got {self.side!r}"
            )
        if self.quantity <= 0:
            raise ValueError(
                f"quantity must be positive, got {self.quantity}"
            )
        if self.order_type not in VALID_ORDER_TYPES:
            raise ValueError(
                f"order_type must be one of {VALID_ORDER_TYPES}, got {self.order_type!r}"
            )
        if self.order_type == "limit" and self.limit_price is None:
            raise ValueError("limit_price is required for limit orders")
        if not (0.0 <= self.signal_strength <= 1.0):
            raise ValueError(
                f"signal_strength must be in [0.0, 1.0], got {self.signal_strength}"
            )
        if self.status not in VALID_ORDER_STATUSES:
            raise ValueError(
                f"status must be one of {VALID_ORDER_STATUSES}, got {self.status!r}"
            )
        if self.filled_at is not None and (
            self.filled_at.tzinfo is None or self.filled_at.tzinfo != timezone.utc
        ):
            raise ValueError(
                f"filled_at must be UTC when provided, got tzinfo={self.filled_at.tzinfo}"
            )
