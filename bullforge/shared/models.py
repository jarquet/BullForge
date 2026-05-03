from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
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
    volume: float  # base-asset units; fractional for crypto (e.g. 0.7 BTC)
    timeframe: str  # "1m", "1d", etc.

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() != timedelta(0):
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
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() != timedelta(0):
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
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() != timedelta(0):
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
    estimator_type: Optional[str] = None  # e.g., "lightgbm", "mlp", "baseline" for meta-models
    id: Optional[int] = None

    def __post_init__(self):
        if self.trained_at.tzinfo is None or self.trained_at.utcoffset() != timedelta(0):
            raise ValueError(
                f"ModelMetadata trained_at must be UTC, got tzinfo={self.trained_at.tzinfo}"
            )
        _meta_types = ("meta_entry", "meta_exec")
        if self.horizon_minutes < 0 or (
            self.horizon_minutes == 0 and self.model_type not in _meta_types
        ):
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
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() != timedelta(0):
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
    "pending", "submitted", "filled", "partially_filled",
    "cancelled", "rejected", "expired",
}

VALID_ORDER_ORIGINS = {"internal", "external"}
VALID_ORDER_RECONCILE_SOURCES = {"submit", "stream", "poll", "startup"}


@dataclass(frozen=True)
class TradingSignal:
    symbol: str
    timestamp: datetime
    direction: int  # -1 = sell, 0 = flat/exit, 1 = buy
    strength: float  # 0.0–1.0
    contributing_predictions: int

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() != timedelta(0):
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
    origin: str = "internal"
    last_reconciled_at: Optional[datetime] = None
    reconcile_source: Optional[str] = None
    id: Optional[int] = None

    def __post_init__(self):
        if self.submitted_at.tzinfo is None or self.submitted_at.utcoffset() != timedelta(0):
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
            self.filled_at.tzinfo is None or self.filled_at.utcoffset() != timedelta(0)
        ):
            raise ValueError(
                f"filled_at must be UTC when provided, got tzinfo={self.filled_at.tzinfo}"
            )
        if self.origin not in VALID_ORDER_ORIGINS:
            raise ValueError(
                f"origin must be one of {VALID_ORDER_ORIGINS}, got {self.origin!r}"
            )
        if self.reconcile_source is not None and self.reconcile_source not in VALID_ORDER_RECONCILE_SOURCES:
            raise ValueError(
                f"reconcile_source must be one of {VALID_ORDER_RECONCILE_SOURCES}, got {self.reconcile_source!r}"
            )
        if self.last_reconciled_at is not None and (
            self.last_reconciled_at.tzinfo is None
            or self.last_reconciled_at.utcoffset() != timedelta(0)
        ):
            raise ValueError(
                f"last_reconciled_at must be UTC when provided, got tzinfo={self.last_reconciled_at.tzinfo}"
            )


VALID_META_SOURCES = {"replay", "live"}


@dataclass(frozen=True)
class MetaFeatureRow:
    symbol: str
    timestamp: datetime
    direction: int  # 1=long, -1=short
    feature_vector: dict[str, float]
    source: str  # "replay" or "live"
    entry_label: Optional[int] = None  # 1=hit target, 0=hit stop
    optimal_pair_idx: Optional[int] = None
    realized_ev: Optional[float] = None

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() != timedelta(0):
            raise ValueError(
                f"MetaFeatureRow timestamp must be UTC, got tzinfo={self.timestamp.tzinfo}"
            )
        if self.direction not in (-1, 1):
            raise ValueError(
                f"direction must be -1 or 1, got {self.direction}"
            )
        if not self.feature_vector:
            raise ValueError("feature_vector must not be empty")
        if self.source not in VALID_META_SOURCES:
            raise ValueError(
                f"source must be one of {VALID_META_SOURCES}, got {self.source!r}"
            )


@dataclass(frozen=True)
class MetaSignalShadow:
    symbol: str
    timestamp: datetime
    direction: int  # 1=long, -1=short
    estimator_type: str
    entry_probability: float
    kelly_fraction: Optional[float] = None
    pair_index: Optional[int] = None
    target_pct: Optional[float] = None
    stop_pct: Optional[float] = None
    model_id: Optional[int] = None
    id: Optional[int] = None

    def __post_init__(self):
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() != timedelta(0):
            raise ValueError(
                f"MetaSignalShadow timestamp must be UTC, got tzinfo={self.timestamp.tzinfo}"
            )
        if self.direction not in (-1, 1):
            raise ValueError(
                f"direction must be -1 or 1, got {self.direction}"
            )
        if not (0.0 <= self.entry_probability <= 1.0):
            raise ValueError(
                f"entry_probability must be in [0.0, 1.0], got {self.entry_probability}"
            )


VALID_TRADE_SIDES = {"long", "short"}
VALID_MODEL_STATUSES = {"active", "warned", "disabled"}


@dataclass(frozen=True)
class Trade:
    symbol: str
    side: str  # "long" or "short"
    entry_time: datetime
    entry_price: float
    quantity: float
    entry_order_id: int
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    hold_duration_seconds: Optional[int] = None
    exit_order_id: Optional[int] = None
    id: Optional[int] = None

    def __post_init__(self):
        if self.entry_time.tzinfo is None or self.entry_time.utcoffset() != timedelta(0):
            raise ValueError(
                f"entry_time must be UTC, got tzinfo={self.entry_time.tzinfo}"
            )
        if self.side not in VALID_TRADE_SIDES:
            raise ValueError(
                f"side must be one of {VALID_TRADE_SIDES}, got {self.side!r}"
            )
        if self.quantity <= 0:
            raise ValueError(
                f"quantity must be positive, got {self.quantity}"
            )
        if self.exit_time is not None and (
            self.exit_time.tzinfo is None or self.exit_time.utcoffset() != timedelta(0)
        ):
            raise ValueError(
                f"exit_time must be UTC when provided, got tzinfo={self.exit_time.tzinfo}"
            )


@dataclass(frozen=True)
class ModelStatus:
    model_id: int
    status: str  # "active", "warned", "disabled"
    reason: str
    updated_at: datetime

    def __post_init__(self):
        if self.updated_at.tzinfo is None or self.updated_at.utcoffset() != timedelta(0):
            raise ValueError(
                f"updated_at must be UTC, got tzinfo={self.updated_at.tzinfo}"
            )
        if self.status not in VALID_MODEL_STATUSES:
            raise ValueError(
                f"status must be one of {VALID_MODEL_STATUSES}, got {self.status!r}"
            )


@dataclass(frozen=True)
class BacktestScope:
    """Identifies which backtest run + fold a sandbox row belongs to.

    Threaded as an optional `scope` kwarg through the engine, trainer,
    inference engine, order executor, and Database methods. `None` means
    the caller is a live code path (rows have NULL scope columns).
    """
    backtest_run_id: int
    fold_index: int
