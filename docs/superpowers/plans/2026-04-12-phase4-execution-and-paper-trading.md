# Phase 4: Execution & Paper Trading — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an execution service that subscribes to model predictions, aggregates them into trading signals, applies risk checks, and submits paper trading orders to Alpaca — tracking all orders in TimescaleDB for analysis.

**Architecture:** A new `execution_service` subscribes to the `model:predictions` Redis channel (published by Phase 3's model service). A `SignalAggregator` combines multi-horizon predictions into a single `TradingSignal` using confidence-weighted voting. A `RiskManager` enforces position limits, portfolio exposure caps, and minimum signal strength. An `AlpacaBroker` wraps Alpaca's `TradingClient` (paper mode) with async-safe wrappers via `asyncio.to_thread`. An `OrderExecutor` orchestrates the full flow: signal → risk check → order submission → fill tracking → DB record. A `PositionTracker` caches account and position state from Alpaca. All orders are persisted in a new `orders` table for post-trade analysis. Both long and short positions are supported — there's no reason to restrict on a paper account.

**Tech Stack:** Python 3.12+, alpaca-py (TradingClient), asyncio, asyncpg (from shared/db.py), Redis pub/sub (from shared/redis.py), aiohttp, pytest, pytest-asyncio

---

## File Structure

```
bullforge/
  shared/
    config.py                              — (modify) Add 6 execution service settings
    models.py                              — (modify) Add TradingSignal and Order dataclasses
    db.py                                  — (modify) Add order insert, update, and query methods

  services/
    execution_service/
      __init__.py                          — Empty package marker
      Dockerfile                           — Container image for execution service
      requirements.txt                     — alpaca-py, asyncpg, redis, aiohttp
      main.py                              — Entrypoint: subscribe to predictions, run execution
      health.py                            — Health check endpoint (/health on :8082)
      alpaca_broker.py                     — Alpaca TradingClient wrapper (sync→async via to_thread)
      signal_aggregator.py                 — Combine multi-horizon predictions into TradingSignal
      risk_manager.py                      — Pre-trade risk checks (limits, exposure, strength)
      position_tracker.py                  — Query and cache Alpaca account + positions
      order_executor.py                    — Orchestrate: signal → risk → order → fill → record

  migrations/
    007_create_orders.sql                  — Orders table with indexes

  tests/
    test_execution_models.py               — Tests for TradingSignal and Order dataclasses
    test_order_db.py                       — Tests for order DB methods
    test_signal_aggregator.py              — Tests for signal aggregation logic
    test_alpaca_broker.py                  — Tests for Alpaca broker wrapper
    test_risk_manager.py                   — Tests for risk checks
    test_position_tracker.py               — Tests for position tracking
    test_order_executor.py                 — Tests for order execution orchestration
```

---

## Task 1: TradingSignal and Order Dataclasses

**Files:**
- Modify: `shared/models.py` — append two new dataclasses after `Prediction`
- Create: `tests/test_execution_models.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_execution_models.py`:

```python
import pytest
from datetime import datetime, timezone, timedelta
from shared.models import TradingSignal, Order


# --- TradingSignal tests ---

def test_valid_trading_signal():
    ts = TradingSignal(
        symbol="SPY",
        timestamp=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        direction=1,
        strength=0.75,
        contributing_predictions=3,
    )
    assert ts.symbol == "SPY"
    assert ts.direction == 1
    assert ts.strength == 0.75
    assert ts.contributing_predictions == 3


def test_trading_signal_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="UTC"):
        TradingSignal(
            symbol="SPY",
            timestamp=datetime(2026, 4, 12, 14, 0, 0),
            direction=1,
            strength=0.75,
            contributing_predictions=3,
        )


def test_trading_signal_rejects_invalid_direction():
    with pytest.raises(ValueError, match="direction"):
        TradingSignal(
            symbol="SPY",
            timestamp=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            direction=2,
            strength=0.75,
            contributing_predictions=3,
        )


def test_trading_signal_rejects_strength_out_of_range():
    with pytest.raises(ValueError, match="strength"):
        TradingSignal(
            symbol="SPY",
            timestamp=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            direction=1,
            strength=1.5,
            contributing_predictions=3,
        )


def test_trading_signal_rejects_negative_predictions():
    with pytest.raises(ValueError, match="contributing_predictions"):
        TradingSignal(
            symbol="SPY",
            timestamp=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            direction=1,
            strength=0.75,
            contributing_predictions=-1,
        )


# --- Order tests ---

def test_valid_order():
    order = Order(
        symbol="SPY",
        side="buy",
        quantity=10.0,
        order_type="market",
        submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        signal_strength=0.8,
    )
    assert order.symbol == "SPY"
    assert order.side == "buy"
    assert order.quantity == 10.0
    assert order.status == "pending"
    assert order.alpaca_order_id is None
    assert order.id is None


def test_order_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="submitted_at must be UTC"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=10.0,
            order_type="market",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0),
            signal_strength=0.8,
        )


def test_order_rejects_invalid_side():
    with pytest.raises(ValueError, match="side"):
        Order(
            symbol="SPY",
            side="hold",
            quantity=10.0,
            order_type="market",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            signal_strength=0.8,
        )


def test_order_rejects_zero_quantity():
    with pytest.raises(ValueError, match="quantity"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=0.0,
            order_type="market",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            signal_strength=0.8,
        )


def test_order_rejects_invalid_order_type():
    with pytest.raises(ValueError, match="order_type"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=10.0,
            order_type="stop",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            signal_strength=0.8,
        )


def test_order_rejects_limit_without_price():
    with pytest.raises(ValueError, match="limit_price"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=10.0,
            order_type="limit",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            signal_strength=0.8,
        )


def test_order_rejects_invalid_status():
    with pytest.raises(ValueError, match="status"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=10.0,
            order_type="market",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            signal_strength=0.8,
            status="unknown",
        )


def test_order_rejects_signal_strength_out_of_range():
    with pytest.raises(ValueError, match="signal_strength"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=10.0,
            order_type="market",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            signal_strength=1.5,
        )


def test_order_accepts_filled_state():
    order = Order(
        symbol="SPY",
        side="buy",
        quantity=10.0,
        order_type="market",
        submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        signal_strength=0.8,
        alpaca_order_id="abc-123",
        status="filled",
        filled_at=datetime(2026, 4, 12, 14, 0, 1, tzinfo=timezone.utc),
        filled_price=420.50,
        filled_quantity=10.0,
        id=42,
    )
    assert order.status == "filled"
    assert order.filled_price == 420.50
    assert order.id == 42


def test_order_rejects_naive_filled_at():
    with pytest.raises(ValueError, match="filled_at must be UTC"):
        Order(
            symbol="SPY",
            side="buy",
            quantity=10.0,
            order_type="market",
            submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            signal_strength=0.8,
            filled_at=datetime(2026, 4, 12, 14, 0, 1),
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_execution_models.py -v`
Expected: FAIL — `ImportError: cannot import name 'TradingSignal' from 'shared.models'`

- [ ] **Step 3: Write the implementation**

Append the following to `shared/models.py` after the `Prediction` class:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_execution_models.py -v`
Expected: 11 passed

- [ ] **Step 5: Commit**

```bash
git add shared/models.py tests/test_execution_models.py
git commit -m "feat: add TradingSignal and Order dataclasses with validation"
```

---

## Task 2: Orders Migration

**Files:**
- Create: `migrations/007_create_orders.sql`

- [ ] **Step 1: Create the migration**

Create `migrations/007_create_orders.sql`:

```sql
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    order_type TEXT NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL,
    signal_strength DOUBLE PRECISION NOT NULL,
    alpaca_order_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    filled_at TIMESTAMPTZ,
    filled_price DOUBLE PRECISION,
    filled_quantity DOUBLE PRECISION,
    limit_price DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_symbol_submitted
    ON orders (symbol, submitted_at DESC);

CREATE INDEX IF NOT EXISTS idx_orders_status
    ON orders (status);

CREATE INDEX IF NOT EXISTS idx_orders_alpaca_id
    ON orders (alpaca_order_id)
    WHERE alpaca_order_id IS NOT NULL;
```

- [ ] **Step 2: Commit**

```bash
git add migrations/007_create_orders.sql
git commit -m "feat: add orders table migration"
```

---

## Task 3: Execution Settings

**Files:**
- Modify: `shared/config.py` — append 6 fields after `model_min_samples`
- Modify: `tests/test_config.py` — append 2 tests
- Modify: `.env.example` — append execution env vars

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_config.py`:

```python
def test_execution_defaults(settings):
    assert settings.execution_max_position_value == 10000.0
    assert settings.execution_max_positions == 5
    assert settings.execution_max_exposure == 0.5
    assert settings.execution_min_signal_strength == 0.5
    assert settings.execution_min_confidence == 0.6
    assert settings.execution_min_agreement == 2


def test_execution_env_override(monkeypatch):
    monkeypatch.setenv("EXECUTION_MAX_POSITION_VALUE", "20000.0")
    monkeypatch.setenv("EXECUTION_MAX_POSITIONS", "10")
    monkeypatch.setenv("EXECUTION_MAX_EXPOSURE", "0.8")
    monkeypatch.setenv("EXECUTION_MIN_SIGNAL_STRENGTH", "0.7")
    monkeypatch.setenv("EXECUTION_MIN_CONFIDENCE", "0.75")
    monkeypatch.setenv("EXECUTION_MIN_AGREEMENT", "3")
    from shared.config import Settings
    s = Settings()
    assert s.execution_max_position_value == 20000.0
    assert s.execution_max_positions == 10
    assert s.execution_max_exposure == 0.8
    assert s.execution_min_signal_strength == 0.7
    assert s.execution_min_confidence == 0.75
    assert s.execution_min_agreement == 3
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_config.py::test_execution_defaults tests/test_config.py::test_execution_env_override -v`
Expected: FAIL — `AttributeError: 'Settings' object has no attribute 'execution_max_position_value'`

- [ ] **Step 3: Write the implementation**

Append to the `Settings` class in `shared/config.py` after the `model_min_samples` field:

```python
    # Execution service
    execution_max_position_value: float = field(
        default_factory=lambda: float(os.getenv("EXECUTION_MAX_POSITION_VALUE", "10000.0"))
    )
    execution_max_positions: int = field(
        default_factory=lambda: int(os.getenv("EXECUTION_MAX_POSITIONS", "5"))
    )
    execution_max_exposure: float = field(
        default_factory=lambda: float(os.getenv("EXECUTION_MAX_EXPOSURE", "0.5"))
    )
    execution_min_signal_strength: float = field(
        default_factory=lambda: float(os.getenv("EXECUTION_MIN_SIGNAL_STRENGTH", "0.5"))
    )
    execution_min_confidence: float = field(
        default_factory=lambda: float(os.getenv("EXECUTION_MIN_CONFIDENCE", "0.6"))
    )
    execution_min_agreement: int = field(
        default_factory=lambda: int(os.getenv("EXECUTION_MIN_AGREEMENT", "2"))
    )
```

Also append to `.env.example`:

```bash
# Execution service
EXECUTION_MAX_POSITION_VALUE=10000.0
EXECUTION_MAX_POSITIONS=5
EXECUTION_MAX_EXPOSURE=0.5
EXECUTION_MIN_SIGNAL_STRENGTH=0.5
EXECUTION_MIN_CONFIDENCE=0.6
EXECUTION_MIN_AGREEMENT=2
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_config.py -v`
Expected: 8 passed (6 existing + 2 new)

- [ ] **Step 5: Commit**

```bash
git add shared/config.py tests/test_config.py .env.example
git commit -m "feat: add execution service settings"
```

---

## Task 4: Order Database Methods

**Files:**
- Modify: `shared/db.py` — update import, add 4 methods after `query_predictions`
- Create: `tests/test_order_db.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_order_db.py`:

```python
import json
import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock
from shared.models import Order


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    conn = AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx
    return pool, conn


def _make_order():
    return Order(
        symbol="SPY",
        side="buy",
        quantity=10.0,
        order_type="market",
        submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        signal_strength=0.8,
        alpaca_order_id="alp-123",
        status="submitted",
    )


@pytest.mark.asyncio
async def test_insert_order(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchval = AsyncMock(return_value=42)
    db = Database(settings)
    db.pool = pool

    order = _make_order()
    row_id = await db.insert_order(order)

    assert row_id == 42
    conn.fetchval.assert_called_once()
    call_args = conn.fetchval.call_args
    assert "INSERT INTO orders" in call_args[0][0]
    assert call_args[0][1] == "SPY"


@pytest.mark.asyncio
async def test_update_order_status(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.execute = AsyncMock()
    db = Database(settings)
    db.pool = pool

    filled_at = datetime(2026, 4, 12, 14, 0, 1, tzinfo=timezone.utc)
    await db.update_order_status(
        order_id=42,
        status="filled",
        filled_at=filled_at,
        filled_price=420.50,
        filled_quantity=10.0,
    )

    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert "UPDATE orders" in call_args[0][0]
    assert call_args[0][1] == 42
    assert call_args[0][2] == "filled"


@pytest.mark.asyncio
async def test_query_orders_by_symbol(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1,
            "symbol": "SPY",
            "side": "buy",
            "quantity": 10.0,
            "order_type": "market",
            "submitted_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            "signal_strength": 0.8,
            "alpaca_order_id": "alp-123",
            "status": "filled",
            "filled_at": datetime(2026, 4, 12, 14, 0, 1, tzinfo=timezone.utc),
            "filled_price": 420.50,
            "filled_quantity": 10.0,
            "limit_price": None,
        }
    ])
    db = Database(settings)
    db.pool = pool

    orders = await db.query_orders(symbol="SPY", limit=10)

    assert len(orders) == 1
    assert orders[0]["symbol"] == "SPY"
    assert orders[0]["status"] == "filled"


@pytest.mark.asyncio
async def test_query_orders_by_status(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[])
    db = Database(settings)
    db.pool = pool

    orders = await db.query_orders(status="submitted", limit=10)

    assert len(orders) == 0
    conn.fetch.assert_called_once()
    call_args = conn.fetch.call_args
    assert "status = " in call_args[0][0]


@pytest.mark.asyncio
async def test_query_orders_no_filters(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[])
    db = Database(settings)
    db.pool = pool

    orders = await db.query_orders(limit=50)

    assert len(orders) == 0
    conn.fetch.assert_called_once()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_order_db.py -v`
Expected: FAIL — `AttributeError: 'Database' object has no attribute 'insert_order'`

- [ ] **Step 3: Write the implementation**

Update the import line in `shared/db.py`:

```python
from shared.models import Bar, FeatureRow, Label, ModelMetadata, Prediction, Order
```

Append the following methods to the `Database` class after `query_predictions`:

```python
    async def insert_order(self, order: Order) -> int:
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO orders
                    (symbol, side, quantity, order_type, submitted_at,
                     signal_strength, alpaca_order_id, status,
                     filled_at, filled_price, filled_quantity, limit_price)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                RETURNING id
                """,
                order.symbol,
                order.side,
                order.quantity,
                order.order_type,
                order.submitted_at,
                order.signal_strength,
                order.alpaca_order_id,
                order.status,
                order.filled_at,
                order.filled_price,
                order.filled_quantity,
                order.limit_price,
            )
        return row_id

    async def update_order_status(
        self,
        order_id: int,
        status: str,
        filled_at: Optional[datetime] = None,
        filled_price: Optional[float] = None,
        filled_quantity: Optional[float] = None,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE orders
                SET status = $2,
                    filled_at = COALESCE($3, filled_at),
                    filled_price = COALESCE($4, filled_price),
                    filled_quantity = COALESCE($5, filled_quantity)
                WHERE id = $1
                """,
                order_id,
                status,
                filled_at,
                filled_price,
                filled_quantity,
            )

    async def query_orders(
        self,
        symbol: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        query = "SELECT * FROM orders WHERE TRUE"
        args: list = []
        idx = 1

        if symbol is not None:
            query += f" AND symbol = ${idx}"
            args.append(symbol)
            idx += 1

        if status is not None:
            query += f" AND status = ${idx}"
            args.append(status)
            idx += 1

        query += f" ORDER BY submitted_at DESC LIMIT ${idx}"
        args.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [dict(r) for r in rows]
```

Note: Add `from datetime import datetime` is not needed — the `datetime` type is already available via `from shared.models import ...` which imports it transitively. The `Optional` type is already imported.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_order_db.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add shared/db.py tests/test_order_db.py
git commit -m "feat: add order database methods (insert, update, query)"
```

---

## Task 5: Signal Aggregator

**Files:**
- Create: `services/execution_service/__init__.py` (empty)
- Create: `services/execution_service/signal_aggregator.py`
- Create: `tests/test_signal_aggregator.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_signal_aggregator.py`:

```python
import pytest
from datetime import datetime, timezone
from shared.models import TradingSignal
from services.execution_service.signal_aggregator import SignalAggregator


@pytest.fixture
def aggregator():
    return SignalAggregator(min_confidence=0.6, min_agreement=2)


def _ts():
    return datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc)


def test_aggregate_strong_buy(aggregator):
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": 1, "confidence": 0.75},
        {"horizon_minutes": 30, "direction": 1, "confidence": 0.70},
    ]
    signal = aggregator.aggregate("SPY", _ts(), predictions)

    assert signal is not None
    assert signal.direction == 1
    assert signal.strength > 0.5
    assert signal.contributing_predictions == 3


def test_aggregate_strong_sell(aggregator):
    predictions = [
        {"horizon_minutes": 5, "direction": -1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": -1, "confidence": 0.70},
    ]
    signal = aggregator.aggregate("SPY", _ts(), predictions)

    assert signal is not None
    assert signal.direction == -1
    assert signal.strength > 0.5
    assert signal.contributing_predictions == 2


def test_aggregate_mixed_produces_flat(aggregator):
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.75},
        {"horizon_minutes": 10, "direction": -1, "confidence": 0.75},
    ]
    signal = aggregator.aggregate("SPY", _ts(), predictions)

    assert signal is not None
    assert signal.direction == 0


def test_aggregate_filters_low_confidence(aggregator):
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": 1, "confidence": 0.70},
        {"horizon_minutes": 30, "direction": -1, "confidence": 0.40},  # below threshold
    ]
    signal = aggregator.aggregate("SPY", _ts(), predictions)

    assert signal is not None
    assert signal.direction == 1  # low-confidence -1 filtered out
    assert signal.contributing_predictions == 2


def test_aggregate_insufficient_agreement(aggregator):
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.80},
        {"horizon_minutes": 10, "direction": -1, "confidence": 0.40},  # below threshold
    ]
    # Only 1 confident prediction, min_agreement=2
    signal = aggregator.aggregate("SPY", _ts(), predictions)

    assert signal is None


def test_aggregate_empty_predictions(aggregator):
    signal = aggregator.aggregate("SPY", _ts(), [])
    assert signal is None


def test_aggregate_all_below_confidence(aggregator):
    predictions = [
        {"horizon_minutes": 5, "direction": 1, "confidence": 0.40},
        {"horizon_minutes": 10, "direction": 1, "confidence": 0.50},
    ]
    signal = aggregator.aggregate("SPY", _ts(), predictions)
    assert signal is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_signal_aggregator.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'services.execution_service'`

- [ ] **Step 3: Write the implementation**

Create empty `services/execution_service/__init__.py`.

Create `services/execution_service/signal_aggregator.py`:

```python
from __future__ import annotations

from datetime import datetime
from typing import Optional

from shared.models import TradingSignal


class SignalAggregator:
    def __init__(self, min_confidence: float = 0.6, min_agreement: int = 2):
        self._min_confidence = min_confidence
        self._min_agreement = min_agreement

    def aggregate(
        self,
        symbol: str,
        timestamp: datetime,
        predictions: list[dict],
    ) -> Optional[TradingSignal]:
        if not predictions:
            return None

        confident = [
            p for p in predictions if p["confidence"] >= self._min_confidence
        ]
        if len(confident) < self._min_agreement:
            return None

        weighted_sum = sum(
            p["direction"] * p["confidence"] for p in confident
        )
        total_weight = sum(p["confidence"] for p in confident)
        avg = weighted_sum / total_weight

        if avg > 0.3:
            direction = 1
        elif avg < -0.3:
            direction = -1
        else:
            direction = 0

        strength = min(abs(avg), 1.0)

        return TradingSignal(
            symbol=symbol,
            timestamp=timestamp,
            direction=direction,
            strength=strength,
            contributing_predictions=len(confident),
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_signal_aggregator.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add services/execution_service/__init__.py services/execution_service/signal_aggregator.py tests/test_signal_aggregator.py
git commit -m "feat: signal aggregator with confidence-weighted voting"
```

---

## Task 6: Alpaca Broker Wrapper

**Files:**
- Create: `services/execution_service/alpaca_broker.py`
- Create: `tests/test_alpaca_broker.py`

The broker wraps Alpaca's synchronous `TradingClient` with async methods using `asyncio.to_thread`. All public methods are async; sync helpers are prefixed with `_sync_`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_alpaca_broker.py`:

```python
import pytest
import pytest_asyncio
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, AsyncMock
from services.execution_service.alpaca_broker import AlpacaBroker


@pytest.fixture
def mock_settings():
    s = MagicMock()
    s.alpaca_api_key = "test_key"
    s.alpaca_secret_key = "test_secret"
    return s


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def broker(mock_settings, mock_client):
    with patch(
        "services.execution_service.alpaca_broker.TradingClient",
        return_value=mock_client,
    ):
        return AlpacaBroker(mock_settings)


@pytest.mark.asyncio
async def test_get_account(broker, mock_client):
    mock_account = MagicMock()
    mock_account.equity = "100000.00"
    mock_account.buying_power = "200000.00"
    mock_account.cash = "100000.00"
    mock_account.portfolio_value = "100000.00"
    mock_client.get_account.return_value = mock_account

    result = await broker.get_account()

    assert result["equity"] == 100000.0
    assert result["buying_power"] == 200000.0
    assert result["cash"] == 100000.0
    assert result["portfolio_value"] == 100000.0


@pytest.mark.asyncio
async def test_get_positions(broker, mock_client):
    mock_pos = MagicMock()
    mock_pos.symbol = "SPY"
    mock_pos.qty = "10"
    mock_pos.side = "long"
    mock_pos.avg_entry_price = "420.00"
    mock_pos.current_price = "425.00"
    mock_pos.unrealized_pl = "50.00"
    mock_pos.market_value = "4250.00"
    mock_client.get_all_positions.return_value = [mock_pos]

    result = await broker.get_positions()

    assert len(result) == 1
    assert result[0]["symbol"] == "SPY"
    assert result[0]["qty"] == 10.0
    assert result[0]["avg_entry_price"] == 420.0
    assert result[0]["unrealized_pl"] == 50.0


@pytest.mark.asyncio
async def test_get_position_found(broker, mock_client):
    mock_pos = MagicMock()
    mock_pos.symbol = "SPY"
    mock_pos.qty = "10"
    mock_pos.side = "long"
    mock_pos.avg_entry_price = "420.00"
    mock_pos.current_price = "425.00"
    mock_pos.unrealized_pl = "50.00"
    mock_pos.market_value = "4250.00"
    mock_client.get_open_position.return_value = mock_pos

    result = await broker.get_position("SPY")

    assert result is not None
    assert result["symbol"] == "SPY"


@pytest.mark.asyncio
async def test_get_position_not_found(broker, mock_client):
    mock_client.get_open_position.side_effect = Exception("position does not exist")

    result = await broker.get_position("AAPL")

    assert result is None


@pytest.mark.asyncio
async def test_submit_market_order(broker, mock_client):
    mock_order = MagicMock()
    mock_order.id = "order-abc-123"
    mock_client.submit_order.return_value = mock_order

    result = await broker.submit_market_order("SPY", 10, "buy")

    assert result == "order-abc-123"
    mock_client.submit_order.assert_called_once()


@pytest.mark.asyncio
async def test_get_order(broker, mock_client):
    mock_order = MagicMock()
    mock_order.id = "order-abc-123"
    mock_order.status = "filled"
    mock_order.filled_qty = "10"
    mock_order.filled_avg_price = "420.50"
    mock_order.filled_at = datetime(2026, 4, 12, 14, 0, 1, tzinfo=timezone.utc)
    mock_client.get_order_by_id.return_value = mock_order

    result = await broker.get_order("order-abc-123")

    assert result["id"] == "order-abc-123"
    assert result["status"] == "filled"
    assert result["filled_qty"] == 10.0
    assert result["filled_avg_price"] == 420.50
    assert result["filled_at"] == datetime(2026, 4, 12, 14, 0, 1, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_close_position(broker, mock_client):
    mock_order = MagicMock()
    mock_order.id = "close-order-456"
    mock_client.close_position.return_value = mock_order

    result = await broker.close_position("SPY")

    assert result == "close-order-456"


@pytest.mark.asyncio
async def test_close_position_fails(broker, mock_client):
    mock_client.close_position.side_effect = Exception("no position")

    result = await broker.close_position("SPY")

    assert result is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_alpaca_broker.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'services.execution_service.alpaca_broker'`

- [ ] **Step 3: Write the implementation**

Create `services/execution_service/alpaca_broker.py`:

```python
from __future__ import annotations

import asyncio
import logging
from datetime import timezone
from typing import Optional

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from shared.config import Settings

logger = logging.getLogger(__name__)


class AlpacaBroker:
    def __init__(self, settings: Settings):
        self._client = TradingClient(
            api_key=settings.alpaca_api_key,
            secret_key=settings.alpaca_secret_key,
            paper=True,
        )

    # --- Async public API (delegates to sync via to_thread) ---

    async def get_account(self) -> dict:
        return await asyncio.to_thread(self._sync_get_account)

    async def get_positions(self) -> list[dict]:
        return await asyncio.to_thread(self._sync_get_positions)

    async def get_position(self, symbol: str) -> Optional[dict]:
        return await asyncio.to_thread(self._sync_get_position, symbol)

    async def submit_market_order(
        self, symbol: str, qty: float, side: str
    ) -> str:
        return await asyncio.to_thread(
            self._sync_submit_market_order, symbol, qty, side,
        )

    async def get_order(self, alpaca_order_id: str) -> dict:
        return await asyncio.to_thread(self._sync_get_order, alpaca_order_id)

    async def close_position(self, symbol: str) -> Optional[str]:
        return await asyncio.to_thread(self._sync_close_position, symbol)

    # --- Sync helpers ---

    def _sync_get_account(self) -> dict:
        account = self._client.get_account()
        return {
            "equity": float(account.equity),
            "buying_power": float(account.buying_power),
            "cash": float(account.cash),
            "portfolio_value": float(account.portfolio_value),
        }

    def _sync_get_positions(self) -> list[dict]:
        positions = self._client.get_all_positions()
        return [self._format_position(p) for p in positions]

    def _sync_get_position(self, symbol: str) -> Optional[dict]:
        try:
            p = self._client.get_open_position(symbol)
            return self._format_position(p)
        except Exception:
            return None

    def _sync_submit_market_order(
        self, symbol: str, qty: float, side: str
    ) -> str:
        order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
        request = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=order_side,
            time_in_force=TimeInForce.DAY,
        )
        order = self._client.submit_order(request)
        return str(order.id)

    def _sync_get_order(self, alpaca_order_id: str) -> dict:
        order = self._client.get_order_by_id(alpaca_order_id)
        filled_at = None
        if order.filled_at:
            filled_at = order.filled_at
            if filled_at.tzinfo is None:
                filled_at = filled_at.replace(tzinfo=timezone.utc)
        return {
            "id": str(order.id),
            "status": str(order.status),
            "filled_qty": float(order.filled_qty) if order.filled_qty else None,
            "filled_avg_price": float(order.filled_avg_price) if order.filled_avg_price else None,
            "filled_at": filled_at,
        }

    def _sync_close_position(self, symbol: str) -> Optional[str]:
        try:
            order = self._client.close_position(symbol)
            return str(order.id) if order else None
        except Exception as e:
            logger.warning("Failed to close position %s: %s", symbol, e)
            return None

    @staticmethod
    def _format_position(p) -> dict:
        return {
            "symbol": p.symbol,
            "qty": float(p.qty),
            "side": str(p.side),
            "avg_entry_price": float(p.avg_entry_price),
            "current_price": float(p.current_price),
            "unrealized_pl": float(p.unrealized_pl),
            "market_value": float(p.market_value),
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_alpaca_broker.py -v`
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add services/execution_service/alpaca_broker.py tests/test_alpaca_broker.py
git commit -m "feat: Alpaca broker wrapper with async-safe TradingClient"
```

---

## Task 7: Risk Manager

**Files:**
- Create: `services/execution_service/risk_manager.py`
- Create: `tests/test_risk_manager.py`

The risk manager is stateless — it takes account/position data as arguments and returns approval decisions. Long-only by default.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_risk_manager.py`:

```python
import pytest
from datetime import datetime, timezone
from shared.models import TradingSignal
from services.execution_service.risk_manager import RiskManager


@pytest.fixture
def rm():
    return RiskManager(
        max_position_value=10000.0,
        max_total_positions=3,
        max_portfolio_exposure=0.5,
        min_signal_strength=0.5,
    )


def _signal(direction=1, strength=0.8):
    return TradingSignal(
        symbol="SPY",
        timestamp=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        direction=direction,
        strength=strength,
        contributing_predictions=3,
    )


def _account(equity=100000.0, buying_power=200000.0):
    return {"equity": equity, "buying_power": buying_power}


def _positions(*symbols):
    return [
        {"symbol": s, "qty": 10.0, "market_value": 4200.0} for s in symbols
    ]


def test_check_entry_approved(rm):
    approved, reason, qty = rm.check_entry(
        _signal(), _account(), [], current_price=420.0,
    )
    assert approved is True
    assert qty == 23  # int(10000 / 420)
    assert reason == "Approved"


def test_check_entry_weak_signal(rm):
    approved, reason, qty = rm.check_entry(
        _signal(strength=0.3), _account(), [], current_price=420.0,
    )
    assert approved is False
    assert "weak" in reason.lower()
    assert qty == 0


def test_check_entry_flat_signal(rm):
    approved, reason, qty = rm.check_entry(
        _signal(direction=0), _account(), [], current_price=420.0,
    )
    assert approved is False
    assert qty == 0


def test_check_entry_short_allowed(rm):
    approved, reason, qty = rm.check_entry(
        _signal(direction=-1), _account(), [], current_price=420.0,
    )
    assert approved is True
    assert qty == 23  # int(10000 / 420)
    assert reason == "Approved"


def test_check_entry_existing_position(rm):
    approved, reason, qty = rm.check_entry(
        _signal(), _account(), _positions("SPY"), current_price=420.0,
    )
    assert approved is False
    assert "position" in reason.lower()


def test_check_entry_max_positions(rm):
    approved, reason, qty = rm.check_entry(
        _signal(), _account(), _positions("QQQ", "AAPL", "MSFT"),
        current_price=420.0,
    )
    assert approved is False
    assert "max" in reason.lower()


def test_check_entry_max_exposure(rm):
    # Positions with large market values exceeding 50% of equity
    big_positions = [
        {"symbol": "QQQ", "qty": 100.0, "market_value": 30000.0},
        {"symbol": "AAPL", "qty": 100.0, "market_value": 25000.0},
    ]
    approved, reason, qty = rm.check_entry(
        _signal(), _account(), big_positions, current_price=420.0,
    )
    assert approved is False
    assert "exposure" in reason.lower()


def test_check_exit_opposing_signal(rm):
    position = {"symbol": "SPY", "qty": 10.0}
    should_exit, reason = rm.check_exit(_signal(direction=-1), position)
    assert should_exit is True
    assert "opposing" in reason.lower()


def test_check_exit_flat_signal(rm):
    position = {"symbol": "SPY", "qty": 10.0}
    should_exit, reason = rm.check_exit(_signal(direction=0, strength=0.0), position)
    assert should_exit is True
    assert "flat" in reason.lower()


def test_check_exit_same_direction(rm):
    position = {"symbol": "SPY", "qty": 10.0}
    should_exit, reason = rm.check_exit(_signal(direction=1), position)
    assert should_exit is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_risk_manager.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'services.execution_service.risk_manager'`

- [ ] **Step 3: Write the implementation**

Create `services/execution_service/risk_manager.py`:

```python
from __future__ import annotations

import logging
from typing import Tuple

from shared.models import TradingSignal

logger = logging.getLogger(__name__)


class RiskManager:
    def __init__(
        self,
        max_position_value: float = 10000.0,
        max_total_positions: int = 5,
        max_portfolio_exposure: float = 0.5,
        min_signal_strength: float = 0.5,
    ):
        self._max_position_value = max_position_value
        self._max_total_positions = max_total_positions
        self._max_portfolio_exposure = max_portfolio_exposure
        self._min_signal_strength = min_signal_strength

    def check_entry(
        self,
        signal: TradingSignal,
        account: dict,
        positions: list[dict],
        current_price: float,
    ) -> Tuple[bool, str, int]:
        """
        Check whether a new position entry is allowed.
        Returns (approved, reason, quantity).
        """
        if signal.direction == 0:
            return False, "Flat signal, no entry", 0

        if signal.strength < self._min_signal_strength:
            return False, f"Signal too weak ({signal.strength:.2f})", 0

        for pos in positions:
            if pos["symbol"] == signal.symbol:
                return False, "Already have position in this symbol", 0

        if len(positions) >= self._max_total_positions:
            return False, "Max positions reached", 0

        total_exposure = sum(abs(p["market_value"]) for p in positions)
        equity = account["equity"]
        if equity > 0 and total_exposure / equity >= self._max_portfolio_exposure:
            return False, "Max portfolio exposure reached", 0

        qty = int(self._max_position_value / current_price)
        if qty <= 0:
            return False, "Position value too small for one share", 0

        return True, "Approved", qty

    def check_exit(
        self,
        signal: TradingSignal,
        position: dict,
    ) -> Tuple[bool, str]:
        """
        Check whether an existing position should be closed.
        Returns (should_exit, reason).
        """
        pos_qty = float(position["qty"])
        pos_direction = 1 if pos_qty > 0 else -1

        if signal.direction == -pos_direction:
            return True, "Opposing signal"

        if signal.direction == 0:
            return True, "Flat signal"

        return False, "Signal matches position"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_risk_manager.py -v`
Expected: 10 passed

- [ ] **Step 5: Commit**

```bash
git add services/execution_service/risk_manager.py tests/test_risk_manager.py
git commit -m "feat: risk manager with position limits and exposure checks"
```

---

## Task 8: Position Tracker

**Files:**
- Create: `services/execution_service/position_tracker.py`
- Create: `tests/test_position_tracker.py`

The position tracker caches account and position state from Alpaca, refreshed on demand.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_position_tracker.py`:

```python
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from services.execution_service.position_tracker import PositionTracker


@pytest.fixture
def mock_broker():
    broker = AsyncMock()
    broker.get_account = AsyncMock(return_value={
        "equity": 100000.0,
        "buying_power": 200000.0,
        "cash": 100000.0,
        "portfolio_value": 100000.0,
    })
    broker.get_positions = AsyncMock(return_value=[
        {
            "symbol": "SPY",
            "qty": 10.0,
            "side": "long",
            "avg_entry_price": 420.0,
            "current_price": 425.0,
            "unrealized_pl": 50.0,
            "market_value": 4250.0,
        },
    ])
    return broker


@pytest.mark.asyncio
async def test_refresh_updates_state(mock_broker):
    tracker = PositionTracker(mock_broker)
    await tracker.refresh()

    assert tracker.account["equity"] == 100000.0
    assert len(tracker.positions) == 1
    assert "SPY" in tracker.positions


@pytest.mark.asyncio
async def test_get_position_found(mock_broker):
    tracker = PositionTracker(mock_broker)
    await tracker.refresh()

    pos = tracker.get_position("SPY")
    assert pos is not None
    assert pos["qty"] == 10.0


@pytest.mark.asyncio
async def test_get_position_not_found(mock_broker):
    tracker = PositionTracker(mock_broker)
    await tracker.refresh()

    pos = tracker.get_position("AAPL")
    assert pos is None


@pytest.mark.asyncio
async def test_has_position(mock_broker):
    tracker = PositionTracker(mock_broker)
    await tracker.refresh()

    assert tracker.has_position("SPY") is True
    assert tracker.has_position("AAPL") is False


@pytest.mark.asyncio
async def test_initial_state_empty():
    broker = AsyncMock()
    tracker = PositionTracker(broker)

    assert tracker.account == {}
    assert tracker.positions == {}
    assert tracker.get_position("SPY") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_position_tracker.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'services.execution_service.position_tracker'`

- [ ] **Step 3: Write the implementation**

Create `services/execution_service/position_tracker.py`:

```python
from __future__ import annotations

from typing import Optional

from services.execution_service.alpaca_broker import AlpacaBroker


class PositionTracker:
    def __init__(self, broker: AlpacaBroker):
        self._broker = broker
        self._positions: dict[str, dict] = {}
        self._account: dict = {}

    async def refresh(self) -> None:
        self._account = await self._broker.get_account()
        positions_list = await self._broker.get_positions()
        self._positions = {p["symbol"]: p for p in positions_list}

    @property
    def account(self) -> dict:
        return dict(self._account)

    @property
    def positions(self) -> dict[str, dict]:
        return dict(self._positions)

    def get_position(self, symbol: str) -> Optional[dict]:
        return self._positions.get(symbol)

    def has_position(self, symbol: str) -> bool:
        return symbol in self._positions
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_position_tracker.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add services/execution_service/position_tracker.py tests/test_position_tracker.py
git commit -m "feat: position tracker caching Alpaca account and positions"
```

---

## Task 9: Order Executor

**Files:**
- Create: `services/execution_service/order_executor.py`
- Create: `tests/test_order_executor.py`

The order executor orchestrates the full flow: receives a signal, refreshes positions, checks risk, submits orders, tracks fills, and records everything in the DB. It uses `dataclasses.replace` to create updated frozen Order instances.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_order_executor.py`:

```python
import pytest
import pytest_asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from shared.models import TradingSignal, Order
from services.execution_service.order_executor import OrderExecutor


def _signal(direction=1, strength=0.8, symbol="SPY"):
    return TradingSignal(
        symbol=symbol,
        timestamp=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        direction=direction,
        strength=strength,
        contributing_predictions=3,
    )


@pytest.fixture
def mock_broker():
    broker = AsyncMock()
    broker.submit_market_order = AsyncMock(return_value="alp-order-123")
    broker.get_order = AsyncMock(return_value={
        "id": "alp-order-123",
        "status": "filled",
        "filled_qty": 10.0,
        "filled_avg_price": 420.50,
        "filled_at": datetime(2026, 4, 12, 14, 0, 1, tzinfo=timezone.utc),
    })
    broker.close_position = AsyncMock(return_value="alp-close-456")
    return broker


@pytest.fixture
def mock_risk():
    risk = MagicMock()
    risk.check_entry = MagicMock(return_value=(True, "Approved", 10))
    risk.check_exit = MagicMock(return_value=(False, "Signal matches position"))
    return risk


@pytest.fixture
def mock_tracker():
    tracker = AsyncMock()
    tracker.refresh = AsyncMock()
    tracker.account = {"equity": 100000.0, "buying_power": 200000.0}
    tracker.positions = {}
    tracker.get_position = MagicMock(return_value=None)
    return tracker


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.insert_order = AsyncMock(return_value=42)
    db.update_order_status = AsyncMock()
    return db


@pytest.fixture
def executor(mock_broker, mock_risk, mock_tracker, mock_db):
    return OrderExecutor(
        broker=mock_broker,
        risk_manager=mock_risk,
        position_tracker=mock_tracker,
        db=mock_db,
        fill_check_delay=0,
    )


@pytest.mark.asyncio
async def test_process_signal_enters_long(executor, mock_broker, mock_db):
    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is not None
    assert order.side == "buy"
    assert order.symbol == "SPY"
    assert order.status == "filled"
    assert order.filled_price == 420.50
    assert order.id == 42
    mock_broker.submit_market_order.assert_called_once_with("SPY", 10, "buy")
    mock_db.insert_order.assert_called_once()
    mock_db.update_order_status.assert_called_once()


@pytest.mark.asyncio
async def test_process_signal_exits_on_opposing(
    executor, mock_broker, mock_db, mock_risk, mock_tracker,
):
    mock_tracker.get_position = MagicMock(
        return_value={"symbol": "SPY", "qty": 10.0, "market_value": 4200.0},
    )
    mock_risk.check_exit = MagicMock(return_value=(True, "Opposing signal"))

    order = await executor.process_signal(
        _signal(direction=-1), current_price=420.0,
    )

    assert order is not None
    assert order.side == "sell"
    mock_broker.close_position.assert_called_once_with("SPY")


@pytest.mark.asyncio
async def test_process_signal_exits_on_flat(
    executor, mock_broker, mock_db, mock_risk, mock_tracker,
):
    mock_tracker.get_position = MagicMock(
        return_value={"symbol": "SPY", "qty": 10.0, "market_value": 4200.0},
    )
    mock_risk.check_exit = MagicMock(return_value=(True, "Flat signal"))

    order = await executor.process_signal(
        _signal(direction=0, strength=0.0), current_price=420.0,
    )

    assert order is not None
    assert order.side == "sell"


@pytest.mark.asyncio
async def test_process_signal_rejected_by_risk(
    executor, mock_broker, mock_risk,
):
    mock_risk.check_entry = MagicMock(
        return_value=(False, "Max positions reached", 0),
    )

    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is None
    mock_broker.submit_market_order.assert_not_called()


@pytest.mark.asyncio
async def test_process_signal_no_action_flat_no_position(executor, mock_broker):
    order = await executor.process_signal(
        _signal(direction=0, strength=0.0), current_price=420.0,
    )

    assert order is None
    mock_broker.submit_market_order.assert_not_called()


@pytest.mark.asyncio
async def test_process_signal_no_action_same_direction(
    executor, mock_broker, mock_risk, mock_tracker,
):
    mock_tracker.get_position = MagicMock(
        return_value={"symbol": "SPY", "qty": 10.0, "market_value": 4200.0},
    )
    mock_risk.check_exit = MagicMock(
        return_value=(False, "Signal matches position"),
    )

    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is None
    mock_broker.submit_market_order.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_order_executor.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'services.execution_service.order_executor'`

- [ ] **Step 3: Write the implementation**

Create `services/execution_service/order_executor.py`:

```python
from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from shared.db import Database
from shared.models import Order, TradingSignal
from shared.redis import RedisClient
from services.execution_service.alpaca_broker import AlpacaBroker
from services.execution_service.risk_manager import RiskManager
from services.execution_service.position_tracker import PositionTracker

logger = logging.getLogger(__name__)


class OrderExecutor:
    def __init__(
        self,
        broker: AlpacaBroker,
        risk_manager: RiskManager,
        position_tracker: PositionTracker,
        db: Database,
        redis_client: Optional[RedisClient] = None,
        fill_check_delay: float = 1.0,
    ):
        self._broker = broker
        self._risk = risk_manager
        self._tracker = position_tracker
        self._db = db
        self._redis = redis_client
        self._fill_check_delay = fill_check_delay

    async def process_signal(
        self, signal: TradingSignal, current_price: float,
    ) -> Optional[Order]:
        await self._tracker.refresh()
        existing = self._tracker.get_position(signal.symbol)

        if existing:
            should_exit, reason = self._risk.check_exit(signal, existing)
            if should_exit:
                return await self._execute_close(signal, existing)
            return None

        if signal.direction == 0:
            return None

        account = self._tracker.account
        positions = list(self._tracker.positions.values())
        approved, reason, qty = self._risk.check_entry(
            signal, account, positions, current_price,
        )

        if not approved:
            logger.info("Entry rejected for %s: %s", signal.symbol, reason)
            return None

        return await self._execute_open(signal, qty)

    async def _execute_open(
        self, signal: TradingSignal, qty: int,
    ) -> Order:
        side = "buy" if signal.direction == 1 else "sell"
        alpaca_id = await self._broker.submit_market_order(
            signal.symbol, qty, side,
        )

        now = datetime.now(timezone.utc)
        order = Order(
            symbol=signal.symbol,
            side=side,
            quantity=float(qty),
            order_type="market",
            submitted_at=now,
            signal_strength=signal.strength,
            alpaca_order_id=alpaca_id,
            status="submitted",
        )
        order_id = await self._db.insert_order(order)

        filled_order = await self._check_fill(order, order_id)
        await self._publish_order_event(filled_order)
        return filled_order

    async def _execute_close(
        self, signal: TradingSignal, position: dict,
    ) -> Optional[Order]:
        alpaca_id = await self._broker.close_position(signal.symbol)
        if not alpaca_id:
            logger.warning("Failed to close position for %s", signal.symbol)
            return None

        qty = abs(float(position["qty"]))
        side = "sell" if float(position["qty"]) > 0 else "buy"
        now = datetime.now(timezone.utc)

        order = Order(
            symbol=signal.symbol,
            side=side,
            quantity=qty,
            order_type="market",
            submitted_at=now,
            signal_strength=signal.strength,
            alpaca_order_id=alpaca_id,
            status="submitted",
        )
        order_id = await self._db.insert_order(order)

        filled_order = await self._check_fill(order, order_id)
        await self._publish_order_event(filled_order)
        return filled_order

    async def _check_fill(self, order: Order, order_id: int) -> Order:
        if self._fill_check_delay > 0:
            await asyncio.sleep(self._fill_check_delay)

        alpaca_order = await self._broker.get_order(order.alpaca_order_id)
        status = alpaca_order["status"]

        if status in ("filled", "partially_filled"):
            await self._db.update_order_status(
                order_id,
                status=status,
                filled_at=alpaca_order.get("filled_at"),
                filled_price=alpaca_order.get("filled_avg_price"),
                filled_quantity=alpaca_order.get("filled_qty"),
            )
            return dataclasses.replace(
                order,
                status=status,
                filled_at=alpaca_order.get("filled_at"),
                filled_price=alpaca_order.get("filled_avg_price"),
                filled_quantity=alpaca_order.get("filled_qty"),
                id=order_id,
            )

        return dataclasses.replace(order, id=order_id)

    async def _publish_order_event(self, order: Order) -> None:
        if not self._redis:
            return
        await self._redis.publish(
            "execution:orders",
            json.dumps({
                "symbol": order.symbol,
                "side": order.side,
                "quantity": order.quantity,
                "status": order.status,
                "filled_price": order.filled_price,
            }),
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_order_executor.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add services/execution_service/order_executor.py tests/test_order_executor.py
git commit -m "feat: order executor orchestrating signal-to-trade pipeline"
```

---

## Task 10: Service Entrypoint, Health, Dockerfile, Docker Compose

**Files:**
- Create: `services/execution_service/health.py`
- Create: `services/execution_service/main.py`
- Create: `services/execution_service/Dockerfile`
- Create: `services/execution_service/requirements.txt`
- Modify: `docker-compose.yml` — add execution_service
- Modify: `.env.example` — already updated in Task 3

- [ ] **Step 1: Write the failing test for health endpoint**

Create `tests/test_execution_health.py`:

```python
import pytest
from unittest.mock import AsyncMock, MagicMock
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop
from services.execution_service.health import create_health_app


@pytest.fixture
def mock_db():
    db = MagicMock()
    pool = MagicMock()
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=None)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx
    db.pool = pool
    return db


@pytest.fixture
def mock_redis():
    redis_client = MagicMock()
    redis_client.client = AsyncMock()
    redis_client.client.ping = AsyncMock(return_value=True)
    return redis_client


@pytest.fixture
def mock_broker():
    broker = AsyncMock()
    broker.get_account = AsyncMock(return_value={
        "equity": 100000.0,
        "buying_power": 200000.0,
        "cash": 100000.0,
        "portfolio_value": 100000.0,
    })
    return broker


@pytest.mark.asyncio
async def test_health_endpoint_ok(aiohttp_client, mock_db, mock_redis, mock_broker):
    app = create_health_app(mock_db, mock_redis, mock_broker)
    client = await aiohttp_client(app)

    resp = await client.get("/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["db"] == "ok"
    assert data["redis"] == "ok"
    assert data["alpaca"] == "ok"


@pytest.mark.asyncio
async def test_health_endpoint_degraded(aiohttp_client, mock_db, mock_redis, mock_broker):
    mock_broker.get_account = AsyncMock(side_effect=Exception("connection refused"))

    app = create_health_app(mock_db, mock_redis, mock_broker)
    client = await aiohttp_client(app)

    resp = await client.get("/health")
    assert resp.status == 503
    data = await resp.json()
    assert data["db"] == "ok"
    assert "error" in data["alpaca"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_execution_health.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'services.execution_service.health'`

- [ ] **Step 3: Write the health endpoint**

Create `services/execution_service/health.py`:

```python
from __future__ import annotations

from aiohttp import web

from shared.db import Database
from shared.redis import RedisClient
from services.execution_service.alpaca_broker import AlpacaBroker


def create_health_app(
    db: Database,
    redis_client: RedisClient,
    broker: AlpacaBroker,
) -> web.Application:
    app = web.Application()
    app["db"] = db
    app["redis"] = redis_client
    app["broker"] = broker
    app.router.add_get("/health", health_handler)
    return app


async def health_handler(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    redis_client: RedisClient = request.app["redis"]
    broker: AlpacaBroker = request.app["broker"]

    checks = {}

    try:
        async with db.pool.acquire() as conn:
            await conn.execute("SELECT 1")
        checks["db"] = "ok"
    except Exception as e:
        checks["db"] = f"error: {e}"

    try:
        await redis_client.client.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {e}"

    try:
        await broker.get_account()
        checks["alpaca"] = "ok"
    except Exception as e:
        checks["alpaca"] = f"error: {e}"

    all_ok = all(v == "ok" for v in checks.values())
    status = 200 if all_ok else 503
    return web.json_response(checks, status=status)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_execution_health.py -v`
Expected: 2 passed

- [ ] **Step 5: Write the service entrypoint**

Create `services/execution_service/main.py`:

```python
from __future__ import annotations

import asyncio
import json
import logging
import signal
from datetime import datetime, timezone

from aiohttp import web

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from services.execution_service.alpaca_broker import AlpacaBroker
from services.execution_service.signal_aggregator import SignalAggregator
from services.execution_service.risk_manager import RiskManager
from services.execution_service.position_tracker import PositionTracker
from services.execution_service.order_executor import OrderExecutor
from services.execution_service.health import create_health_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("execution_service")


async def main() -> None:
    settings = Settings()
    db = Database(settings)
    redis_client = RedisClient(settings)

    logger.info("Connecting to TimescaleDB...")
    await db.connect()
    logger.info("Connecting to Redis...")
    await redis_client.connect()

    broker = AlpacaBroker(settings)
    aggregator = SignalAggregator(
        min_confidence=settings.execution_min_confidence,
        min_agreement=settings.execution_min_agreement,
    )
    risk_manager = RiskManager(
        max_position_value=settings.execution_max_position_value,
        max_total_positions=settings.execution_max_positions,
        max_portfolio_exposure=settings.execution_max_exposure,
        min_signal_strength=settings.execution_min_signal_strength,
    )
    position_tracker = PositionTracker(broker)
    executor = OrderExecutor(
        broker=broker,
        risk_manager=risk_manager,
        position_tracker=position_tracker,
        db=db,
        redis_client=redis_client,
    )

    async def on_prediction(message: str) -> None:
        data = json.loads(message)
        symbol = data["symbol"]
        timeframe = data["timeframe"]
        predictions = data["predictions"]

        signal_obj = aggregator.aggregate(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            predictions=predictions,
        )
        if signal_obj is None:
            logger.debug("No actionable signal for %s %s", symbol, timeframe)
            return

        bars = await db.query_bars(symbol, "1m", limit=1)
        if not bars:
            logger.warning("No price data for %s", symbol)
            return
        current_price = bars[0].close

        order = await executor.process_signal(signal_obj, current_price)
        if order:
            logger.info(
                "Executed: %s %s %s qty=%.1f status=%s",
                order.side, order.symbol, order.order_type,
                order.quantity, order.status,
            )

    await redis_client.subscribe("model:predictions", on_prediction)
    logger.info("Subscribed to model:predictions")

    health_app = create_health_app(db, redis_client, broker)
    runner = web.AppRunner(health_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8082)
    await site.start()
    logger.info("Health check server running on :8082")

    shutdown_event = asyncio.Event()

    def handle_signal():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    logger.info("Execution service running. Press Ctrl+C to stop.")

    try:
        await shutdown_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down...")
        await runner.cleanup()
        await redis_client.disconnect()
        await db.disconnect()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 6: Write the Dockerfile**

Create `services/execution_service/Dockerfile`:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY services/execution_service/requirements.txt /app/services/execution_service/requirements.txt
RUN pip install --no-cache-dir -r services/execution_service/requirements.txt

# Copy shared code and service code
COPY shared/ /app/shared/
COPY migrations/ /app/migrations/
COPY services/__init__.py /app/services/__init__.py
COPY services/execution_service/ /app/services/execution_service/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "services.execution_service.main"]
```

- [ ] **Step 7: Write requirements.txt**

Create `services/execution_service/requirements.txt`:

```
alpaca-py>=0.30.0
asyncpg>=0.29.0
redis>=5.0.0
aiohttp>=3.9.0
```

- [ ] **Step 8: Update docker-compose.yml**

Add the `execution_service` service after `model_service`, and no new volumes needed:

```yaml
  execution_service:
    build:
      context: .
      dockerfile: services/execution_service/Dockerfile
    env_file:
      - .env
    environment:
      TIMESCALEDB_HOST: timescaledb
      REDIS_HOST: redis
    ports:
      - "18082:8082"
    depends_on:
      timescaledb:
        condition: service_healthy
      redis:
        condition: service_healthy
      model_service:
        condition: service_started
    restart: unless-stopped
```

- [ ] **Step 9: Run full test suite to verify no regressions**

Run: `cd bullforge && python -m pytest tests/ -v`
Expected: All tests pass (existing + new)

- [ ] **Step 10: Commit**

```bash
git add services/execution_service/health.py services/execution_service/main.py services/execution_service/Dockerfile services/execution_service/requirements.txt docker-compose.yml tests/test_execution_health.py
git commit -m "feat: execution service entrypoint with Docker Compose integration"
```
