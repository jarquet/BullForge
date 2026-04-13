# Phase 5A: Monitoring Service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a monitoring service that continuously computes trading metrics, tracks per-model fidelity against backtested performance, implements a 4-tier automatic degradation/halt system, and publishes model status to Redis so the signal aggregator can exclude underperforming models.

**Architecture:** A periodic monitoring loop queries the trades and predictions tables to compute rolling metrics (win rate, profit factor, Sharpe ratio, drawdown). A `FidelityTracker` evaluates each model's live prediction accuracy against its backtested accuracy from `model_registry.metrics`. A `HaltManager` implements the 4-tier degradation response (warn → disable model → halt direction → full halt) and publishes model status to Redis `models:status`. The execution service's `SignalAggregator` subscribes to this channel and filters out disabled models before aggregation. Round-trip trades are recorded by the `OrderExecutor` in a new `trades` table, enabling accurate P&L and win-rate metrics. The model service is also updated to include `model_id` in published prediction messages so the aggregator can filter by model.

**Tech Stack:** Python 3.12+, asyncio, asyncpg (from shared/db.py), Redis pub/sub (from shared/redis.py), aiohttp, alpaca-py (read-only account queries), pytest, pytest-asyncio

**Scope note:** Phase 5 is split into three sub-plans. This plan (5A) covers the monitoring service core. Phase 5B (Telegram Bot) and Phase 5C (Dashboard) are separate plans that consume the metrics, alerts, and model status this plan produces.

---

## File Structure

```
bullforge/
  shared/
    config.py                              — (modify) Add 6 monitoring settings
    models.py                              — (modify) Add Trade and ModelStatus dataclasses
    db.py                                  — (modify) Add trade, model status, halt event, metric snapshot, and prediction outcome methods

  services/
    execution_service/
      order_executor.py                    — (modify) Record trades on open/close fills
      signal_aggregator.py                 — (modify) Filter predictions from disabled models

    model_service/
      main.py                              — (modify) Include model_id in published prediction dicts

    monitoring_service/
      __init__.py                          — Empty package marker
      Dockerfile                           — Container image for monitoring service
      requirements.txt                     — asyncpg, redis, aiohttp, alpaca-py
      main.py                              — Entrypoint: periodic monitoring loop
      health.py                            — Health check endpoint (/health on :8083)
      metrics.py                           — MetricsComputer: compute all metric categories
      fidelity.py                          — FidelityTracker: per-model fidelity evaluation
      halt_manager.py                      — HaltManager: 4-tier degradation system

  migrations/
    008_create_trades.sql                  — Round-trip trades table
    009_create_monitoring.sql              — model_status, halt_events, metric_snapshots tables + prediction index

  tests/
    test_monitoring_models.py              — Tests for Trade and ModelStatus dataclasses
    test_monitoring_db.py                  — Tests for all new DB methods
    test_metrics_computer.py               — Tests for MetricsComputer
    test_fidelity_tracker.py               — Tests for FidelityTracker
    test_halt_manager.py                   — Tests for HaltManager
    test_monitoring_health.py              — Tests for monitoring health endpoint
```

---

## Task 1: Monitoring Config Settings

**Files:**
- Modify: `shared/config.py`
- Modify: `tests/test_config.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_config.py`:

```python
def test_monitoring_settings_defaults(settings):
    assert settings.monitoring_port == 8083
    assert settings.monitoring_interval == 60
    assert settings.fidelity_warning_margin == 0.05
    assert settings.fidelity_critical_margin == 0.10
    assert settings.fidelity_min_trades == 20
    assert settings.drawdown_halt_threshold == 0.10
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd bullforge && python -m pytest tests/test_config.py::test_monitoring_settings_defaults -v`
Expected: FAIL — `AttributeError: 'Settings' object has no attribute 'monitoring_port'`

- [ ] **Step 3: Add monitoring settings to Settings**

In `shared/config.py`, append these fields inside the `Settings` class after the execution settings block (after line 116):

```python
    # Monitoring service
    monitoring_port: int = field(
        default_factory=lambda: int(os.getenv("MONITORING_PORT", "8083"))
    )
    monitoring_interval: int = field(
        default_factory=lambda: int(os.getenv("MONITORING_INTERVAL", "60"))
    )
    fidelity_warning_margin: float = field(
        default_factory=lambda: float(os.getenv("FIDELITY_WARNING_MARGIN", "0.05"))
    )
    fidelity_critical_margin: float = field(
        default_factory=lambda: float(os.getenv("FIDELITY_CRITICAL_MARGIN", "0.10"))
    )
    fidelity_min_trades: int = field(
        default_factory=lambda: int(os.getenv("FIDELITY_MIN_TRADES", "20"))
    )
    drawdown_halt_threshold: float = field(
        default_factory=lambda: float(os.getenv("DRAWDOWN_HALT_THRESHOLD", "0.10"))
    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd bullforge && python -m pytest tests/test_config.py -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add shared/config.py tests/test_config.py
git commit -m "feat: add monitoring service config settings"
```

---

## Task 2: Trade and ModelStatus Dataclasses

**Files:**
- Modify: `shared/models.py`
- Create: `tests/test_monitoring_models.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_monitoring_models.py`:

```python
import pytest
from datetime import datetime, timezone


def test_trade_valid():
    from shared.models import Trade

    t = Trade(
        symbol="SPY",
        side="long",
        entry_time=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        entry_price=420.0,
        quantity=10.0,
        entry_order_id=1,
    )
    assert t.symbol == "SPY"
    assert t.side == "long"
    assert t.exit_time is None
    assert t.pnl is None
    assert t.id is None


def test_trade_with_exit():
    from shared.models import Trade

    t = Trade(
        symbol="SPY",
        side="long",
        entry_time=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        entry_price=420.0,
        quantity=10.0,
        entry_order_id=1,
        exit_time=datetime(2026, 4, 12, 15, 0, 0, tzinfo=timezone.utc),
        exit_price=425.0,
        pnl=50.0,
        hold_duration_seconds=3600,
        exit_order_id=2,
        id=42,
    )
    assert t.exit_price == 425.0
    assert t.pnl == 50.0
    assert t.hold_duration_seconds == 3600
    assert t.id == 42


def test_trade_rejects_non_utc_entry_time():
    from shared.models import Trade

    with pytest.raises(ValueError, match="entry_time must be UTC"):
        Trade(
            symbol="SPY",
            side="long",
            entry_time=datetime(2026, 4, 12, 14, 0, 0),
            entry_price=420.0,
            quantity=10.0,
            entry_order_id=1,
        )


def test_trade_rejects_invalid_side():
    from shared.models import Trade

    with pytest.raises(ValueError, match="side must be one of"):
        Trade(
            symbol="SPY",
            side="up",
            entry_time=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            entry_price=420.0,
            quantity=10.0,
            entry_order_id=1,
        )


def test_trade_rejects_non_positive_quantity():
    from shared.models import Trade

    with pytest.raises(ValueError, match="quantity must be positive"):
        Trade(
            symbol="SPY",
            side="long",
            entry_time=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            entry_price=420.0,
            quantity=0,
            entry_order_id=1,
        )


def test_model_status_valid():
    from shared.models import ModelStatus

    ms = ModelStatus(
        model_id=5,
        status="active",
        reason="Within expected range",
        updated_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
    )
    assert ms.model_id == 5
    assert ms.status == "active"
    assert ms.reason == "Within expected range"


def test_model_status_rejects_invalid_status():
    from shared.models import ModelStatus

    with pytest.raises(ValueError, match="status must be one of"):
        ModelStatus(
            model_id=5,
            status="paused",
            reason="test",
            updated_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        )


def test_model_status_rejects_non_utc():
    from shared.models import ModelStatus

    with pytest.raises(ValueError, match="updated_at must be UTC"):
        ModelStatus(
            model_id=5,
            status="active",
            reason="test",
            updated_at=datetime(2026, 4, 12, 14, 0, 0),
        )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_monitoring_models.py -v`
Expected: FAIL — `ImportError: cannot import name 'Trade'`

- [ ] **Step 3: Add Trade and ModelStatus to shared/models.py**

Append after the `Order` class (after line 217) in `shared/models.py`:

```python
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
        if self.entry_time.tzinfo is None or self.entry_time.tzinfo != timezone.utc:
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
            self.exit_time.tzinfo is None or self.exit_time.tzinfo != timezone.utc
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
        if self.updated_at.tzinfo is None or self.updated_at.tzinfo != timezone.utc:
            raise ValueError(
                f"updated_at must be UTC, got tzinfo={self.updated_at.tzinfo}"
            )
        if self.status not in VALID_MODEL_STATUSES:
            raise ValueError(
                f"status must be one of {VALID_MODEL_STATUSES}, got {self.status!r}"
            )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_monitoring_models.py -v`
Expected: ALL PASS (8 tests)

- [ ] **Step 5: Run the full test suite to check for regressions**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add shared/models.py tests/test_monitoring_models.py
git commit -m "feat: add Trade and ModelStatus dataclasses"
```

---

## Task 3: DB Migrations

**Files:**
- Create: `migrations/008_create_trades.sql`
- Create: `migrations/009_create_monitoring.sql`

- [ ] **Step 1: Create the trades migration**

Create `migrations/008_create_trades.sql`:

```sql
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_time TIMESTAMPTZ NOT NULL,
    exit_time TIMESTAMPTZ,
    entry_price DOUBLE PRECISION NOT NULL,
    exit_price DOUBLE PRECISION,
    quantity DOUBLE PRECISION NOT NULL,
    pnl DOUBLE PRECISION,
    hold_duration_seconds INTEGER,
    entry_order_id INTEGER NOT NULL REFERENCES orders(id),
    exit_order_id INTEGER REFERENCES orders(id)
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_exit_time ON trades(exit_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_side ON trades(side);
```

- [ ] **Step 2: Create the monitoring tables migration**

Create `migrations/009_create_monitoring.sql`:

```sql
CREATE TABLE IF NOT EXISTS model_status (
    model_id INTEGER PRIMARY KEY REFERENCES model_registry(id),
    status TEXT NOT NULL DEFAULT 'active',
    reason TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS halt_events (
    id SERIAL PRIMARY KEY,
    tier INTEGER NOT NULL,
    reason TEXT NOT NULL,
    direction TEXT,
    activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_halt_events_active
    ON halt_events(activated_at DESC) WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS metric_snapshots (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    category TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT '',
    window TEXT NOT NULL DEFAULT 'all',
    metrics JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_metric_snapshots_lookup
    ON metric_snapshots(category, scope, window, timestamp DESC);

-- Index for fidelity tracker queries on predictions table
CREATE INDEX IF NOT EXISTS idx_predictions_model_id ON predictions(model_id);
```

- [ ] **Step 3: Commit**

```bash
git add migrations/008_create_trades.sql migrations/009_create_monitoring.sql
git commit -m "feat: add trades, model_status, halt_events, metric_snapshots migrations"
```

---

## Task 4: DB Methods — Trades, Model Status, Metrics & Prediction Outcomes

**Files:**
- Modify: `shared/db.py`
- Create: `tests/test_monitoring_db.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_monitoring_db.py`:

```python
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from shared.models import Trade, ModelStatus


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    conn = AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx
    return pool, conn


def _make_trade():
    return Trade(
        symbol="SPY",
        side="long",
        entry_time=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        entry_price=420.0,
        quantity=10.0,
        entry_order_id=1,
    )


@pytest.mark.asyncio
async def test_insert_trade(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchval = AsyncMock(return_value=1)
    db = Database(settings)
    db.pool = pool

    trade = _make_trade()
    row_id = await db.insert_trade(trade)

    assert row_id == 1
    conn.fetchval.assert_called_once()
    call_args = conn.fetchval.call_args
    assert "INSERT INTO trades" in call_args[0][0]
    assert call_args[0][1] == "SPY"


@pytest.mark.asyncio
async def test_update_trade_exit(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.execute = AsyncMock()
    db = Database(settings)
    db.pool = pool

    exit_time = datetime(2026, 4, 12, 15, 0, 0, tzinfo=timezone.utc)
    await db.update_trade_exit(
        trade_id=1,
        exit_time=exit_time,
        exit_price=425.0,
        exit_order_id=2,
        pnl=50.0,
        hold_duration_seconds=3600,
    )

    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert "UPDATE trades" in call_args[0][0]
    assert call_args[0][1] == 1


@pytest.mark.asyncio
async def test_get_open_trade(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchrow = AsyncMock(return_value={
        "id": 1,
        "symbol": "SPY",
        "side": "long",
        "entry_time": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        "entry_price": 420.0,
        "quantity": 10.0,
        "entry_order_id": 1,
        "exit_time": None,
        "exit_price": None,
        "pnl": None,
        "hold_duration_seconds": None,
        "exit_order_id": None,
    })
    db = Database(settings)
    db.pool = pool

    trade = await db.get_open_trade("SPY")

    assert trade is not None
    assert isinstance(trade, Trade)
    assert trade.symbol == "SPY"
    assert trade.exit_time is None


@pytest.mark.asyncio
async def test_get_open_trade_not_found(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchrow = AsyncMock(return_value=None)
    db = Database(settings)
    db.pool = pool

    trade = await db.get_open_trade("AAPL")

    assert trade is None


@pytest.mark.asyncio
async def test_query_closed_trades(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1,
            "symbol": "SPY",
            "side": "long",
            "entry_time": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            "entry_price": 420.0,
            "quantity": 10.0,
            "entry_order_id": 1,
            "exit_time": datetime(2026, 4, 12, 15, 0, 0, tzinfo=timezone.utc),
            "exit_price": 425.0,
            "pnl": 50.0,
            "hold_duration_seconds": 3600,
            "exit_order_id": 2,
        },
    ])
    db = Database(settings)
    db.pool = pool

    trades = await db.query_closed_trades(limit=50)

    assert len(trades) == 1
    assert isinstance(trades[0], Trade)
    assert trades[0].pnl == 50.0


@pytest.mark.asyncio
async def test_query_closed_trades_with_since(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[])
    db = Database(settings)
    db.pool = pool

    since = datetime(2026, 4, 12, 0, 0, 0, tzinfo=timezone.utc)
    trades = await db.query_closed_trades(since=since, limit=50)

    assert len(trades) == 0
    call_args = conn.fetch.call_args
    assert "exit_time >=" in call_args[0][0]


@pytest.mark.asyncio
async def test_upsert_model_status(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.execute = AsyncMock()
    db = Database(settings)
    db.pool = pool

    await db.upsert_model_status(5, "warned", "Accuracy degrading")

    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert "INSERT INTO model_status" in call_args[0][0]
    assert "ON CONFLICT" in call_args[0][0]
    assert call_args[0][1] == 5
    assert call_args[0][2] == "warned"


@pytest.mark.asyncio
async def test_get_all_model_statuses(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[
        {
            "model_id": 5,
            "status": "active",
            "reason": "OK",
            "updated_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        },
    ])
    db = Database(settings)
    db.pool = pool

    statuses = await db.get_all_model_statuses()

    assert len(statuses) == 1
    assert isinstance(statuses[0], ModelStatus)
    assert statuses[0].model_id == 5


@pytest.mark.asyncio
async def test_query_prediction_outcomes(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[
        {
            "predicted_direction": 1,
            "confidence": 0.75,
            "price_move": 0.5,
            "is_correct": True,
        },
        {
            "predicted_direction": -1,
            "confidence": 0.60,
            "price_move": 0.3,
            "is_correct": False,
        },
    ])
    db = Database(settings)
    db.pool = pool

    outcomes = await db.query_prediction_outcomes(model_id=5, limit=50)

    assert len(outcomes) == 2
    assert outcomes[0]["is_correct"] is True
    assert outcomes[1]["is_correct"] is False


@pytest.mark.asyncio
async def test_insert_halt_event(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchval = AsyncMock(return_value=1)
    db = Database(settings)
    db.pool = pool

    halt_id = await db.insert_halt_event(4, "Drawdown exceeded threshold")

    assert halt_id == 1
    conn.fetchval.assert_called_once()
    call_args = conn.fetchval.call_args
    assert "INSERT INTO halt_events" in call_args[0][0]


@pytest.mark.asyncio
async def test_get_active_halts(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1,
            "tier": 4,
            "reason": "Drawdown exceeded",
            "direction": None,
            "activated_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        },
    ])
    db = Database(settings)
    db.pool = pool

    halts = await db.get_active_halts()

    assert len(halts) == 1
    assert halts[0]["tier"] == 4


@pytest.mark.asyncio
async def test_resolve_halt_event(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.execute = AsyncMock()
    db = Database(settings)
    db.pool = pool

    await db.resolve_halt_event(1)

    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert "UPDATE halt_events" in call_args[0][0]
    assert "resolved_at" in call_args[0][0]


@pytest.mark.asyncio
async def test_insert_metric_snapshot(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.execute = AsyncMock()
    db = Database(settings)
    db.pool = pool

    await db.insert_metric_snapshot(
        category="trading",
        scope="",
        window="last_20",
        metrics={"win_rate": 0.65, "profit_factor": 1.8},
    )

    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert "INSERT INTO metric_snapshots" in call_args[0][0]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_monitoring_db.py -v`
Expected: FAIL — `AttributeError: 'Database' object has no attribute 'insert_trade'`

- [ ] **Step 3: Add all new DB methods to shared/db.py**

First, add `Trade` and `ModelStatus` to the import at the top of `shared/db.py` (line 12):

```python
from shared.models import Bar, FeatureRow, Label, ModelMetadata, Prediction, Order, Trade, ModelStatus
```

Then append these methods at the end of the `Database` class (after the `query_orders` method):

```python
    # ── Trade methods ──────────────────────────────────────────────

    async def insert_trade(self, trade: Trade) -> int:
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO trades
                    (symbol, side, entry_time, exit_time, entry_price, exit_price,
                     quantity, pnl, hold_duration_seconds, entry_order_id, exit_order_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING id
                """,
                trade.symbol,
                trade.side,
                trade.entry_time,
                trade.exit_time,
                trade.entry_price,
                trade.exit_price,
                trade.quantity,
                trade.pnl,
                trade.hold_duration_seconds,
                trade.entry_order_id,
                trade.exit_order_id,
            )
        return row_id

    async def update_trade_exit(
        self,
        trade_id: int,
        exit_time: datetime,
        exit_price: float,
        exit_order_id: int,
        pnl: float,
        hold_duration_seconds: int,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE trades
                SET exit_time = $2, exit_price = $3, exit_order_id = $4,
                    pnl = $5, hold_duration_seconds = $6
                WHERE id = $1
                """,
                trade_id,
                exit_time,
                exit_price,
                exit_order_id,
                pnl,
                hold_duration_seconds,
            )

    async def get_open_trade(self, symbol: str) -> Optional[Trade]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, symbol, side, entry_time, exit_time, entry_price,
                       exit_price, quantity, pnl, hold_duration_seconds,
                       entry_order_id, exit_order_id
                FROM trades
                WHERE symbol = $1 AND exit_time IS NULL
                ORDER BY entry_time DESC
                LIMIT 1
                """,
                symbol,
            )
        if row is None:
            return None
        return Trade(
            id=row["id"],
            symbol=row["symbol"],
            side=row["side"],
            entry_time=row["entry_time"],
            exit_time=row["exit_time"],
            entry_price=row["entry_price"],
            exit_price=row["exit_price"],
            quantity=row["quantity"],
            pnl=row["pnl"],
            hold_duration_seconds=row["hold_duration_seconds"],
            entry_order_id=row["entry_order_id"],
            exit_order_id=row["exit_order_id"],
        )

    async def query_closed_trades(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[Trade]:
        query = """SELECT id, symbol, side, entry_time, exit_time, entry_price,
                          exit_price, quantity, pnl, hold_duration_seconds,
                          entry_order_id, exit_order_id
                   FROM trades WHERE exit_time IS NOT NULL"""
        args: list = []
        idx = 1

        if symbol is not None:
            query += f" AND symbol = ${idx}"
            args.append(symbol)
            idx += 1

        if since is not None:
            query += f" AND exit_time >= ${idx}"
            args.append(since)
            idx += 1

        query += f" ORDER BY exit_time DESC LIMIT ${idx}"
        args.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [
            Trade(
                id=r["id"],
                symbol=r["symbol"],
                side=r["side"],
                entry_time=r["entry_time"],
                exit_time=r["exit_time"],
                entry_price=r["entry_price"],
                exit_price=r["exit_price"],
                quantity=r["quantity"],
                pnl=r["pnl"],
                hold_duration_seconds=r["hold_duration_seconds"],
                entry_order_id=r["entry_order_id"],
                exit_order_id=r["exit_order_id"],
            )
            for r in rows
        ]

    # ── Model status methods ───────────────────────────────────────

    async def upsert_model_status(
        self, model_id: int, status: str, reason: str,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO model_status (model_id, status, reason, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (model_id)
                DO UPDATE SET status = $2, reason = $3, updated_at = NOW()
                """,
                model_id,
                status,
                reason,
            )

    async def get_all_model_statuses(self) -> list[ModelStatus]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT model_id, status, reason, updated_at
                FROM model_status
                ORDER BY model_id
                """
            )
        return [
            ModelStatus(
                model_id=r["model_id"],
                status=r["status"],
                reason=r["reason"] or "",
                updated_at=r["updated_at"],
            )
            for r in rows
        ]

    # ── Prediction outcome methods ─────────────────────────────────

    async def query_prediction_outcomes(
        self, model_id: int, limit: int = 100,
    ) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    p.predicted_direction,
                    p.confidence,
                    b_exit.close - b_entry.close AS price_move,
                    CASE
                        WHEN p.predicted_direction = 1
                             AND b_exit.close > b_entry.close THEN TRUE
                        WHEN p.predicted_direction = -1
                             AND b_exit.close < b_entry.close THEN TRUE
                        WHEN p.predicted_direction = 0
                             AND ABS(b_exit.close - b_entry.close)
                                 / NULLIF(b_entry.close, 0) < 0.001 THEN TRUE
                        ELSE FALSE
                    END AS is_correct
                FROM predictions p
                JOIN bars_1m b_entry
                    ON b_entry.symbol = p.symbol
                    AND b_entry.timestamp = p.timestamp
                JOIN bars_1m b_exit
                    ON b_exit.symbol = p.symbol
                    AND b_exit.timestamp = p.timestamp
                        + (p.horizon_minutes * interval '1 minute')
                WHERE p.model_id = $1
                ORDER BY p.timestamp DESC
                LIMIT $2
                """,
                model_id,
                limit,
            )
        return [dict(r) for r in rows]

    # ── Halt event methods ─────────────────────────────────────────

    async def insert_halt_event(
        self, tier: int, reason: str, direction: Optional[str] = None,
    ) -> int:
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO halt_events (tier, reason, direction)
                VALUES ($1, $2, $3)
                RETURNING id
                """,
                tier,
                reason,
                direction,
            )
        return row_id

    async def get_active_halts(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, tier, reason, direction, activated_at
                FROM halt_events
                WHERE resolved_at IS NULL
                ORDER BY activated_at DESC
                """
            )
        return [dict(r) for r in rows]

    async def resolve_halt_event(self, halt_id: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE halt_events
                SET resolved_at = NOW()
                WHERE id = $1
                """,
                halt_id,
            )

    # ── Metric snapshot methods ────────────────────────────────────

    async def insert_metric_snapshot(
        self,
        category: str,
        scope: str,
        window: str,
        metrics: dict,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO metric_snapshots (category, scope, window, metrics)
                VALUES ($1, $2, $3, $4)
                """,
                category,
                scope,
                window,
                json.dumps(metrics),
            )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_monitoring_db.py -v`
Expected: ALL PASS (14 tests)

- [ ] **Step 5: Run the full test suite**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add shared/db.py tests/test_monitoring_db.py
git commit -m "feat: add trade, model status, halt event, and metric snapshot DB methods"
```

---

## Task 5: Order Executor — Trade Recording

**Files:**
- Modify: `services/execution_service/order_executor.py`
- Modify: `tests/test_order_executor.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_order_executor.py`:

```python
@pytest.mark.asyncio
async def test_execute_open_records_trade(executor, mock_broker, mock_db):
    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is not None
    mock_db.insert_trade.assert_called_once()
    trade_arg = mock_db.insert_trade.call_args[0][0]
    assert trade_arg.symbol == "SPY"
    assert trade_arg.side == "long"
    assert trade_arg.entry_price == 420.50
    assert trade_arg.quantity == 10.0
    assert trade_arg.entry_order_id == 42


@pytest.mark.asyncio
async def test_execute_open_no_trade_on_unfilled(
    mock_broker, mock_risk, mock_tracker, mock_db,
):
    mock_broker.get_order = AsyncMock(return_value={
        "id": "alp-order-123", "status": "submitted",
        "filled_qty": None, "filled_avg_price": None, "filled_at": None,
    })
    executor = OrderExecutor(
        broker=mock_broker,
        risk_manager=mock_risk,
        position_tracker=mock_tracker,
        db=mock_db,
        fill_check_delay=0,
        fill_check_retries=1,
    )

    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is not None
    assert order.status == "submitted"
    mock_db.insert_trade.assert_not_called()


@pytest.mark.asyncio
async def test_execute_close_updates_trade(
    executor, mock_broker, mock_db, mock_risk, mock_tracker,
):
    mock_tracker.get_position = MagicMock(
        return_value={"symbol": "SPY", "qty": 10.0, "market_value": 4200.0},
    )
    mock_risk.check_exit = MagicMock(return_value=(True, "Opposing signal"))
    mock_db.get_open_trade = AsyncMock(return_value=MagicMock(
        id=1,
        entry_price=420.0,
        entry_time=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        side="long",
        quantity=10.0,
    ))

    order = await executor.process_signal(
        _signal(direction=-1), current_price=425.0,
    )

    assert order is not None
    assert order.side == "sell"
    mock_db.update_trade_exit.assert_called_once()
    call_kwargs = mock_db.update_trade_exit.call_args
    assert call_kwargs[1]["trade_id"] == 1
    assert call_kwargs[1]["exit_price"] == 420.50


@pytest.mark.asyncio
async def test_execute_close_no_trade_update_when_no_open_trade(
    executor, mock_broker, mock_db, mock_risk, mock_tracker,
):
    mock_tracker.get_position = MagicMock(
        return_value={"symbol": "SPY", "qty": 10.0, "market_value": 4200.0},
    )
    mock_risk.check_exit = MagicMock(return_value=(True, "Opposing signal"))
    mock_db.get_open_trade = AsyncMock(return_value=None)

    order = await executor.process_signal(
        _signal(direction=-1), current_price=425.0,
    )

    assert order is not None
    mock_db.update_trade_exit.assert_not_called()
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_order_executor.py::test_execute_open_records_trade -v`
Expected: FAIL — `insert_trade` not called

- [ ] **Step 3: Update mock_db fixture**

In `tests/test_order_executor.py`, update the `mock_db` fixture to include the new methods:

```python
@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.insert_order = AsyncMock(return_value=42)
    db.update_order_status = AsyncMock()
    db.insert_trade = AsyncMock(return_value=1)
    db.update_trade_exit = AsyncMock()
    db.get_open_trade = AsyncMock(return_value=None)
    return db
```

- [ ] **Step 4: Add Trade import to order_executor.py**

In `services/execution_service/order_executor.py`, update the imports (line 11):

```python
from shared.models import Order, Trade, TradingSignal
```

- [ ] **Step 5: Add trade recording to _execute_open**

In `services/execution_service/order_executor.py`, modify `_execute_open` to record a trade after a filled order. Replace the method body after `filled_order = await self._check_fill(order, order_id)`:

```python
    async def _execute_open(
        self, signal: TradingSignal, qty: int,
    ) -> Optional[Order]:
        side = "buy" if signal.direction == 1 else "sell"
        try:
            alpaca_id = await self._broker.submit_market_order(
                signal.symbol, qty, side,
            )
        except Exception as e:
            logger.error("Failed to submit order for %s: %s", signal.symbol, e)
            return None

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

        if filled_order.status == "filled" and filled_order.filled_price is not None:
            trade_side = "long" if side == "buy" else "short"
            trade = Trade(
                symbol=signal.symbol,
                side=trade_side,
                entry_time=filled_order.filled_at or now,
                entry_price=filled_order.filled_price,
                quantity=filled_order.filled_quantity or float(qty),
                entry_order_id=order_id,
            )
            await self._db.insert_trade(trade)

        await self._publish_order_event(filled_order)
        return filled_order
```

- [ ] **Step 6: Add trade closing to _execute_close**

Replace `_execute_close` in `services/execution_service/order_executor.py`:

```python
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

        if filled_order.status == "filled" and filled_order.filled_price is not None:
            open_trade = await self._db.get_open_trade(signal.symbol)
            if open_trade:
                pnl = (filled_order.filled_price - open_trade.entry_price) * open_trade.quantity
                if open_trade.side == "short":
                    pnl = -pnl
                fill_time = filled_order.filled_at or now
                hold_secs = int((fill_time - open_trade.entry_time).total_seconds())
                await self._db.update_trade_exit(
                    trade_id=open_trade.id,
                    exit_time=fill_time,
                    exit_price=filled_order.filled_price,
                    exit_order_id=order_id,
                    pnl=pnl,
                    hold_duration_seconds=hold_secs,
                )

        await self._publish_order_event(filled_order)
        return filled_order
```

- [ ] **Step 7: Run all order executor tests**

Run: `cd bullforge && python -m pytest tests/test_order_executor.py -v`
Expected: ALL PASS

- [ ] **Step 8: Run the full test suite**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 9: Commit**

```bash
git add services/execution_service/order_executor.py tests/test_order_executor.py
git commit -m "feat: order executor records round-trip trades on open/close"
```

---

## Task 6: MetricsComputer

**Files:**
- Create: `services/monitoring_service/__init__.py`
- Create: `services/monitoring_service/metrics.py`
- Create: `tests/test_metrics_computer.py`

- [ ] **Step 1: Create the empty package marker**

Create `services/monitoring_service/__init__.py` as an empty file.

- [ ] **Step 2: Write the failing tests**

Create `tests/test_metrics_computer.py`:

```python
import pytest
from datetime import datetime, timezone
from shared.models import Trade
from services.monitoring_service.metrics import MetricsComputer


def _closed_trade(symbol="SPY", side="long", pnl=50.0, hold_secs=3600):
    return Trade(
        symbol=symbol,
        side=side,
        entry_time=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        entry_price=420.0,
        quantity=10.0,
        entry_order_id=1,
        exit_time=datetime(2026, 4, 12, 15, 0, 0, tzinfo=timezone.utc),
        exit_price=420.0 + pnl / 10.0,
        pnl=pnl,
        hold_duration_seconds=hold_secs,
        exit_order_id=2,
        id=1,
    )


@pytest.fixture
def computer():
    return MetricsComputer()


def test_trading_metrics_basic(computer):
    trades = [
        _closed_trade(pnl=100.0),
        _closed_trade(pnl=-30.0),
        _closed_trade(pnl=50.0),
    ]
    m = computer.compute_trading_metrics(trades)

    assert m["total_trades"] == 3
    assert m["win_rate"] == pytest.approx(2 / 3)
    assert m["total_pnl"] == pytest.approx(120.0)
    assert m["avg_win"] == pytest.approx(75.0)
    assert m["avg_loss"] == pytest.approx(-30.0)
    assert m["profit_factor"] == pytest.approx(150.0 / 30.0)


def test_trading_metrics_all_wins(computer):
    trades = [_closed_trade(pnl=50.0), _closed_trade(pnl=25.0)]
    m = computer.compute_trading_metrics(trades)

    assert m["win_rate"] == 1.0
    assert m["profit_factor"] == float("inf")
    assert m["avg_loss"] == 0.0


def test_trading_metrics_all_losses(computer):
    trades = [_closed_trade(pnl=-50.0), _closed_trade(pnl=-25.0)]
    m = computer.compute_trading_metrics(trades)

    assert m["win_rate"] == 0.0
    assert m["profit_factor"] == 0.0
    assert m["avg_win"] == 0.0


def test_trading_metrics_empty(computer):
    m = computer.compute_trading_metrics([])

    assert m["total_trades"] == 0
    assert m["win_rate"] == 0.0
    assert m["total_pnl"] == 0.0


def test_trading_metrics_hold_duration(computer):
    trades = [
        _closed_trade(hold_secs=3600),
        _closed_trade(hold_secs=7200),
    ]
    m = computer.compute_trading_metrics(trades)

    assert m["avg_hold_duration_seconds"] == pytest.approx(5400.0)


def test_trading_metrics_sharpe_ratio(computer):
    trades = [
        _closed_trade(pnl=100.0),
        _closed_trade(pnl=-30.0),
        _closed_trade(pnl=50.0),
        _closed_trade(pnl=80.0),
    ]
    m = computer.compute_trading_metrics(trades)

    # Sharpe = mean(pnl) / std(pnl) — verify it's a reasonable number
    assert "sharpe_ratio" in m
    assert m["sharpe_ratio"] > 0


def test_trading_metrics_single_trade(computer):
    trades = [_closed_trade(pnl=50.0)]
    m = computer.compute_trading_metrics(trades)

    assert m["total_trades"] == 1
    assert m["sharpe_ratio"] == 0.0  # can't compute std from 1 trade


def test_account_metrics(computer):
    account = {
        "equity": 105000.0,
        "buying_power": 200000.0,
        "cash": 95000.0,
        "portfolio_value": 105000.0,
    }
    equity_history = [100000.0, 102000.0, 105000.0, 103000.0, 105000.0]

    m = computer.compute_account_metrics(account, equity_history)

    assert m["equity"] == 105000.0
    assert m["buying_power"] == 200000.0
    assert m["peak_equity"] == 105000.0
    assert m["current_drawdown"] == pytest.approx(0.0)


def test_account_metrics_with_drawdown(computer):
    account = {
        "equity": 90000.0,
        "buying_power": 180000.0,
        "cash": 90000.0,
        "portfolio_value": 90000.0,
    }
    equity_history = [100000.0, 102000.0, 95000.0, 90000.0]

    m = computer.compute_account_metrics(account, equity_history)

    assert m["peak_equity"] == 102000.0
    assert m["current_drawdown"] == pytest.approx(
        (102000.0 - 90000.0) / 102000.0
    )
    assert m["max_drawdown"] == pytest.approx(
        (102000.0 - 90000.0) / 102000.0
    )


def test_direction_metrics(computer):
    trades = [
        _closed_trade(side="long", pnl=100.0),
        _closed_trade(side="long", pnl=-20.0),
        _closed_trade(side="short", pnl=50.0),
    ]
    long_m = computer.compute_direction_metrics(trades, "long")
    short_m = computer.compute_direction_metrics(trades, "short")

    assert long_m["total_trades"] == 2
    assert long_m["total_pnl"] == pytest.approx(80.0)
    assert short_m["total_trades"] == 1
    assert short_m["total_pnl"] == pytest.approx(50.0)
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_metrics_computer.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'services.monitoring_service'`

- [ ] **Step 4: Implement MetricsComputer**

Create `services/monitoring_service/metrics.py`:

```python
from __future__ import annotations

import math
from typing import List

from shared.models import Trade


class MetricsComputer:
    def compute_trading_metrics(self, trades: List[Trade]) -> dict:
        if not trades:
            return {
                "total_trades": 0,
                "win_rate": 0.0,
                "total_pnl": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "profit_factor": 0.0,
                "sharpe_ratio": 0.0,
                "avg_hold_duration_seconds": 0.0,
                "best_trade": 0.0,
                "worst_trade": 0.0,
            }

        pnls = [t.pnl for t in trades if t.pnl is not None]
        if not pnls:
            return {
                "total_trades": len(trades),
                "win_rate": 0.0,
                "total_pnl": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "profit_factor": 0.0,
                "sharpe_ratio": 0.0,
                "avg_hold_duration_seconds": 0.0,
                "best_trade": 0.0,
                "worst_trade": 0.0,
            }

        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]

        total_pnl = sum(pnls)
        win_rate = len(wins) / len(pnls) if pnls else 0.0
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0

        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        if gross_loss > 0:
            profit_factor = gross_profit / gross_loss
        elif gross_profit > 0:
            profit_factor = float("inf")
        else:
            profit_factor = 0.0

        # Sharpe ratio: mean(pnl) / std(pnl)
        if len(pnls) >= 2:
            mean_pnl = total_pnl / len(pnls)
            variance = sum((p - mean_pnl) ** 2 for p in pnls) / (len(pnls) - 1)
            std_pnl = math.sqrt(variance)
            sharpe_ratio = mean_pnl / std_pnl if std_pnl > 0 else 0.0
        else:
            sharpe_ratio = 0.0

        durations = [
            t.hold_duration_seconds
            for t in trades
            if t.hold_duration_seconds is not None
        ]
        avg_hold = sum(durations) / len(durations) if durations else 0.0

        return {
            "total_trades": len(pnls),
            "win_rate": win_rate,
            "total_pnl": total_pnl,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "profit_factor": profit_factor,
            "sharpe_ratio": sharpe_ratio,
            "avg_hold_duration_seconds": avg_hold,
            "best_trade": max(pnls),
            "worst_trade": min(pnls),
        }

    def compute_account_metrics(
        self, account: dict, equity_history: List[float],
    ) -> dict:
        equity = account.get("equity", 0.0)
        peak = max(equity_history) if equity_history else equity
        current_drawdown = (peak - equity) / peak if peak > 0 else 0.0

        # Max drawdown from equity history
        max_dd = 0.0
        running_peak = 0.0
        for eq in equity_history:
            running_peak = max(running_peak, eq)
            if running_peak > 0:
                dd = (running_peak - eq) / running_peak
                max_dd = max(max_dd, dd)

        return {
            "equity": equity,
            "buying_power": account.get("buying_power", 0.0),
            "cash": account.get("cash", 0.0),
            "portfolio_value": account.get("portfolio_value", 0.0),
            "peak_equity": peak,
            "current_drawdown": current_drawdown,
            "max_drawdown": max_dd,
        }

    def compute_direction_metrics(
        self, trades: List[Trade], side: str,
    ) -> dict:
        filtered = [t for t in trades if t.side == side]
        return self.compute_trading_metrics(filtered)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_metrics_computer.py -v`
Expected: ALL PASS (11 tests)

- [ ] **Step 6: Run the full test suite**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add services/monitoring_service/__init__.py services/monitoring_service/metrics.py tests/test_metrics_computer.py
git commit -m "feat: MetricsComputer for trading, account, and direction metrics"
```

---

## Task 7: FidelityTracker

**Files:**
- Create: `services/monitoring_service/fidelity.py`
- Create: `tests/test_fidelity_tracker.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_fidelity_tracker.py`:

```python
import pytest
from unittest.mock import AsyncMock, MagicMock
from services.monitoring_service.fidelity import FidelityTracker


@pytest.fixture
def mock_db():
    return AsyncMock()


@pytest.fixture
def tracker(mock_db, settings):
    return FidelityTracker(mock_db, settings)


@pytest.mark.asyncio
async def test_evaluate_model_active(tracker, mock_db):
    mock_db.query_prediction_outcomes = AsyncMock(return_value=[
        {"predicted_direction": 1, "confidence": 0.8, "price_move": 0.5, "is_correct": True},
        {"predicted_direction": -1, "confidence": 0.7, "price_move": -0.3, "is_correct": True},
        {"predicted_direction": 1, "confidence": 0.6, "price_move": 0.2, "is_correct": True},
    ] * 7)  # 21 outcomes, all correct

    status, reason = await tracker.evaluate_model(model_id=5, backtested_accuracy=0.65)

    assert status == "active"
    assert "Within expected range" in reason


@pytest.mark.asyncio
async def test_evaluate_model_warned(tracker, mock_db):
    # 20 outcomes: 12 correct (60%), backtested 68% — drop of 8% > warning margin (5%)
    correct = [{"predicted_direction": 1, "confidence": 0.8, "price_move": 0.5, "is_correct": True}] * 12
    wrong = [{"predicted_direction": 1, "confidence": 0.7, "price_move": -0.3, "is_correct": False}] * 8
    mock_db.query_prediction_outcomes = AsyncMock(return_value=correct + wrong)

    status, reason = await tracker.evaluate_model(model_id=5, backtested_accuracy=0.68)

    assert status == "warned"
    assert "degrading" in reason.lower()


@pytest.mark.asyncio
async def test_evaluate_model_disabled(tracker, mock_db):
    # 20 outcomes: 10 correct (50%), backtested 65% — drop of 15% > critical margin (10%)
    correct = [{"predicted_direction": 1, "confidence": 0.8, "price_move": 0.5, "is_correct": True}] * 10
    wrong = [{"predicted_direction": 1, "confidence": 0.7, "price_move": -0.3, "is_correct": False}] * 10
    mock_db.query_prediction_outcomes = AsyncMock(return_value=correct + wrong)

    status, reason = await tracker.evaluate_model(model_id=5, backtested_accuracy=0.65)

    assert status == "disabled"
    assert "below backtested" in reason.lower()


@pytest.mark.asyncio
async def test_evaluate_model_insufficient_data(tracker, mock_db):
    # Only 5 outcomes, need 20 minimum
    mock_db.query_prediction_outcomes = AsyncMock(return_value=[
        {"predicted_direction": 1, "confidence": 0.8, "price_move": 0.5, "is_correct": True},
    ] * 5)

    status, reason = await tracker.evaluate_model(model_id=5, backtested_accuracy=0.65)

    assert status == "active"
    assert "Insufficient data" in reason


@pytest.mark.asyncio
async def test_evaluate_model_profit_factor_below_one(tracker, mock_db):
    # 20 outcomes: all "correct" but we test that profit_factor < 1 also triggers disable
    # FidelityTracker checks accuracy, not profit factor directly.
    # Profit factor is a MetricsComputer concern. This test confirms accuracy-only logic.
    correct = [{"predicted_direction": 1, "confidence": 0.8, "price_move": 0.5, "is_correct": True}] * 11
    wrong = [{"predicted_direction": 1, "confidence": 0.7, "price_move": -0.3, "is_correct": False}] * 9
    mock_db.query_prediction_outcomes = AsyncMock(return_value=correct + wrong)

    # 55% accuracy, backtested 60% — drop of 5% = exactly warning margin
    status, reason = await tracker.evaluate_model(model_id=5, backtested_accuracy=0.60)

    assert status == "warned"


@pytest.mark.asyncio
async def test_evaluate_all_models(tracker, mock_db):
    models = [
        MagicMock(id=1, metrics={"accuracy": 0.65}),
        MagicMock(id=2, metrics={"accuracy": 0.70}),
    ]
    mock_db.query_all_active_models = AsyncMock(return_value=models)

    # Model 1: good accuracy, Model 2: bad accuracy
    async def mock_outcomes(model_id, limit):
        if model_id == 1:
            return [{"is_correct": True}] * 15 + [{"is_correct": False}] * 5
        else:
            return [{"is_correct": True}] * 8 + [{"is_correct": False}] * 12

    mock_db.query_prediction_outcomes = AsyncMock(side_effect=mock_outcomes)

    results = await tracker.evaluate_all_models()

    assert len(results) == 2
    assert results[0] == (1, "active", pytest.approx(0.75, abs=0.01))
    assert results[1][0] == 2
    assert results[1][1] == "disabled"  # 40% vs 70% backtested
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_fidelity_tracker.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement FidelityTracker**

Create `services/monitoring_service/fidelity.py`:

```python
from __future__ import annotations

import logging
from typing import List, Tuple

from shared.config import Settings
from shared.db import Database

logger = logging.getLogger(__name__)


class FidelityTracker:
    def __init__(self, db: Database, settings: Settings):
        self._db = db
        self._warning_margin = settings.fidelity_warning_margin
        self._critical_margin = settings.fidelity_critical_margin
        self._min_trades = settings.fidelity_min_trades

    async def evaluate_model(
        self, model_id: int, backtested_accuracy: float,
    ) -> Tuple[str, str]:
        outcomes = await self._db.query_prediction_outcomes(
            model_id, limit=self._min_trades,
        )

        if len(outcomes) < self._min_trades:
            return (
                "active",
                f"Insufficient data: {len(outcomes)}/{self._min_trades} predictions",
            )

        correct = sum(1 for o in outcomes if o["is_correct"])
        live_accuracy = correct / len(outcomes)

        if live_accuracy < backtested_accuracy - self._critical_margin:
            return (
                "disabled",
                f"Live accuracy {live_accuracy:.2%} below backtested "
                f"{backtested_accuracy:.2%} by {backtested_accuracy - live_accuracy:.2%}",
            )

        if live_accuracy < backtested_accuracy - self._warning_margin:
            return (
                "warned",
                f"Live accuracy {live_accuracy:.2%} degrading from "
                f"backtested {backtested_accuracy:.2%}",
            )

        return ("active", "Within expected range")

    async def evaluate_all_models(
        self,
    ) -> List[Tuple[int, str, float]]:
        models = await self._db.query_all_active_models()
        results = []

        for model in models:
            backtested = model.metrics.get("accuracy", 0.5)
            status, reason = await self.evaluate_model(model.id, backtested)

            outcomes = await self._db.query_prediction_outcomes(
                model.id, limit=self._min_trades,
            )
            live_accuracy = (
                sum(1 for o in outcomes if o["is_correct"]) / len(outcomes)
                if outcomes
                else 0.0
            )

            results.append((model.id, status, live_accuracy))

            if status != "active":
                logger.warning(
                    "Model %d fidelity: %s — %s (live=%.2f%%, backtested=%.2f%%)",
                    model.id, status, reason,
                    live_accuracy * 100, backtested * 100,
                )

        return results
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_fidelity_tracker.py -v`
Expected: ALL PASS (6 tests)

- [ ] **Step 5: Run the full test suite**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add services/monitoring_service/fidelity.py tests/test_fidelity_tracker.py
git commit -m "feat: FidelityTracker evaluates per-model prediction accuracy vs backtested"
```

---

## Task 8: HaltManager

**Files:**
- Create: `services/monitoring_service/halt_manager.py`
- Create: `tests/test_halt_manager.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_halt_manager.py`:

```python
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from shared.models import ModelStatus
from services.monitoring_service.halt_manager import HaltManager


def _status(model_id, status="active", reason="OK"):
    return ModelStatus(
        model_id=model_id,
        status=status,
        reason=reason,
        updated_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.insert_halt_event = AsyncMock(return_value=1)
    db.get_active_halts = AsyncMock(return_value=[])
    db.resolve_halt_event = AsyncMock()
    return db


@pytest.fixture
def mock_redis():
    redis_client = MagicMock()
    redis_client.publish = AsyncMock()
    return redis_client


@pytest.fixture
def manager(mock_db, mock_redis, settings):
    return HaltManager(mock_db, mock_redis, settings)


@pytest.mark.asyncio
async def test_no_alerts_when_all_active(manager, mock_db):
    statuses = [_status(1), _status(2), _status(3)]
    alerts = await manager.evaluate(statuses, current_drawdown=0.02)

    assert len(alerts) == 0
    mock_db.insert_halt_event.assert_not_called()


@pytest.mark.asyncio
async def test_tier1_warning(manager):
    statuses = [_status(1, "warned", "Degrading"), _status(2), _status(3)]
    alerts = await manager.evaluate(statuses, current_drawdown=0.02)

    assert len(alerts) == 1
    assert alerts[0]["tier"] == 1
    assert alerts[0]["model_id"] == 1


@pytest.mark.asyncio
async def test_tier2_disable(manager, mock_db):
    statuses = [_status(1, "disabled", "Critical"), _status(2), _status(3)]
    alerts = await manager.evaluate(statuses, current_drawdown=0.02)

    tier2_alerts = [a for a in alerts if a["tier"] == 2]
    assert len(tier2_alerts) == 1
    assert tier2_alerts[0]["model_id"] == 1


@pytest.mark.asyncio
async def test_tier3_halt_direction(manager, mock_db):
    # 2 of 3 models disabled — majority → halt direction
    # Models 1,2 are "long" direction, model 3 is active
    # HaltManager doesn't know model direction inherently;
    # it works on majority of ALL models being disabled to trigger tier 3.
    # Spec: "Majority of long (or short) sub-models disabled"
    # For simplicity, tier 3 = majority of models disabled for ANY group
    statuses = [
        _status(1, "disabled", "Critical"),
        _status(2, "disabled", "Critical"),
        _status(3),
    ]
    alerts = await manager.evaluate(statuses, current_drawdown=0.02)

    tier3_alerts = [a for a in alerts if a["tier"] == 3]
    assert len(tier3_alerts) == 1
    mock_db.insert_halt_event.assert_called()


@pytest.mark.asyncio
async def test_tier4_full_halt_drawdown(manager, mock_db):
    statuses = [_status(1), _status(2), _status(3)]
    alerts = await manager.evaluate(statuses, current_drawdown=0.15)

    tier4_alerts = [a for a in alerts if a["tier"] == 4]
    assert len(tier4_alerts) == 1
    assert "drawdown" in tier4_alerts[0]["reason"].lower()
    mock_db.insert_halt_event.assert_called()


@pytest.mark.asyncio
async def test_tier4_full_halt_majority_disabled(manager, mock_db):
    statuses = [
        _status(1, "disabled", "Critical"),
        _status(2, "disabled", "Critical"),
        _status(3, "disabled", "Critical"),
    ]
    alerts = await manager.evaluate(statuses, current_drawdown=0.02)

    tier4_alerts = [a for a in alerts if a["tier"] == 4]
    assert len(tier4_alerts) == 1
    assert "majority" in tier4_alerts[0]["reason"].lower()


@pytest.mark.asyncio
async def test_publish_status(manager, mock_redis):
    statuses = [_status(1, "active"), _status(2, "disabled", "Bad")]

    await manager.publish_status(statuses)

    mock_redis.publish.assert_called_once()
    call_args = mock_redis.publish.call_args
    assert call_args[0][0] == "models:status"


@pytest.mark.asyncio
async def test_is_halted(manager):
    assert manager.is_halted is False

    statuses = [_status(1)]
    await manager.evaluate(statuses, current_drawdown=0.15)

    assert manager.is_halted is True
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_halt_manager.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement HaltManager**

Create `services/monitoring_service/halt_manager.py`:

```python
from __future__ import annotations

import json
import logging
from typing import List

from shared.config import Settings
from shared.db import Database
from shared.models import ModelStatus
from shared.redis import RedisClient

logger = logging.getLogger(__name__)


class HaltManager:
    def __init__(
        self, db: Database, redis_client: RedisClient, settings: Settings,
    ):
        self._db = db
        self._redis = redis_client
        self._drawdown_threshold = settings.drawdown_halt_threshold
        self._full_halt = False

    @property
    def is_halted(self) -> bool:
        return self._full_halt

    async def evaluate(
        self,
        model_statuses: List[ModelStatus],
        current_drawdown: float,
    ) -> List[dict]:
        alerts: List[dict] = []

        if not model_statuses:
            return alerts

        # Tier 4: Full halt on drawdown
        if current_drawdown >= self._drawdown_threshold:
            self._full_halt = True
            await self._db.insert_halt_event(
                4, f"Drawdown {current_drawdown:.2%} exceeds threshold {self._drawdown_threshold:.2%}",
            )
            alerts.append({
                "tier": 4,
                "type": "drawdown_halt",
                "reason": f"Drawdown {current_drawdown:.2%} exceeds threshold",
            })
            logger.critical("TIER 4 HALT: %s", alerts[-1]["reason"])
            return alerts

        disabled = [m for m in model_statuses if m.status == "disabled"]
        warned = [m for m in model_statuses if m.status == "warned"]
        total = len(model_statuses)

        # Tier 4: Majority of ALL models disabled
        if len(disabled) > total / 2:
            self._full_halt = True
            reason = f"Majority of models disabled ({len(disabled)}/{total})"
            await self._db.insert_halt_event(4, reason)
            alerts.append({
                "tier": 4,
                "type": "majority_halt",
                "reason": reason,
            })
            logger.critical("TIER 4 HALT: %s", reason)
            return alerts

        # Tier 3: Majority disabled (more than half but checked at lower threshold)
        # Spec: "Majority of long (or short) sub-models disabled"
        # Without per-model direction metadata, tier 3 fires when disabled count
        # reaches half of total (approaching majority).
        if total >= 2 and len(disabled) >= total / 2:
            reason = f"Half or more models disabled ({len(disabled)}/{total})"
            await self._db.insert_halt_event(3, reason)
            alerts.append({
                "tier": 3,
                "type": "direction_halt",
                "reason": reason,
            })
            logger.error("TIER 3 HALT: %s", reason)

        # Tier 2: Individual model disabled
        for m in disabled:
            alerts.append({
                "tier": 2,
                "type": "model_disabled",
                "model_id": m.model_id,
                "reason": m.reason,
            })
            logger.warning("TIER 2: Model %d disabled — %s", m.model_id, m.reason)

        # Tier 1: Individual model warned
        for m in warned:
            alerts.append({
                "tier": 1,
                "type": "model_warned",
                "model_id": m.model_id,
                "reason": m.reason,
            })
            logger.warning("TIER 1: Model %d warned — %s", m.model_id, m.reason)

        return alerts

    async def publish_status(self, statuses: List[ModelStatus]) -> None:
        payload = {
            str(m.model_id): m.status
            for m in statuses
        }
        await self._redis.publish("models:status", json.dumps(payload))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_halt_manager.py -v`
Expected: ALL PASS (8 tests)

- [ ] **Step 5: Run the full test suite**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add services/monitoring_service/halt_manager.py tests/test_halt_manager.py
git commit -m "feat: HaltManager with 4-tier degradation system"
```

---

## Task 9: Signal Aggregator & Model Service Integration

**Files:**
- Modify: `services/execution_service/signal_aggregator.py`
- Modify: `services/model_service/main.py`
- Modify: `tests/test_signal_aggregator.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_signal_aggregator.py`:

```python
def test_aggregate_filters_disabled_models():
    agg = SignalAggregator(min_confidence=0.6, min_agreement=2)
    agg.update_disabled_models({2})

    predictions = [
        {"direction": 1, "confidence": 0.8, "model_id": 1},
        {"direction": 1, "confidence": 0.7, "model_id": 2},  # disabled
        {"direction": 1, "confidence": 0.9, "model_id": 3},
    ]

    result = agg.aggregate("SPY", datetime.now(timezone.utc), predictions)

    # Model 2 filtered out, but models 1 and 3 still meet min_agreement=2
    assert result is not None
    assert result.contributing_predictions == 2


def test_aggregate_disabled_models_cause_no_signal():
    agg = SignalAggregator(min_confidence=0.6, min_agreement=2)
    agg.update_disabled_models({1, 2})

    predictions = [
        {"direction": 1, "confidence": 0.8, "model_id": 1},  # disabled
        {"direction": 1, "confidence": 0.7, "model_id": 2},  # disabled
        {"direction": 1, "confidence": 0.9, "model_id": 3},
    ]

    result = agg.aggregate("SPY", datetime.now(timezone.utc), predictions)

    # Only 1 active prediction, below min_agreement=2
    assert result is None


def test_aggregate_no_model_id_not_filtered():
    agg = SignalAggregator(min_confidence=0.6, min_agreement=2)
    agg.update_disabled_models({99})

    # Predictions without model_id (backward compatibility)
    predictions = [
        {"direction": 1, "confidence": 0.8},
        {"direction": 1, "confidence": 0.7},
    ]

    result = agg.aggregate("SPY", datetime.now(timezone.utc), predictions)

    assert result is not None
    assert result.contributing_predictions == 2
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_signal_aggregator.py::test_aggregate_filters_disabled_models -v`
Expected: FAIL — `AttributeError: 'SignalAggregator' object has no attribute 'update_disabled_models'`

- [ ] **Step 3: Add model filtering to SignalAggregator**

Replace `services/execution_service/signal_aggregator.py`:

```python
from __future__ import annotations

from datetime import datetime
from typing import Optional, Set

from shared.models import TradingSignal


class SignalAggregator:
    def __init__(self, min_confidence: float = 0.6, min_agreement: int = 2):
        self._min_confidence = min_confidence
        self._min_agreement = min_agreement
        self._disabled_model_ids: Set[int] = set()

    def update_disabled_models(self, disabled_ids: Set[int]) -> None:
        self._disabled_model_ids = disabled_ids

    def aggregate(
        self,
        symbol: str,
        timestamp: datetime,
        predictions: list[dict],
    ) -> Optional[TradingSignal]:
        if not predictions:
            return None

        # Filter out predictions from disabled models
        active_preds = [
            p for p in predictions
            if p.get("model_id") not in self._disabled_model_ids
        ]

        confident = [
            p for p in active_preds if p["confidence"] >= self._min_confidence
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

- [ ] **Step 4: Add model_id to model service prediction publishing**

In `services/model_service/main.py`, update the prediction dict in `on_feature_update` (around line 84) to include `model_id`:

Change:
```python
                    "predictions": [
                        {
                            "horizon_minutes": p.horizon_minutes,
                            "direction": p.predicted_direction,
                            "confidence": p.confidence,
                        }
                        for p in predictions
                    ],
```

To:
```python
                    "predictions": [
                        {
                            "horizon_minutes": p.horizon_minutes,
                            "direction": p.predicted_direction,
                            "confidence": p.confidence,
                            "model_id": p.model_id,
                        }
                        for p in predictions
                    ],
```

- [ ] **Step 5: Run all signal aggregator tests**

Run: `cd bullforge && python -m pytest tests/test_signal_aggregator.py -v`
Expected: ALL PASS

- [ ] **Step 6: Run the full test suite**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add services/execution_service/signal_aggregator.py services/model_service/main.py tests/test_signal_aggregator.py
git commit -m "feat: signal aggregator filters disabled models, model service publishes model_id"
```

---

## Task 10: Execution Service — Subscribe to Model Status

**Files:**
- Modify: `services/execution_service/main.py`

- [ ] **Step 1: Add model status subscription**

In `services/execution_service/main.py`, add a handler for the `models:status` channel. After the `on_prediction` handler definition (around line 89), add:

```python
    async def on_model_status(message: str) -> None:
        try:
            statuses = json.loads(message)
            disabled_ids = {
                int(model_id)
                for model_id, status in statuses.items()
                if status == "disabled"
            }
            aggregator.update_disabled_models(disabled_ids)
            logger.info(
                "Updated model status: %d disabled models", len(disabled_ids),
            )
        except Exception:
            logger.exception("Error processing model status update")

    await redis_client.subscribe("models:status", on_model_status)
    logger.info("Subscribed to models:status")
```

- [ ] **Step 2: Run the full test suite to verify no regressions**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 3: Commit**

```bash
git add services/execution_service/main.py
git commit -m "feat: execution service subscribes to models:status for disabled model filtering"
```

---

## Task 11: Monitoring Health Check

**Files:**
- Create: `services/monitoring_service/health.py`
- Create: `tests/test_monitoring_health.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_monitoring_health.py`:

```python
import pytest
from unittest.mock import AsyncMock, MagicMock
from services.monitoring_service.health import create_health_app


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


@pytest.mark.asyncio
async def test_health_endpoint_ok(aiohttp_client, mock_db, mock_redis):
    app = create_health_app(mock_db, mock_redis)
    client = await aiohttp_client(app)

    resp = await client.get("/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["db"] == "ok"
    assert data["redis"] == "ok"


@pytest.mark.asyncio
async def test_health_endpoint_db_failure(aiohttp_client, mock_db, mock_redis):
    conn = AsyncMock()
    conn.execute = AsyncMock(side_effect=Exception("connection refused"))
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    mock_db.pool.acquire.return_value = ctx

    app = create_health_app(mock_db, mock_redis)
    client = await aiohttp_client(app)

    resp = await client.get("/health")
    assert resp.status == 503
    data = await resp.json()
    assert "error" in data["db"]
    assert data["redis"] == "ok"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_monitoring_health.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement health check**

Create `services/monitoring_service/health.py`:

```python
from __future__ import annotations

from aiohttp import web

from shared.db import Database
from shared.redis import RedisClient


def create_health_app(db: Database, redis_client: RedisClient) -> web.Application:
    async def health_handler(request: web.Request) -> web.Response:
        checks = {}
        all_ok = True

        # Database check
        try:
            async with db.pool.acquire() as conn:
                await conn.execute("SELECT 1")
            checks["db"] = "ok"
        except Exception as e:
            checks["db"] = f"error: {e}"
            all_ok = False

        # Redis check
        try:
            await redis_client.client.ping()
            checks["redis"] = "ok"
        except Exception as e:
            checks["redis"] = f"error: {e}"
            all_ok = False

        status = 200 if all_ok else 503
        return web.json_response(checks, status=status)

    app = web.Application()
    app.router.add_get("/health", health_handler)
    return app
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_monitoring_health.py -v`
Expected: ALL PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add services/monitoring_service/health.py tests/test_monitoring_health.py
git commit -m "feat: monitoring service health check endpoint"
```

---

## Task 12: Monitoring Service Entrypoint

**Files:**
- Create: `services/monitoring_service/main.py`
- Create: `services/monitoring_service/requirements.txt`
- Create: `services/monitoring_service/Dockerfile`

- [ ] **Step 1: Create requirements.txt**

Create `services/monitoring_service/requirements.txt`:

```
asyncpg>=0.29.0
redis>=5.0.0
aiohttp>=3.9.0
alpaca-py>=0.21.0
```

- [ ] **Step 2: Create the entrypoint**

Create `services/monitoring_service/main.py`:

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
from shared.models import ModelStatus
from shared.redis import RedisClient
from services.execution_service.alpaca_broker import AlpacaBroker
from services.monitoring_service.health import create_health_app
from services.monitoring_service.metrics import MetricsComputer
from services.monitoring_service.fidelity import FidelityTracker
from services.monitoring_service.halt_manager import HaltManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("monitoring_service")


async def monitoring_loop(
    db: Database,
    broker: AlpacaBroker,
    redis_client: RedisClient,
    metrics_computer: MetricsComputer,
    fidelity_tracker: FidelityTracker,
    halt_manager: HaltManager,
    settings: Settings,
    shutdown_event: asyncio.Event,
) -> None:
    equity_history: list[float] = []

    while not shutdown_event.is_set():
        try:
            # 1. Get current account state
            try:
                account = await broker.get_account()
                equity_history.append(account["equity"])
            except Exception as e:
                logger.warning("Failed to get account: %s", e)
                account = {}

            # 2. Compute trade metrics over rolling windows
            all_trades = await db.query_closed_trades(limit=1000)

            trading_metrics = metrics_computer.compute_trading_metrics(all_trades)
            await db.insert_metric_snapshot("trading", "", "all", trading_metrics)

            if len(all_trades) >= 20:
                last_20 = metrics_computer.compute_trading_metrics(all_trades[:20])
                await db.insert_metric_snapshot("trading", "", "last_20", last_20)

            if len(all_trades) >= 50:
                last_50 = metrics_computer.compute_trading_metrics(all_trades[:50])
                await db.insert_metric_snapshot("trading", "", "last_50", last_50)

            # Account metrics
            if account:
                account_metrics = metrics_computer.compute_account_metrics(
                    account, equity_history,
                )
                await db.insert_metric_snapshot("account", "", "all", account_metrics)

            # Per-symbol metrics
            symbols = set(t.symbol for t in all_trades)
            for sym in symbols:
                sym_trades = [t for t in all_trades if t.symbol == sym]
                sym_metrics = metrics_computer.compute_trading_metrics(sym_trades)
                await db.insert_metric_snapshot("symbol", sym, "all", sym_metrics)

            # Per-direction metrics
            for direction in ("long", "short"):
                dir_metrics = metrics_computer.compute_direction_metrics(
                    all_trades, direction,
                )
                await db.insert_metric_snapshot("direction", direction, "all", dir_metrics)

            # 3. Evaluate model fidelity
            fidelity_results = await fidelity_tracker.evaluate_all_models()
            now = datetime.now(timezone.utc)
            statuses = []
            for model_id, status, live_accuracy in fidelity_results:
                reason = f"Live accuracy: {live_accuracy:.2%}"
                await db.upsert_model_status(model_id, status, reason)
                statuses.append(ModelStatus(
                    model_id=model_id,
                    status=status,
                    reason=reason,
                    updated_at=now,
                ))

            # 4. Evaluate halt conditions
            current_drawdown = 0.0
            if account:
                acct_metrics = metrics_computer.compute_account_metrics(
                    account, equity_history,
                )
                current_drawdown = acct_metrics.get("current_drawdown", 0.0)

            alerts = await halt_manager.evaluate(statuses, current_drawdown)

            # 5. Publish status to Redis
            await halt_manager.publish_status(statuses)

            # 6. Publish alerts for Telegram bot (Phase 5B)
            for alert in alerts:
                await redis_client.publish(
                    "monitoring:alerts", json.dumps(alert),
                )

            logger.info(
                "Monitoring tick: %d trades, %d models, %d alerts, drawdown=%.2f%%",
                len(all_trades), len(statuses), len(alerts),
                current_drawdown * 100,
            )

        except Exception:
            logger.exception("Error in monitoring loop")

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=settings.monitoring_interval,
            )
            break
        except asyncio.TimeoutError:
            pass


async def main() -> None:
    settings = Settings()
    db = Database(settings)
    redis_client = RedisClient(settings)

    logger.info("Connecting to TimescaleDB...")
    await db.connect()
    logger.info("Connecting to Redis...")
    await redis_client.connect()

    broker = AlpacaBroker(settings)
    metrics_computer = MetricsComputer()
    fidelity_tracker = FidelityTracker(db, settings)
    halt_manager = HaltManager(db, redis_client, settings)

    # Start health check server
    health_app = create_health_app(db, redis_client)
    runner = web.AppRunner(health_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", settings.monitoring_port)
    await site.start()
    logger.info("Health check server running on :%d", settings.monitoring_port)

    # Graceful shutdown
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

    logger.info(
        "Monitoring service running (interval=%ds). Press Ctrl+C to stop.",
        settings.monitoring_interval,
    )

    await monitoring_loop(
        db, broker, redis_client,
        metrics_computer, fidelity_tracker, halt_manager,
        settings, shutdown_event,
    )

    logger.info("Shutting down...")
    await runner.cleanup()
    await redis_client.disconnect()
    await db.disconnect()
    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 3: Create the Dockerfile**

Create `services/monitoring_service/Dockerfile`:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY services/monitoring_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ /app/shared/
COPY services/execution_service/alpaca_broker.py /app/services/execution_service/alpaca_broker.py
COPY services/execution_service/__init__.py /app/services/execution_service/__init__.py
COPY services/monitoring_service/ /app/services/monitoring_service/
COPY migrations/ /app/migrations/

CMD ["python", "-m", "services.monitoring_service.main"]
```

- [ ] **Step 4: Verify the module imports correctly**

Run: `cd bullforge && python -c "from services.monitoring_service.main import main; print('OK')"`
Expected: `OK`

- [ ] **Step 5: Run the full test suite**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add services/monitoring_service/main.py services/monitoring_service/requirements.txt services/monitoring_service/Dockerfile
git commit -m "feat: monitoring service entrypoint with periodic metrics loop"
```

---

## Task 13: Docker Compose Integration

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add monitoring_service to docker-compose.yml**

Add after the `execution_service` block (before the `volumes:` section) in `docker-compose.yml`:

```yaml
  monitoring_service:
    build:
      context: .
      dockerfile: services/monitoring_service/Dockerfile
    env_file:
      - .env
    environment:
      TIMESCALEDB_HOST: timescaledb
      REDIS_HOST: redis
    ports:
      - "18083:8083"
    depends_on:
      timescaledb:
        condition: service_healthy
      redis:
        condition: service_healthy
      execution_service:
        condition: service_started
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8083/health')"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 15s
    restart: unless-stopped
```

- [ ] **Step 2: Verify docker-compose syntax**

Run: `cd bullforge && docker compose config --quiet`
Expected: No errors

- [ ] **Step 3: Run the full test suite one final time**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add monitoring_service to Docker Compose"
```

---

## Summary

This plan adds the following to BullForge:

| Component | Description |
|---|---|
| **Trade recording** | Order executor records round-trip trades with P&L on position open/close |
| **MetricsComputer** | Pure-function computation of win rate, profit factor, Sharpe ratio, drawdown, per-symbol and per-direction metrics |
| **FidelityTracker** | Evaluates each model's live prediction accuracy against backtested accuracy using a SQL join of predictions with actual bar prices |
| **HaltManager** | 4-tier degradation: warn → disable model → halt direction → full halt, with Redis publishing |
| **Signal aggregator integration** | Filters predictions from disabled models before aggregation |
| **Model service integration** | Includes `model_id` in published prediction messages |
| **Monitoring service** | Periodic loop that computes metrics, evaluates fidelity, manages halts, and publishes to Redis |
| **DB migrations** | `trades`, `model_status`, `halt_events`, `metric_snapshots` tables |

**Phase 5B (Telegram Bot)** will subscribe to `monitoring:alerts` for real-time alerts with action buttons.
**Phase 5C (Dashboard)** will query `metric_snapshots` and subscribe to Redis for SSE live updates.
