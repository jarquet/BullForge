# Order Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep BullForge's `orders` table consistent with Alpaca's authoritative state by adding a reconciler built from a trade-updates WebSocket stream, a 60s polling sweeper, and a shared core that owns all Alpaca→DB translation.

**Architecture:** A new `ReconcilerCore` class in `execution_service` is the single source of truth for applying Alpaca order snapshots to our `orders` and `trades` tables. It is fed by three paths: (1) `OrderExecutor` immediately after submit, (2) `TradeUpdatesStream` for real-time push events, and (3) `PollingSweeper` as a 60s safety net that also runs at startup and on stream reconnect. `OrderExecutor._check_fill` is removed — the core handles all fill bookkeeping.

**Tech Stack:** Python 3.11, asyncio, asyncpg, `alpaca-py` (`TradingStream`, `TradingClient`), pytest + pytest-asyncio, TimescaleDB.

**Spec:** [docs/superpowers/specs/2026-04-14-order-reconciliation-design.md](../specs/2026-04-14-order-reconciliation-design.md)

---

## File Structure

**Create:**
- `bullforge/migrations/010_order_reconciliation.sql`
- `bullforge/services/execution_service/reconciler_core.py`
- `bullforge/services/execution_service/trade_updates_stream.py`
- `bullforge/services/execution_service/polling_sweeper.py`
- `bullforge/tests/test_reconciler_core.py`
- `bullforge/tests/test_trade_updates_stream.py`
- `bullforge/tests/test_polling_sweeper.py`
- `bullforge/tests/test_order_reconciliation_integration.py`

**Modify:**
- `bullforge/shared/models.py` — add `origin`, `last_reconciled_at`, `reconcile_source` fields to `Order`; add `'expired'` to `VALID_ORDER_STATUSES`.
- `bullforge/shared/db.py` — add `query_non_terminal_orders`, `upsert_order_from_alpaca`, `get_order_by_alpaca_id`; extend `insert_order` and `query_orders` for new columns.
- `bullforge/shared/config.py` — add reconciler settings.
- `bullforge/services/execution_service/alpaca_broker.py` — add `list_open_orders(after)` for orphan scan.
- `bullforge/services/execution_service/order_executor.py` — remove `_check_fill`; delegate to `ReconcilerCore`.
- `bullforge/services/execution_service/main.py` — instantiate and start reconciler components.
- `bullforge/tests/test_order_executor.py` — adapt to new submit-then-delegate behavior.

---

## Task 1: Migration — new columns and partial indexes

**Files:**
- Create: `bullforge/migrations/010_order_reconciliation.sql`

- [ ] **Step 1: Write the migration**

Create `bullforge/migrations/010_order_reconciliation.sql`:

```sql
ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'internal'
    CHECK (origin IN ('internal', 'external')),
  ADD COLUMN IF NOT EXISTS last_reconciled_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reconcile_source TEXT
    CHECK (reconcile_source IN ('submit','stream','poll','startup'));

DROP INDEX IF EXISTS idx_orders_alpaca_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_alpaca_id
  ON orders (alpaca_order_id)
  WHERE alpaca_order_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_orders_non_terminal
  ON orders (submitted_at DESC)
  WHERE status NOT IN ('filled','cancelled','rejected','expired');
```

- [ ] **Step 2: Apply migration locally**

Run: `docker compose exec -T timescaledb psql -U postgres -d bullforge < bullforge/migrations/010_order_reconciliation.sql`

Expected: no errors; `\d orders` shows the three new columns.

- [ ] **Step 3: Verify indexes**

Run: `docker compose exec -T timescaledb psql -U postgres -d bullforge -c "\d orders"`

Expected output contains:
- `"idx_orders_alpaca_id" UNIQUE, btree (alpaca_order_id) WHERE alpaca_order_id IS NOT NULL`
- `"idx_orders_non_terminal" btree (submitted_at DESC) WHERE status NOT IN (...)`

- [ ] **Step 4: Commit**

```bash
git add bullforge/migrations/010_order_reconciliation.sql
git commit -m "feat(db): add order reconciliation columns and partial indexes"
```

---

## Task 2: Update `Order` model and status constants

**Files:**
- Modify: `bullforge/shared/models.py`
- Modify: `bullforge/tests/test_execution_models.py`

- [ ] **Step 1: Write a failing test for new Order fields**

Append to `bullforge/tests/test_execution_models.py`:

```python
def test_order_has_reconciliation_fields():
    from shared.models import Order
    from datetime import datetime, timezone
    o = Order(
        symbol="SPY", side="buy", quantity=1.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
    )
    assert o.origin == "internal"
    assert o.last_reconciled_at is None
    assert o.reconcile_source is None


def test_order_origin_must_be_valid():
    import pytest
    from shared.models import Order
    from datetime import datetime, timezone
    with pytest.raises(ValueError, match="origin"):
        Order(
            symbol="SPY", side="buy", quantity=1.0, order_type="market",
            submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
            origin="bogus",
        )


def test_order_status_expired_is_valid():
    from shared.models import Order
    from datetime import datetime, timezone
    o = Order(
        symbol="SPY", side="buy", quantity=1.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
        status="expired",
    )
    assert o.status == "expired"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_execution_models.py -v -k "reconciliation_fields or origin_must_be_valid or status_expired"`

Expected: FAIL — `Order` has no `origin` field, `'expired'` not in `VALID_ORDER_STATUSES`.

- [ ] **Step 3: Update `Order` and constants**

In `bullforge/shared/models.py`, modify the `VALID_ORDER_STATUSES` set (around line 136):

```python
VALID_ORDER_STATUSES = {
    "pending", "submitted", "filled", "partially_filled",
    "cancelled", "rejected", "expired",
}

VALID_ORDER_ORIGINS = {"internal", "external"}
VALID_ORDER_RECONCILE_SOURCES = {"submit", "stream", "poll", "startup"}
```

Modify the `Order` dataclass (around line 168) to add three new fields BEFORE `id` (keep `id` last so existing positional usage is unaffected):

```python
@dataclass(frozen=True)
class Order:
    symbol: str
    side: str
    quantity: float
    order_type: str
    submitted_at: datetime
    signal_strength: float
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
```

Extend `__post_init__` with validation (append after the existing `filled_at` block):

```python
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
            or self.last_reconciled_at.tzinfo != timezone.utc
        ):
            raise ValueError(
                f"last_reconciled_at must be UTC when provided, got tzinfo={self.last_reconciled_at.tzinfo}"
            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_execution_models.py -v`

Expected: PASS for all three new tests. Existing tests still pass.

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/models.py bullforge/tests/test_execution_models.py
git commit -m "feat(models): add reconciliation fields and 'expired' status to Order"
```

---

## Task 3: Extend `Database` — non-terminal query and alpaca-id lookup

**Files:**
- Modify: `bullforge/shared/db.py`
- Modify: `bullforge/tests/test_order_db.py`

- [ ] **Step 1: Write failing tests**

Append to `bullforge/tests/test_order_db.py` (use the existing DB fixture from the file — inspect it first if unfamiliar):

```python
import pytest
from datetime import datetime, timezone, timedelta
from shared.models import Order


@pytest.mark.asyncio
async def test_query_non_terminal_orders_excludes_terminal(db):
    now = datetime.now(timezone.utc)
    terminal = Order(
        symbol="SPY", side="buy", quantity=1.0, order_type="market",
        submitted_at=now, signal_strength=0.5,
        alpaca_order_id="alp-t", status="filled",
    )
    open_order = Order(
        symbol="QQQ", side="buy", quantity=1.0, order_type="market",
        submitted_at=now, signal_strength=0.5,
        alpaca_order_id="alp-o", status="submitted",
    )
    await db.insert_order(terminal)
    await db.insert_order(open_order)

    rows = await db.query_non_terminal_orders(older_than=timedelta(hours=48))
    alpaca_ids = {r.alpaca_order_id for r in rows}
    assert "alp-o" in alpaca_ids
    assert "alp-t" not in alpaca_ids


@pytest.mark.asyncio
async def test_query_non_terminal_orders_respects_window(db):
    old_time = datetime.now(timezone.utc) - timedelta(hours=72)
    ancient = Order(
        symbol="SPY", side="buy", quantity=1.0, order_type="market",
        submitted_at=old_time, signal_strength=0.5,
        alpaca_order_id="alp-ancient", status="submitted",
    )
    await db.insert_order(ancient)

    rows = await db.query_non_terminal_orders(older_than=timedelta(hours=48))
    assert all(r.alpaca_order_id != "alp-ancient" for r in rows)


@pytest.mark.asyncio
async def test_get_order_by_alpaca_id_returns_none_for_unknown(db):
    result = await db.get_order_by_alpaca_id("does-not-exist")
    assert result is None


@pytest.mark.asyncio
async def test_get_order_by_alpaca_id_returns_match(db):
    o = Order(
        symbol="SPY", side="buy", quantity=1.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
        alpaca_order_id="alp-xyz", status="submitted",
    )
    await db.insert_order(o)
    result = await db.get_order_by_alpaca_id("alp-xyz")
    assert result is not None
    assert result.alpaca_order_id == "alp-xyz"
    assert result.symbol == "SPY"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_order_db.py -v -k "non_terminal or by_alpaca_id"`

Expected: FAIL — methods don't exist.

- [ ] **Step 3: Implement DB methods**

In `bullforge/shared/db.py`, first update `insert_order` and `query_orders` to include the new columns.

Update `insert_order` (around line 479):

```python
    async def insert_order(self, order: Order) -> int:
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO orders
                    (symbol, side, quantity, order_type, submitted_at,
                     signal_strength, alpaca_order_id, status,
                     filled_at, filled_price, filled_quantity, limit_price,
                     origin, last_reconciled_at, reconcile_source)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                RETURNING id
                """,
                order.symbol, order.side, order.quantity, order.order_type,
                order.submitted_at, order.signal_strength, order.alpaca_order_id,
                order.status, order.filled_at, order.filled_price,
                order.filled_quantity, order.limit_price,
                order.origin, order.last_reconciled_at, order.reconcile_source,
            )
        return row_id
```

Update `query_orders` SELECT list and the `Order(...)` construction to include the three new columns (`origin`, `last_reconciled_at`, `reconcile_source`).

Add two new methods below `update_order_status` (around line 528):

```python
    async def get_order_by_alpaca_id(self, alpaca_order_id: str) -> Optional[Order]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, symbol, side, quantity, order_type, submitted_at,
                       signal_strength, alpaca_order_id, status,
                       filled_at, filled_price, filled_quantity, limit_price,
                       origin, last_reconciled_at, reconcile_source
                FROM orders
                WHERE alpaca_order_id = $1
                """,
                alpaca_order_id,
            )
        if row is None:
            return None
        return Order(
            id=row["id"], symbol=row["symbol"], side=row["side"],
            quantity=row["quantity"], order_type=row["order_type"],
            submitted_at=row["submitted_at"],
            signal_strength=row["signal_strength"],
            alpaca_order_id=row["alpaca_order_id"], status=row["status"],
            filled_at=row["filled_at"], filled_price=row["filled_price"],
            filled_quantity=row["filled_quantity"],
            limit_price=row["limit_price"],
            origin=row["origin"],
            last_reconciled_at=row["last_reconciled_at"],
            reconcile_source=row["reconcile_source"],
        )

    async def query_non_terminal_orders(
        self, older_than: timedelta,
    ) -> list[Order]:
        cutoff = datetime.now(timezone.utc) - older_than
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, symbol, side, quantity, order_type, submitted_at,
                       signal_strength, alpaca_order_id, status,
                       filled_at, filled_price, filled_quantity, limit_price,
                       origin, last_reconciled_at, reconcile_source
                FROM orders
                WHERE status NOT IN ('filled','cancelled','rejected','expired')
                  AND submitted_at >= $1
                  AND alpaca_order_id IS NOT NULL
                ORDER BY submitted_at DESC
                """,
                cutoff,
            )
        return [
            Order(
                id=r["id"], symbol=r["symbol"], side=r["side"],
                quantity=r["quantity"], order_type=r["order_type"],
                submitted_at=r["submitted_at"],
                signal_strength=r["signal_strength"],
                alpaca_order_id=r["alpaca_order_id"], status=r["status"],
                filled_at=r["filled_at"], filled_price=r["filled_price"],
                filled_quantity=r["filled_quantity"],
                limit_price=r["limit_price"],
                origin=r["origin"],
                last_reconciled_at=r["last_reconciled_at"],
                reconcile_source=r["reconcile_source"],
            )
            for r in rows
        ]
```

Ensure `from datetime import timedelta` is imported at top of file.

- [ ] **Step 4: Run tests to verify they pass**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_order_db.py -v`

Expected: all PASS (new and existing).

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/db.py bullforge/tests/test_order_db.py
git commit -m "feat(db): add non-terminal order query and alpaca-id lookup"
```

---

## Task 4: Implement `upsert_order_from_alpaca`

**Files:**
- Modify: `bullforge/shared/db.py`
- Modify: `bullforge/tests/test_order_db.py`

- [ ] **Step 1: Write failing tests**

Append to `bullforge/tests/test_order_db.py`:

```python
@pytest.mark.asyncio
async def test_upsert_inserts_new_external_order(db):
    snapshot = {
        "id": "alp-new-1",
        "symbol": "SPY",
        "side": "buy",
        "qty": 3.0,
        "order_type": "market",
        "submitted_at": datetime.now(timezone.utc),
        "status": "filled",
        "filled_qty": 3.0,
        "filled_avg_price": 420.0,
        "filled_at": datetime.now(timezone.utc),
    }
    result = await db.upsert_order_from_alpaca(snapshot, source="startup")
    assert result.inserted is True
    assert result.old_status is None
    assert result.new_status == "filled"
    assert result.order.origin == "external"


@pytest.mark.asyncio
async def test_upsert_updates_existing_order_status(db):
    initial = Order(
        symbol="SPY", side="buy", quantity=1.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
        alpaca_order_id="alp-exist", status="submitted",
    )
    await db.insert_order(initial)

    snapshot = {
        "id": "alp-exist",
        "symbol": "SPY",
        "side": "buy",
        "qty": 1.0,
        "order_type": "market",
        "submitted_at": datetime.now(timezone.utc),
        "status": "cancelled",
    }
    result = await db.upsert_order_from_alpaca(snapshot, source="poll")
    assert result.inserted is False
    assert result.old_status == "submitted"
    assert result.new_status == "cancelled"
    assert result.order.reconcile_source == "poll"
    assert result.order.origin == "internal"  # preserved


@pytest.mark.asyncio
async def test_upsert_is_idempotent_on_same_terminal_state(db):
    snapshot = {
        "id": "alp-idem",
        "symbol": "SPY",
        "side": "buy",
        "qty": 1.0,
        "order_type": "market",
        "submitted_at": datetime.now(timezone.utc),
        "status": "filled",
        "filled_qty": 1.0,
        "filled_avg_price": 100.0,
        "filled_at": datetime.now(timezone.utc),
    }
    await db.upsert_order_from_alpaca(snapshot, source="stream")
    result = await db.upsert_order_from_alpaca(snapshot, source="poll")
    assert result.old_status == "filled"
    assert result.new_status == "filled"
```

- [ ] **Step 2: Run tests — expect FAIL**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_order_db.py -v -k "upsert"`

Expected: FAIL — method missing.

- [ ] **Step 3: Implement `upsert_order_from_alpaca`**

In `bullforge/shared/db.py`, add a result dataclass and a status normalizer near the top (below imports):

```python
@dataclass(frozen=True)
class UpsertResult:
    order: "Order"
    old_status: Optional[str]  # None if newly inserted
    new_status: str
    inserted: bool


# Alpaca uses a larger set of statuses than our domain model. Map them
# onto our canonical VALID_ORDER_STATUSES so we don't fail upserts and
# don't have to teach the rest of the codebase Alpaca-specific states.
_ALPACA_STATUS_MAP = {
    "new": "submitted",
    "accepted": "submitted",
    "pending_new": "submitted",
    "accepted_for_bidding": "submitted",
    "held": "submitted",
    "pending_cancel": "submitted",
    "pending_replace": "submitted",
    "calculated": "submitted",
    "suspended": "submitted",
    "replaced": "cancelled",
    "canceled": "cancelled",
    "cancelled": "cancelled",
    "rejected": "rejected",
    "filled": "filled",
    "partially_filled": "partially_filled",
    "expired": "expired",
    "done_for_day": "expired",
    "stopped": "filled",
}


def _normalize_alpaca_status(raw: str) -> str:
    mapped = _ALPACA_STATUS_MAP.get(raw)
    if mapped is None:
        raise ValueError(f"unknown Alpaca order status: {raw!r}")
    return mapped
```

Add the method to `Database` (after `query_non_terminal_orders`). The method normalizes `snapshot["status"]` via `_normalize_alpaca_status` before writing and returning:

```python
    async def upsert_order_from_alpaca(
        self, snapshot: dict, source: str,
    ) -> UpsertResult:
        alpaca_id = snapshot["id"]
        canonical_status = _normalize_alpaca_status(snapshot["status"])
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                existing = await conn.fetchrow(
                    "SELECT status, origin FROM orders "
                    "WHERE alpaca_order_id = $1 FOR UPDATE",
                    alpaca_id,
                )
                now_ts = datetime.now(timezone.utc)
                if existing is None:
                    new_id = await conn.fetchval(
                        """
                        INSERT INTO orders
                          (symbol, side, quantity, order_type, submitted_at,
                           signal_strength, alpaca_order_id, status,
                           filled_at, filled_price, filled_quantity,
                           origin, last_reconciled_at, reconcile_source)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
                                'external',$12,$13)
                        RETURNING id
                        """,
                        snapshot["symbol"],
                        snapshot["side"],
                        float(snapshot["qty"]),
                        snapshot.get("order_type", "market"),
                        snapshot.get("submitted_at") or now_ts,
                        0.0,  # signal_strength — unknown for external
                        alpaca_id,
                        canonical_status,
                        snapshot.get("filled_at"),
                        snapshot.get("filled_avg_price"),
                        snapshot.get("filled_qty"),
                        now_ts,
                        source,
                    )
                    order = await self.get_order_by_alpaca_id(alpaca_id)
                    return UpsertResult(
                        order=order, old_status=None,
                        new_status=canonical_status, inserted=True,
                    )

                old_status = existing["status"]
                await conn.execute(
                    """
                    UPDATE orders
                    SET status = $2,
                        filled_at = COALESCE($3, filled_at),
                        filled_price = COALESCE($4, filled_price),
                        filled_quantity = COALESCE($5, filled_quantity),
                        last_reconciled_at = $6,
                        reconcile_source = $7
                    WHERE alpaca_order_id = $1
                    """,
                    alpaca_id,
                    snapshot["status"],
                    snapshot.get("filled_at"),
                    snapshot.get("filled_avg_price"),
                    snapshot.get("filled_qty"),
                    now_ts,
                    source,
                )
        order = await self.get_order_by_alpaca_id(alpaca_id)
        return UpsertResult(
            order=order, old_status=old_status,
            new_status=canonical_status, inserted=False,
        )
```

Export `UpsertResult` from `shared/db.py` (add to `__all__` if present, otherwise skip).

- [ ] **Step 4: Run tests — expect PASS**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_order_db.py -v -k "upsert"`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/db.py bullforge/tests/test_order_db.py
git commit -m "feat(db): upsert_order_from_alpaca with transition tracking"
```

---

## Task 5: Add reconciler settings to `Settings`

**Files:**
- Modify: `bullforge/shared/config.py`
- Modify: `bullforge/tests/test_config.py`

- [ ] **Step 1: Write failing test**

Append to `bullforge/tests/test_config.py`:

```python
def test_reconciler_settings_have_defaults(monkeypatch):
    for var in (
        "EXECUTION_RECONCILER_POLL_INTERVAL_S",
        "EXECUTION_RECONCILER_ORPHAN_EVERY",
        "EXECUTION_RECONCILER_ORPHAN_WINDOW_HOURS",
        "EXECUTION_RECONCILER_STREAM_ENABLED",
    ):
        monkeypatch.delenv(var, raising=False)
    from shared.config import Settings
    s = Settings()
    assert s.execution_reconciler_poll_interval_s == 60
    assert s.execution_reconciler_orphan_every == 5
    assert s.execution_reconciler_orphan_window_hours == 48
    assert s.execution_reconciler_stream_enabled is True
```

- [ ] **Step 2: Run — expect FAIL**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_config.py -v -k reconciler`

Expected: FAIL — attributes missing.

- [ ] **Step 3: Add settings**

In `bullforge/shared/config.py`, after `execution_min_agreement` (around line 119), add:

```python
    execution_reconciler_poll_interval_s: int = field(
        default_factory=lambda: int(
            os.getenv("EXECUTION_RECONCILER_POLL_INTERVAL_S", "60")
        )
    )
    execution_reconciler_orphan_every: int = field(
        default_factory=lambda: int(
            os.getenv("EXECUTION_RECONCILER_ORPHAN_EVERY", "5")
        )
    )
    execution_reconciler_orphan_window_hours: int = field(
        default_factory=lambda: int(
            os.getenv("EXECUTION_RECONCILER_ORPHAN_WINDOW_HOURS", "48")
        )
    )
    execution_reconciler_stream_enabled: bool = field(
        default_factory=lambda: os.getenv(
            "EXECUTION_RECONCILER_STREAM_ENABLED", "true"
        ).lower() == "true"
    )
```

- [ ] **Step 4: Run — expect PASS**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_config.py -v -k reconciler`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/config.py bullforge/tests/test_config.py
git commit -m "feat(config): add reconciler interval, orphan, and stream settings"
```

---

## Task 6: Extend `AlpacaBroker` — `list_open_orders(after)`

**Files:**
- Modify: `bullforge/services/execution_service/alpaca_broker.py`
- Modify: `bullforge/tests/test_alpaca_broker.py`

- [ ] **Step 1: Write failing test**

Append to `bullforge/tests/test_alpaca_broker.py`:

```python
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch


@pytest.mark.asyncio
async def test_list_open_orders_calls_alpaca_with_bounded_window():
    from services.execution_service.alpaca_broker import AlpacaBroker
    from shared.config import Settings

    fake_client = MagicMock()
    fake_client.get_orders.return_value = [
        MagicMock(
            id="alp-1", symbol="SPY", side=MagicMock(value="buy"),
            qty="1", status=MagicMock(value="new"),
            filled_qty="0", filled_avg_price=None, filled_at=None,
            submitted_at=datetime.now(timezone.utc),
            order_type=MagicMock(value="market"),
        ),
    ]
    with patch(
        "services.execution_service.alpaca_broker.TradingClient",
        return_value=fake_client,
    ):
        broker = AlpacaBroker(Settings())
        after = datetime.now(timezone.utc) - timedelta(hours=48)
        results = await broker.list_open_orders(after=after)

    assert len(results) == 1
    assert results[0]["id"] == "alp-1"
    assert results[0]["status"] == "new"
    args, kwargs = fake_client.get_orders.call_args
    req = args[0] if args else kwargs.get("filter")
    assert req is not None
```

- [ ] **Step 2: Run — expect FAIL**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_alpaca_broker.py -v -k list_open_orders`

Expected: FAIL — method missing.

- [ ] **Step 3: Implement `list_open_orders`**

In `bullforge/services/execution_service/alpaca_broker.py`:

Add import at top:

```python
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus
```

Add public async method (after `close_position`):

```python
    async def list_open_orders(self, after) -> list[dict]:
        return await asyncio.to_thread(self._sync_list_open_orders, after)
```

Add sync helper (after `_sync_close_position`):

```python
    def _sync_list_open_orders(self, after) -> list[dict]:
        request = GetOrdersRequest(
            status=QueryOrderStatus.OPEN,
            after=after,
            limit=500,
        )
        orders = self._client.get_orders(request)
        return [self._format_order_snapshot(o) for o in orders]

    @staticmethod
    def _format_order_snapshot(o) -> dict:
        status = o.status.value if hasattr(o.status, "value") else str(o.status)
        side = o.side.value if hasattr(o.side, "value") else str(o.side)
        order_type = (
            o.order_type.value if hasattr(o.order_type, "value")
            else str(o.order_type)
        )
        filled_at = o.filled_at
        if filled_at is not None and filled_at.tzinfo is None:
            filled_at = filled_at.replace(tzinfo=timezone.utc)
        submitted_at = o.submitted_at
        if submitted_at is not None and submitted_at.tzinfo is None:
            submitted_at = submitted_at.replace(tzinfo=timezone.utc)
        return {
            "id": str(o.id),
            "symbol": o.symbol,
            "side": side,
            "qty": float(o.qty),
            "order_type": order_type,
            "status": status,
            "submitted_at": submitted_at,
            "filled_qty": float(o.filled_qty) if o.filled_qty else None,
            "filled_avg_price": (
                float(o.filled_avg_price) if o.filled_avg_price else None
            ),
            "filled_at": filled_at,
        }
```

Also refactor `_sync_get_order` to reuse `_format_order_snapshot` so the stream/sweep/submit paths all produce the same shape. Replace the body of `_sync_get_order` with:

```python
    def _sync_get_order(self, alpaca_order_id: str) -> dict:
        order = self._client.get_order_by_id(alpaca_order_id)
        return self._format_order_snapshot(order)
```

- [ ] **Step 4: Run — expect PASS**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_alpaca_broker.py -v`

Expected: PASS (including existing tests — the `_sync_get_order` refactor must preserve the previously-tested contract).

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/execution_service/alpaca_broker.py bullforge/tests/test_alpaca_broker.py
git commit -m "feat(broker): list_open_orders with bounded window; unify snapshot shape"
```

---

## Task 7: Build `ReconcilerCore` — status-only transitions

**Files:**
- Create: `bullforge/services/execution_service/reconciler_core.py`
- Create: `bullforge/tests/test_reconciler_core.py`

- [ ] **Step 1: Write failing tests for status-only transitions**

Create `bullforge/tests/test_reconciler_core.py`:

```python
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from shared.db import UpsertResult
from shared.models import Order
from services.execution_service.reconciler_core import ReconcilerCore


def _snapshot(status="submitted", alpaca_id="alp-1", symbol="SPY", side="buy"):
    return {
        "id": alpaca_id, "symbol": symbol, "side": side,
        "qty": 1.0, "order_type": "market",
        "submitted_at": datetime.now(timezone.utc),
        "status": status,
        "filled_qty": 1.0 if status == "filled" else None,
        "filled_avg_price": 100.0 if status == "filled" else None,
        "filled_at": datetime.now(timezone.utc) if status == "filled" else None,
    }


def _order(status="submitted", side="buy", origin="internal", order_id=1):
    return Order(
        symbol="SPY", side=side, quantity=1.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
        alpaca_order_id="alp-1", status=status, origin=origin,
        id=order_id,
    )


def _upsert_result(old_status, new_status, inserted=False, origin="internal"):
    return UpsertResult(
        order=_order(status=new_status, origin=origin),
        old_status=old_status, new_status=new_status, inserted=inserted,
    )


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.upsert_order_from_alpaca = AsyncMock()
    db.insert_trade = AsyncMock(return_value=1)
    db.update_trade_exit = AsyncMock()
    db.get_open_trade = AsyncMock(return_value=None)
    return db


@pytest.fixture
def mock_redis():
    r = AsyncMock()
    r.publish = AsyncMock()
    return r


@pytest.fixture
def core(mock_db, mock_redis):
    return ReconcilerCore(db=mock_db, redis_client=mock_redis)


@pytest.mark.asyncio
async def test_submitted_to_cancelled_publishes_and_does_not_insert_trade(
    core, mock_db, mock_redis,
):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="submitted", new_status="cancelled",
    )
    await core.apply_alpaca_order(_snapshot(status="cancelled"), source="poll")
    mock_db.insert_trade.assert_not_called()
    mock_db.update_trade_exit.assert_not_called()
    mock_redis.publish.assert_awaited_once()
    channel, payload = mock_redis.publish.await_args.args
    assert channel == "execution:orders"
    assert json.loads(payload)["status"] == "cancelled"


@pytest.mark.asyncio
async def test_no_status_change_does_not_publish(core, mock_db, mock_redis):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="filled", new_status="filled",
    )
    await core.apply_alpaca_order(_snapshot(status="filled"), source="poll")
    mock_redis.publish.assert_not_called()
    mock_db.insert_trade.assert_not_called()


@pytest.mark.asyncio
async def test_filled_to_cancelled_emits_anomaly(core, mock_db, mock_redis):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="filled", new_status="cancelled",
    )
    await core.apply_alpaca_order(_snapshot(status="cancelled"), source="stream")
    channels = [call.args[0] for call in mock_redis.publish.await_args_list]
    assert "execution:orders:anomaly" in channels
    mock_db.update_trade_exit.assert_not_called()
```

- [ ] **Step 2: Run — expect FAIL**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_reconciler_core.py -v`

Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Implement status-transition logic**

Create `bullforge/services/execution_service/reconciler_core.py`:

```python
from __future__ import annotations

import json
import logging
from typing import Optional

from shared.db import Database
from shared.models import Order, Trade
from shared.redis import RedisClient

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = {"filled", "cancelled", "rejected", "expired"}


class ReconcilerCore:
    def __init__(
        self,
        db: Database,
        redis_client: Optional[RedisClient] = None,
    ):
        self._db = db
        self._redis = redis_client

    async def apply_alpaca_order(
        self, alpaca_order: dict, source: str,
    ) -> None:
        result = await self._db.upsert_order_from_alpaca(
            alpaca_order, source=source,
        )
        old_status = result.old_status
        new_status = result.new_status
        order = result.order

        status_changed = old_status != new_status
        if status_changed:
            logger.info(
                "reconciler alpaca_id=%s old=%s new=%s source=%s origin=%s",
                order.alpaca_order_id, old_status, new_status, source,
                order.origin,
            )

        if old_status == "filled" and new_status in TERMINAL_STATUSES and new_status != "filled":
            logger.error(
                "anomaly: order %s went from filled → %s",
                order.alpaca_order_id, new_status,
            )
            await self._publish("execution:orders:anomaly", order)
            await self._publish("execution:orders", order)
            return

        if status_changed and new_status == "filled":
            await self._handle_fill(order)

        if status_changed:
            await self._publish("execution:orders", order)

    async def _handle_fill(self, order: Order) -> None:
        # Implemented in Task 8
        pass

    async def _publish(self, channel: str, order: Order) -> None:
        if self._redis is None:
            return
        payload = json.dumps({
            "symbol": order.symbol,
            "side": order.side,
            "quantity": order.quantity,
            "status": order.status,
            "filled_price": order.filled_price,
            "alpaca_order_id": order.alpaca_order_id,
            "origin": order.origin,
        })
        await self._redis.publish(channel, payload)
```

- [ ] **Step 4: Run — expect PASS**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_reconciler_core.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/execution_service/reconciler_core.py bullforge/tests/test_reconciler_core.py
git commit -m "feat(reconciler): status transition logic + anomaly detection"
```

---

## Task 8: `ReconcilerCore` — fill → trade side-effects

**Files:**
- Modify: `bullforge/services/execution_service/reconciler_core.py`
- Modify: `bullforge/tests/test_reconciler_core.py`

- [ ] **Step 1: Write failing tests**

Append to `bullforge/tests/test_reconciler_core.py`:

```python
from shared.models import Trade


@pytest.mark.asyncio
async def test_fill_on_buy_with_no_open_trade_inserts_entry_trade(
    core, mock_db, mock_redis,
):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="submitted", new_status="filled",
    )
    mock_db.get_open_trade.return_value = None
    await core.apply_alpaca_order(_snapshot(status="filled"), source="stream")
    mock_db.insert_trade.assert_awaited_once()
    trade_arg = mock_db.insert_trade.await_args.args[0]
    assert isinstance(trade_arg, Trade)
    assert trade_arg.side == "long"
    assert trade_arg.entry_order_id == 1


@pytest.mark.asyncio
async def test_fill_on_sell_with_open_long_closes_trade(
    core, mock_db, mock_redis,
):
    open_trade = Trade(
        id=77, symbol="SPY", side="long",
        entry_time=datetime(2026, 4, 14, 13, 0, tzinfo=timezone.utc),
        entry_price=100.0, quantity=1.0, entry_order_id=10,
    )
    mock_db.get_open_trade.return_value = open_trade
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="submitted", new_status="filled",
    )
    snap = _snapshot(status="filled", side="sell")
    snap["filled_avg_price"] = 110.0
    await core.apply_alpaca_order(snap, source="stream")
    mock_db.update_trade_exit.assert_awaited_once()
    kwargs = mock_db.update_trade_exit.await_args.kwargs
    assert kwargs["trade_id"] == 77
    assert kwargs["pnl"] == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_duplicate_fill_event_does_not_insert_duplicate_trade(
    core, mock_db, mock_redis,
):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="filled", new_status="filled",
    )
    await core.apply_alpaca_order(_snapshot(status="filled"), source="stream")
    mock_db.insert_trade.assert_not_called()
    mock_db.update_trade_exit.assert_not_called()
```

Note: `Trade` has fields `id, symbol, side, entry_time, entry_price, quantity, entry_order_id`. If the actual dataclass differs, adjust the test construction accordingly (inspect `bullforge/shared/models.py` first).

- [ ] **Step 2: Run — expect FAIL**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_reconciler_core.py -v -k "fill_on_"`

Expected: FAIL — `_handle_fill` is a no-op pass.

- [ ] **Step 3: Implement `_handle_fill`**

In `bullforge/services/execution_service/reconciler_core.py`, replace `_handle_fill`:

```python
    async def _handle_fill(self, order: Order) -> None:
        if order.filled_price is None or order.filled_at is None:
            logger.warning(
                "fill event for %s missing price/time; skipping trade bookkeeping",
                order.alpaca_order_id,
            )
            return

        open_trade = await self._db.get_open_trade(order.symbol)

        is_closing = open_trade is not None and (
            (open_trade.side == "long" and order.side == "sell")
            or (open_trade.side == "short" and order.side == "buy")
        )

        if is_closing:
            pnl = (order.filled_price - open_trade.entry_price) * open_trade.quantity
            if open_trade.side == "short":
                pnl = -pnl
            hold_secs = int(
                (order.filled_at - open_trade.entry_time).total_seconds()
            )
            await self._db.update_trade_exit(
                trade_id=open_trade.id,
                exit_time=order.filled_at,
                exit_price=order.filled_price,
                exit_order_id=order.id,
                pnl=pnl,
                hold_duration_seconds=hold_secs,
            )
        else:
            trade_side = "long" if order.side == "buy" else "short"
            trade = Trade(
                symbol=order.symbol,
                side=trade_side,
                entry_time=order.filled_at,
                entry_price=order.filled_price,
                quantity=order.filled_quantity or order.quantity,
                entry_order_id=order.id,
            )
            await self._db.insert_trade(trade)
```

- [ ] **Step 4: Run — expect PASS**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_reconciler_core.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/execution_service/reconciler_core.py bullforge/tests/test_reconciler_core.py
git commit -m "feat(reconciler): handle fill → trade insert/close with PnL"
```

---

## Task 9: `PollingSweeper` — periodic + orphan detection

**Files:**
- Create: `bullforge/services/execution_service/polling_sweeper.py`
- Create: `bullforge/tests/test_polling_sweeper.py`

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/test_polling_sweeper.py`:

```python
import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from shared.models import Order
from services.execution_service.polling_sweeper import PollingSweeper


def _order(alpaca_id, status="submitted"):
    return Order(
        symbol="SPY", side="buy", quantity=1.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
        alpaca_order_id=alpaca_id, status=status, id=1,
    )


@pytest.mark.asyncio
async def test_sweep_fetches_only_non_terminal_and_forwards():
    db = AsyncMock()
    db.query_non_terminal_orders = AsyncMock(return_value=[_order("a"), _order("b")])
    broker = AsyncMock()
    broker.get_order = AsyncMock(side_effect=lambda aid: {
        "id": aid, "symbol": "SPY", "side": "buy", "qty": 1.0,
        "order_type": "market",
        "submitted_at": datetime.now(timezone.utc),
        "status": "cancelled",
    })
    broker.list_open_orders = AsyncMock(return_value=[])
    core = AsyncMock()

    sweeper = PollingSweeper(
        db=db, broker=broker, core=core,
        interval_s=60, orphan_every=5, orphan_window_hours=48,
    )
    await sweeper.sweep_now()

    assert core.apply_alpaca_order.await_count == 2
    for call in core.apply_alpaca_order.await_args_list:
        assert call.kwargs.get("source") == "poll" or call.args[1] == "poll"


@pytest.mark.asyncio
async def test_sweep_continues_when_one_fetch_fails():
    db = AsyncMock()
    db.query_non_terminal_orders = AsyncMock(
        return_value=[_order("good"), _order("bad")]
    )
    broker = AsyncMock()

    async def get_order(aid):
        if aid == "bad":
            raise RuntimeError("boom")
        return {
            "id": aid, "symbol": "SPY", "side": "buy", "qty": 1.0,
            "order_type": "market",
            "submitted_at": datetime.now(timezone.utc),
            "status": "cancelled",
        }
    broker.get_order = AsyncMock(side_effect=get_order)
    broker.list_open_orders = AsyncMock(return_value=[])
    core = AsyncMock()

    sweeper = PollingSweeper(
        db=db, broker=broker, core=core,
        interval_s=60, orphan_every=5, orphan_window_hours=48,
    )
    await sweeper.sweep_now()

    assert core.apply_alpaca_order.await_count == 1


@pytest.mark.asyncio
async def test_orphan_scan_runs_every_n_ticks_only():
    db = AsyncMock()
    db.query_non_terminal_orders = AsyncMock(return_value=[])
    db.get_order_by_alpaca_id = AsyncMock(return_value=None)
    broker = AsyncMock()
    broker.list_open_orders = AsyncMock(return_value=[
        {"id": "orphan-1", "symbol": "SPY", "side": "buy", "qty": 1.0,
         "order_type": "market",
         "submitted_at": datetime.now(timezone.utc), "status": "new"},
    ])
    core = AsyncMock()

    sweeper = PollingSweeper(
        db=db, broker=broker, core=core,
        interval_s=60, orphan_every=3, orphan_window_hours=48,
    )
    for _ in range(2):
        await sweeper.sweep_now()
    assert broker.list_open_orders.await_count == 0

    await sweeper.sweep_now()  # third tick
    assert broker.list_open_orders.await_count == 1
    # Orphan forwarded to core with source='startup' or 'poll' — either is fine
    assert core.apply_alpaca_order.await_count == 1


@pytest.mark.asyncio
async def test_orphan_scan_skips_known_alpaca_ids():
    db = AsyncMock()
    db.query_non_terminal_orders = AsyncMock(return_value=[])
    db.get_order_by_alpaca_id = AsyncMock(return_value=_order("known"))
    broker = AsyncMock()
    broker.list_open_orders = AsyncMock(return_value=[
        {"id": "known", "symbol": "SPY", "side": "buy", "qty": 1.0,
         "order_type": "market",
         "submitted_at": datetime.now(timezone.utc), "status": "new"},
    ])
    core = AsyncMock()

    sweeper = PollingSweeper(
        db=db, broker=broker, core=core,
        interval_s=60, orphan_every=1, orphan_window_hours=48,
    )
    await sweeper.sweep_now()
    core.apply_alpaca_order.assert_not_called()
```

- [ ] **Step 2: Run — expect FAIL**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_polling_sweeper.py -v`

Expected: FAIL — module missing.

- [ ] **Step 3: Implement `PollingSweeper`**

Create `bullforge/services/execution_service/polling_sweeper.py`:

```python
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from shared.db import Database
from services.execution_service.alpaca_broker import AlpacaBroker
from services.execution_service.reconciler_core import ReconcilerCore

logger = logging.getLogger(__name__)


class PollingSweeper:
    def __init__(
        self,
        db: Database,
        broker: AlpacaBroker,
        core: ReconcilerCore,
        interval_s: int = 60,
        orphan_every: int = 5,
        orphan_window_hours: int = 48,
    ):
        self._db = db
        self._broker = broker
        self._core = core
        self._interval_s = interval_s
        self._orphan_every = orphan_every
        self._orphan_window = timedelta(hours=orphan_window_hours)
        self._tick_count = 0
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.sweep_now()
            except Exception:
                logger.exception("PollingSweeper tick failed")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._interval_s,
                )
            except asyncio.TimeoutError:
                pass

    async def sweep_now(self) -> None:
        self._tick_count += 1
        await self._reconcile_non_terminal()
        if self._tick_count % self._orphan_every == 0:
            await self._scan_orphans()

    async def _reconcile_non_terminal(self) -> None:
        orders = await self._db.query_non_terminal_orders(
            older_than=self._orphan_window,
        )
        for o in orders:
            try:
                snap = await self._broker.get_order(o.alpaca_order_id)
            except Exception as e:
                logger.warning(
                    "sweeper: get_order failed for %s: %s",
                    o.alpaca_order_id, e,
                )
                continue
            try:
                await self._core.apply_alpaca_order(snap, source="poll")
            except Exception:
                logger.exception(
                    "sweeper: apply_alpaca_order failed for %s",
                    o.alpaca_order_id,
                )

    async def _scan_orphans(self) -> None:
        after = datetime.now(timezone.utc) - self._orphan_window
        try:
            snapshots = await self._broker.list_open_orders(after=after)
        except Exception:
            logger.exception("sweeper: list_open_orders failed")
            return
        for snap in snapshots:
            alpaca_id = snap["id"]
            existing = await self._db.get_order_by_alpaca_id(alpaca_id)
            if existing is not None:
                continue
            try:
                await self._core.apply_alpaca_order(snap, source="poll")
            except Exception:
                logger.exception(
                    "sweeper: orphan ingest failed for %s", alpaca_id,
                )
```

- [ ] **Step 4: Run — expect PASS**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_polling_sweeper.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/execution_service/polling_sweeper.py bullforge/tests/test_polling_sweeper.py
git commit -m "feat(reconciler): polling sweeper with orphan detection"
```

---

## Task 10: `TradeUpdatesStream` — live push with reconnect

**Files:**
- Create: `bullforge/services/execution_service/trade_updates_stream.py`
- Create: `bullforge/tests/test_trade_updates_stream.py`

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/test_trade_updates_stream.py`:

```python
import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from services.execution_service.trade_updates_stream import (
    TradeUpdatesStream, normalize_trade_update,
)


def test_normalize_trade_update_matches_broker_snapshot_shape():
    event = MagicMock()
    event.order = MagicMock(
        id="alp-42",
        symbol="SPY",
        side=MagicMock(value="buy"),
        qty="2",
        order_type=MagicMock(value="market"),
        status=MagicMock(value="filled"),
        submitted_at=datetime(2026, 4, 14, tzinfo=timezone.utc),
        filled_qty="2",
        filled_avg_price="420.5",
        filled_at=datetime(2026, 4, 14, 14, 0, 1, tzinfo=timezone.utc),
    )
    snap = normalize_trade_update(event)
    assert snap["id"] == "alp-42"
    assert snap["status"] == "filled"
    assert snap["filled_qty"] == 2.0
    assert snap["filled_avg_price"] == 420.5
    assert snap["filled_at"].tzinfo == timezone.utc


@pytest.mark.asyncio
async def test_handler_forwards_to_core():
    core = AsyncMock()
    stream = TradeUpdatesStream(
        settings=MagicMock(
            alpaca_api_key="k", alpaca_secret_key="s",
            alpaca_base_url="https://paper-api.alpaca.markets",
        ),
        core=core,
        on_reconnect=AsyncMock(),
    )
    event = MagicMock()
    event.order = MagicMock(
        id="a", symbol="SPY", side=MagicMock(value="buy"), qty="1",
        order_type=MagicMock(value="market"),
        status=MagicMock(value="filled"),
        submitted_at=datetime.now(timezone.utc),
        filled_qty="1", filled_avg_price="100",
        filled_at=datetime.now(timezone.utc),
    )
    await stream._handle_event(event)
    core.apply_alpaca_order.assert_awaited_once()
    args, kwargs = core.apply_alpaca_order.await_args
    assert (kwargs.get("source") or args[1]) == "stream"


@pytest.mark.asyncio
async def test_handler_swallows_malformed_event():
    core = AsyncMock()
    stream = TradeUpdatesStream(
        settings=MagicMock(
            alpaca_api_key="k", alpaca_secret_key="s",
            alpaca_base_url="https://paper-api.alpaca.markets",
        ),
        core=core,
        on_reconnect=AsyncMock(),
    )
    bad = MagicMock()
    bad.order = None
    await stream._handle_event(bad)
    core.apply_alpaca_order.assert_not_called()


@pytest.mark.asyncio
async def test_reconnect_backoff_schedule():
    stream = TradeUpdatesStream(
        settings=MagicMock(
            alpaca_api_key="k", alpaca_secret_key="s",
            alpaca_base_url="https://paper-api.alpaca.markets",
        ),
        core=AsyncMock(),
        on_reconnect=AsyncMock(),
    )
    delays = [stream._backoff_delay(n) for n in range(7)]
    assert delays[0] == 1
    assert delays[1] == 2
    assert delays[2] == 4
    assert delays[-1] <= 30
    assert delays == sorted(delays)
```

- [ ] **Step 2: Run — expect FAIL**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_trade_updates_stream.py -v`

Expected: FAIL — module missing.

- [ ] **Step 3: Implement stream wrapper**

Create `bullforge/services/execution_service/trade_updates_stream.py`:

```python
from __future__ import annotations

import asyncio
import logging
from datetime import timezone
from typing import Awaitable, Callable, Optional

from alpaca.trading.stream import TradingStream

from shared.config import Settings
from services.execution_service.reconciler_core import ReconcilerCore

logger = logging.getLogger(__name__)


def normalize_trade_update(event) -> dict:
    o = event.order
    if o is None:
        raise ValueError("trade update event missing order")

    def _enum(v):
        return v.value if hasattr(v, "value") else str(v)

    filled_at = getattr(o, "filled_at", None)
    if filled_at is not None and filled_at.tzinfo is None:
        filled_at = filled_at.replace(tzinfo=timezone.utc)
    submitted_at = getattr(o, "submitted_at", None)
    if submitted_at is not None and submitted_at.tzinfo is None:
        submitted_at = submitted_at.replace(tzinfo=timezone.utc)

    return {
        "id": str(o.id),
        "symbol": o.symbol,
        "side": _enum(o.side),
        "qty": float(o.qty),
        "order_type": _enum(o.order_type),
        "status": _enum(o.status),
        "submitted_at": submitted_at,
        "filled_qty": float(o.filled_qty) if getattr(o, "filled_qty", None) else None,
        "filled_avg_price": (
            float(o.filled_avg_price)
            if getattr(o, "filled_avg_price", None) else None
        ),
        "filled_at": filled_at,
    }


class TradeUpdatesStream:
    def __init__(
        self,
        settings: Settings,
        core: ReconcilerCore,
        on_reconnect: Callable[[], Awaitable[None]],
        max_backoff_s: int = 30,
    ):
        self._settings = settings
        self._core = core
        self._on_reconnect = on_reconnect
        self._max_backoff_s = max_backoff_s
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    def _backoff_delay(self, attempt: int) -> int:
        return min(2 ** attempt, self._max_backoff_s)

    async def _handle_event(self, event) -> None:
        try:
            snap = normalize_trade_update(event)
        except Exception:
            logger.exception("malformed trade update event")
            return
        try:
            await self._core.apply_alpaca_order(snap, source="stream")
        except Exception:
            logger.exception("stream: apply_alpaca_order failed")

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        attempt = 0
        while not self._stop_event.is_set():
            client = TradingStream(
                api_key=self._settings.alpaca_api_key,
                secret_key=self._settings.alpaca_secret_key,
                paper=True,
            )
            client.subscribe_trade_updates(self._handle_event)
            try:
                if attempt > 0:
                    try:
                        await self._on_reconnect()
                    except Exception:
                        logger.exception("on_reconnect callback failed")
                attempt = 0
                await asyncio.to_thread(client.run)
            except Exception:
                logger.exception("trade updates stream dropped")
            if self._stop_event.is_set():
                return
            delay = self._backoff_delay(attempt)
            attempt += 1
            logger.info("reconnecting trade stream in %ds", delay)
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=delay,
                )
            except asyncio.TimeoutError:
                pass
```

- [ ] **Step 4: Run — expect PASS**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_trade_updates_stream.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/execution_service/trade_updates_stream.py bullforge/tests/test_trade_updates_stream.py
git commit -m "feat(reconciler): trade-updates stream with reconnect backoff"
```

---

## Task 11: Refactor `OrderExecutor` — delegate to `ReconcilerCore`

**Files:**
- Modify: `bullforge/services/execution_service/order_executor.py`
- Modify: `bullforge/tests/test_order_executor.py`

- [ ] **Step 1: Update tests to reflect new contract**

The new contract: after submitting, `OrderExecutor` fetches the initial Alpaca snapshot and delegates to `core.apply_alpaca_order(snap, source='submit')`. It does NOT poll for fills itself and does NOT insert trades directly. The returned `Order` reflects the post-submit snapshot status (usually still `submitted`/`accepted` — the stream or sweeper updates to `filled` asynchronously).

Rewrite `bullforge/tests/test_order_executor.py` to:

1. Remove assertions that depend on synchronous fill or trade insertion (e.g., `assert order.status == "filled"`, `mock_db.insert_trade.assert_called_once()`).
2. Replace with assertions that `core.apply_alpaca_order` was called with `source='submit'`.
3. Keep risk-manager, entry/exit gating, and "rejected signal doesn't submit" tests intact.

Example updated fixture and test:

```python
@pytest.fixture
def mock_core():
    from unittest.mock import AsyncMock
    core = AsyncMock()
    core.apply_alpaca_order = AsyncMock()
    return core


@pytest.fixture
def executor(mock_broker, mock_risk, mock_tracker, mock_db, mock_core):
    return OrderExecutor(
        broker=mock_broker,
        risk_manager=mock_risk,
        position_tracker=mock_tracker,
        db=mock_db,
        core=mock_core,
    )


@pytest.mark.asyncio
async def test_process_signal_submits_and_delegates_to_core(
    executor, mock_broker, mock_core,
):
    order = await executor.process_signal(_signal(), current_price=420.0)
    assert order is not None
    mock_broker.submit_market_order.assert_awaited_once()
    mock_core.apply_alpaca_order.assert_awaited_once()
    args, kwargs = mock_core.apply_alpaca_order.await_args
    assert (kwargs.get("source") or args[1]) == "submit"


@pytest.mark.asyncio
async def test_process_signal_rejected_by_risk_does_not_submit(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_risk.check_entry = AsyncMock(return_value=(False, "Too risky", 0))
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
    )
    result = await executor.process_signal(_signal(), current_price=420.0)
    assert result is None
    mock_broker.submit_market_order.assert_not_called()
    mock_core.apply_alpaca_order.assert_not_called()
```

Preserve any tests for: exit signals, `direction=0`, existing-position behavior. Update them so they assert `core.apply_alpaca_order` where they previously asserted DB inserts/updates.

- [ ] **Step 2: Run — expect FAIL**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_order_executor.py -v`

Expected: FAIL — `OrderExecutor.__init__` does not accept `core`.

- [ ] **Step 3: Refactor `OrderExecutor`**

Rewrite `bullforge/services/execution_service/order_executor.py`:

```python
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from shared.db import Database
from shared.models import Order, TradingSignal
from services.execution_service.alpaca_broker import AlpacaBroker
from services.execution_service.reconciler_core import ReconcilerCore
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
        core: ReconcilerCore,
    ):
        self._broker = broker
        self._risk = risk_manager
        self._tracker = position_tracker
        self._db = db
        self._core = core

    async def process_signal(
        self, signal: TradingSignal, current_price: float,
    ) -> Optional[Order]:
        await self._tracker.refresh()
        existing = self._tracker.get_position(signal.symbol)

        if existing:
            should_exit, _reason = self._risk.check_exit(signal, existing)
            if should_exit:
                return await self._execute_close(signal, existing)
            return None

        if signal.direction == 0:
            return None

        account = self._tracker.account
        positions = list(self._tracker.positions.values())
        approved, reason, qty = await self._risk.check_entry(
            signal, account, positions, current_price,
        )
        if not approved:
            logger.info("Entry rejected for %s: %s", signal.symbol, reason)
            return None

        return await self._execute_open(signal, qty)

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

        try:
            snap = await self._broker.get_order(alpaca_id)
            snap.setdefault("symbol", signal.symbol)
            snap.setdefault("side", side)
            snap.setdefault("qty", float(qty))
            snap.setdefault("order_type", "market")
            snap.setdefault("submitted_at", now)
            await self._core.apply_alpaca_order(snap, source="submit")
        except Exception:
            logger.exception(
                "submit-path reconciliation failed for %s; "
                "stream/sweeper will recover",
                alpaca_id,
            )

        existing = await self._db.get_order_by_alpaca_id(alpaca_id)
        return existing or order.__class__(**{**order.__dict__, "id": order_id})

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

        try:
            snap = await self._broker.get_order(alpaca_id)
            snap.setdefault("symbol", signal.symbol)
            snap.setdefault("side", side)
            snap.setdefault("qty", qty)
            snap.setdefault("order_type", "market")
            snap.setdefault("submitted_at", now)
            await self._core.apply_alpaca_order(snap, source="submit")
        except Exception:
            logger.exception(
                "close-path reconciliation failed for %s; "
                "stream/sweeper will recover",
                alpaca_id,
            )

        existing = await self._db.get_order_by_alpaca_id(alpaca_id)
        return existing or order.__class__(**{**order.__dict__, "id": order_id})
```

Note: `Order` is a frozen dataclass; `order.__dict__` + `__class__(**...)` rebuilds it with the returned `order_id`. If the existing project uses `dataclasses.replace`, prefer that instead.

- [ ] **Step 4: Run — expect PASS**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_order_executor.py -v`

Expected: PASS. All tests in the file pass with the new contract.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/execution_service/order_executor.py bullforge/tests/test_order_executor.py
git commit -m "refactor(executor): delegate fill bookkeeping to ReconcilerCore"
```

---

## Task 12: Wire everything into `main.py`

**Files:**
- Modify: `bullforge/services/execution_service/main.py`

- [ ] **Step 1: Inspect current `main.py`**

Open `bullforge/services/execution_service/main.py`. Locate the block that constructs `OrderExecutor` (around line 62). Locate the shutdown block.

- [ ] **Step 2: Update imports and wiring**

Add imports at the top:

```python
from services.execution_service.reconciler_core import ReconcilerCore
from services.execution_service.polling_sweeper import PollingSweeper
from services.execution_service.trade_updates_stream import TradeUpdatesStream
```

Replace the `OrderExecutor(...)` construction with:

```python
    core = ReconcilerCore(db=db, redis_client=redis_client)
    executor = OrderExecutor(
        broker=broker,
        risk_manager=risk_manager,
        position_tracker=position_tracker,
        db=db,
        core=core,
    )

    sweeper = PollingSweeper(
        db=db,
        broker=broker,
        core=core,
        interval_s=settings.execution_reconciler_poll_interval_s,
        orphan_every=settings.execution_reconciler_orphan_every,
        orphan_window_hours=settings.execution_reconciler_orphan_window_hours,
    )

    logger.info("Running startup reconciliation sweep...")
    await sweeper.sweep_now()
    await sweeper.start()
    logger.info("PollingSweeper started (interval=%ds)",
                settings.execution_reconciler_poll_interval_s)

    stream: TradeUpdatesStream | None = None
    if settings.execution_reconciler_stream_enabled:
        stream = TradeUpdatesStream(
            settings=settings, core=core,
            on_reconnect=sweeper.sweep_now,
        )
        await stream.start()
        logger.info("TradeUpdatesStream started")
    else:
        logger.info("TradeUpdatesStream disabled by config")
```

In the `finally` shutdown block, before `await redis_client.disconnect()`, add:

```python
        if stream is not None:
            await stream.stop()
        await sweeper.stop()
```

- [ ] **Step 3: Build and start the stack**

Run:

```bash
cd bullforge && bin/up execution_service
```

Expected: execution_service starts cleanly; logs show "Running startup reconciliation sweep", "PollingSweeper started", and (if enabled) "TradeUpdatesStream started".

- [ ] **Step 4: Verify the reported bug is resolved**

Check the 6 previously-stuck orders:

```bash
docker compose exec -T timescaledb psql -U postgres -d bullforge \
  -c "SELECT alpaca_order_id, status, reconcile_source, last_reconciled_at FROM orders WHERE alpaca_order_id IS NOT NULL ORDER BY submitted_at DESC LIMIT 20;"
```

Expected: the 6 orders that were previously `submitted` now show `status='cancelled'`, `reconcile_source='startup'` or `'poll'`, and a recent `last_reconciled_at`.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/execution_service/main.py
git commit -m "feat(execution): start reconciler sweeper and stream on boot"
```

---

## Task 13: Integration test — late cancellation is reconciled

**Files:**
- Create: `bullforge/tests/test_order_reconciliation_integration.py`

- [ ] **Step 1: Write the integration test**

Create `bullforge/tests/test_order_reconciliation_integration.py`:

```python
import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from shared.db import Database
from shared.models import Order
from shared.redis import RedisClient
from shared.config import Settings
from services.execution_service.reconciler_core import ReconcilerCore
from services.execution_service.polling_sweeper import PollingSweeper


class FakeBroker:
    def __init__(self):
        self._state = {}  # alpaca_id -> snapshot dict

    def set_status(self, alpaca_id: str, status: str, **fields):
        snap = self._state.get(alpaca_id, {
            "id": alpaca_id, "symbol": "SPY", "side": "buy", "qty": 1.0,
            "order_type": "market",
            "submitted_at": datetime.now(timezone.utc),
        })
        snap["status"] = status
        snap.update(fields)
        self._state[alpaca_id] = snap

    async def get_order(self, alpaca_id: str) -> dict:
        return dict(self._state[alpaca_id])

    async def list_open_orders(self, after):
        return [
            dict(s) for s in self._state.values()
            if s["status"] not in ("filled", "cancelled", "rejected", "expired")
        ]


@pytest.mark.asyncio
async def test_late_cancellation_is_reconciled(db):
    """Regression: an order left in 'submitted' state gets corrected to
    'cancelled' after the sweeper runs."""
    alpaca_id = "alp-late-cancel"
    stale = Order(
        symbol="SPY", side="buy", quantity=1.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
        alpaca_order_id=alpaca_id, status="submitted",
    )
    await db.insert_order(stale)

    broker = FakeBroker()
    broker.set_status(alpaca_id, "cancelled")

    redis_mock = AsyncMock()
    core = ReconcilerCore(db=db, redis_client=redis_mock)
    sweeper = PollingSweeper(
        db=db, broker=broker, core=core,
        interval_s=60, orphan_every=5, orphan_window_hours=48,
    )
    await sweeper.sweep_now()

    reconciled = await db.get_order_by_alpaca_id(alpaca_id)
    assert reconciled.status == "cancelled"
    assert reconciled.reconcile_source == "poll"
    assert reconciled.last_reconciled_at is not None


@pytest.mark.asyncio
async def test_orphan_external_order_is_ingested(db):
    alpaca_id = "alp-external"
    broker = FakeBroker()
    broker.set_status(alpaca_id, "new")

    redis_mock = AsyncMock()
    core = ReconcilerCore(db=db, redis_client=redis_mock)
    sweeper = PollingSweeper(
        db=db, broker=broker, core=core,
        interval_s=60, orphan_every=1, orphan_window_hours=48,
    )
    await sweeper.sweep_now()

    ingested = await db.get_order_by_alpaca_id(alpaca_id)
    assert ingested is not None
    assert ingested.origin == "external"
    assert ingested.status == "new" or ingested.status == "submitted"
```

(The `db` fixture is the one already in use across `test_order_db.py`. Alpaca's `'new'` status maps to BullForge `'submitted'` via `_ALPACA_STATUS_MAP` added in Task 4.)

- [ ] **Step 2: Run — expect PASS**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_order_reconciliation_integration.py -v`

Expected: PASS.

- [ ] **Step 3: Run the full suite**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/ -x`

Expected: everything passes.

- [ ] **Step 4: Commit**

```bash
git add bullforge/tests/test_order_reconciliation_integration.py
git commit -m "test(reconciler): late-cancel + orphan-ingest integration tests"
```

---

## Task 14: Observability — reconciler counters (optional, can defer)

**Files:**
- Modify: `bullforge/services/execution_service/reconciler_core.py`

- [ ] **Step 1: Add counter publishes**

If the monitoring service already exposes a Redis channel for counters (check `services/monitoring_service/` for the channel name before implementing), emit increments at the end of `apply_alpaca_order`:

```python
    async def _emit_counter(self, name: str, labels: dict) -> None:
        if self._redis is None:
            return
        await self._redis.publish(
            "monitoring:counters",
            json.dumps({"name": name, "labels": labels, "value": 1}),
        )
```

Call sites:
- Status transition: `await self._emit_counter("reconciler_events_total", {"source": source, "transition": f"{old_status}->{new_status}"})`
- Orphan ingestion (inside the `inserted=True` branch path): `await self._emit_counter("reconciler_orphans_ingested_total", {"source": source})`
- Anomaly: `await self._emit_counter("reconciler_anomalies_total", {})`

If no existing counter channel, skip this task — log-based observability from Task 7's `logger.info` is sufficient for initial rollout.

- [ ] **Step 2: Commit (if implemented)**

```bash
git add bullforge/services/execution_service/reconciler_core.py
git commit -m "feat(reconciler): emit counter metrics for events/orphans/anomalies"
```

---

## Final verification

- [ ] **Step 1: Full test suite**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/ -v`

Expected: 100% pass.

- [ ] **Step 2: Service runs clean for 5 minutes**

Start the stack, tail `execution_service` logs, verify:
- Startup sweep logs once.
- If stream enabled, trade stream connects.
- Every 60s, the sweep runs without exceptions.
- Every 5 minutes, an orphan scan runs.

```bash
docker compose logs -f --tail=100 execution_service
```

- [ ] **Step 3: Inspect orders table**

```bash
docker compose exec -T timescaledb psql -U postgres -d bullforge \
  -c "SELECT status, origin, reconcile_source, COUNT(*) FROM orders GROUP BY 1,2,3 ORDER BY 1;"
```

Expected: no lingering `submitted` rows with `alpaca_order_id` set and old `submitted_at` (> 5 minutes ago).
