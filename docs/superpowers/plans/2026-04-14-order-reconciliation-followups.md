# Order Reconciliation Follow-Ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the six post-review follow-ups from the order-reconciliation feature: add a real-Postgres integration test, make `OrderExecutor`'s submit path race-safe, add a DB-level CHECK on `orders.status`, eliminate the post-commit re-read in `upsert_order_from_alpaca`, wire reconciler observability counters, and document the live-stack verification step.

**Architecture:** Work continues on the existing `feature/order-reconciliation` branch in `.worktrees/order-reconciliation`. The real-DB test uses the existing `docker-compose.test.yml` stack (`timescaledb-test` + `test-runner`) — no new infrastructure. Observability is additive: the reconciler publishes counter-increment events to a new `monitoring:reconciler` Redis channel; no monitoring-service consumer is in scope (documented as a follow-up hook).

**Tech Stack:** Python 3.12, asyncpg, pytest-asyncio, Docker Compose, TimescaleDB/Postgres 16, Redis.

---

## File Structure

**New files:**
- `bullforge/tests/integration/__init__.py` — empty package marker
- `bullforge/tests/integration/conftest.py` — session-scoped `real_db` fixture that connects to `timescaledb-test`, runs migrations 001-010, and truncates `orders`/`trades` between tests
- `bullforge/tests/integration/test_order_reconciliation_real_db.py` — regression tests exercising real SQL (UPSERT ON CONFLICT, FOR UPDATE, partial unique index, CHECK constraint)
- `bullforge/migrations/011_orders_status_check.sql` — adds `CHECK (status IN (...))` to `orders.status`

**Modified files:**
- `bullforge/shared/db.py` — `upsert_order_from_alpaca` uses `RETURNING` to eliminate the post-commit `get_order_by_alpaca_id` round-trip; submit-path insert gets `ON CONFLICT (alpaca_order_id) DO NOTHING` variant exposed as `insert_order_if_new`
- `bullforge/services/execution_service/order_executor.py` — swap `db.insert_order` for `db.insert_order_if_new` to make submit/stream race idempotent
- `bullforge/services/execution_service/reconciler_core.py` — publish counter events on every apply (events, orphans, anomalies) and wire a `record_stream_reconnect()` hook
- `bullforge/services/execution_service/trade_updates_stream.py` — call `core.record_stream_reconnect()` on each successful reconnect
- `bullforge/tests/test_order_db.py` — update upsert tests for the `RETURNING`-based shape; add tests for `insert_order_if_new`
- `bullforge/tests/test_reconciler_core.py` — assert counter events are published
- `bullforge/Dockerfile.test` — no change expected; verify it already installs `asyncpg` (it does via execution-service requirements)
- `bullforge/bin/test` — no change; the integration tests live under `tests/` so existing `bin/test` picks them up

---

## Task 1: Real-DB integration test scaffolding

**Files:**
- Create: `bullforge/tests/integration/__init__.py`
- Create: `bullforge/tests/integration/conftest.py`
- Test: covered by Task 2

- [ ] **Step 1: Create empty package marker**

```python
# bullforge/tests/integration/__init__.py
```

- [ ] **Step 2: Write the session-scoped `real_db` fixture**

```python
# bullforge/tests/integration/conftest.py
from __future__ import annotations

import os
from pathlib import Path

import asyncpg
import pytest_asyncio

from shared.config import Settings
from shared.db import Database

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"


@pytest_asyncio.fixture(scope="session")
async def _raw_conn():
    settings = Settings()
    conn = await asyncpg.connect(
        host=settings.timescaledb_host,
        port=settings.timescaledb_port,
        user=settings.timescaledb_user,
        password=settings.timescaledb_password,
        database=settings.timescaledb_db,
    )
    try:
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            await conn.execute(path.read_text())
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def real_db(_raw_conn):
    await _raw_conn.execute("TRUNCATE trades, orders RESTART IDENTITY CASCADE")
    settings = Settings()
    db = Database(settings)
    await db.connect()
    try:
        yield db
    finally:
        await db.disconnect()
```

- [ ] **Step 3: Run the collection step to confirm fixture parses**

Run: `docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm test-runner pytest tests/integration/ --collect-only -q`
Expected: `no tests ran` (no test files yet) with zero collection errors.

- [ ] **Step 4: Commit**

```bash
git add bullforge/tests/integration/__init__.py bullforge/tests/integration/conftest.py
git commit -m "test(reconciler): add real-db integration fixture"
```

---

## Task 2: Real-DB regression tests for reconciler

**Files:**
- Create: `bullforge/tests/integration/test_order_reconciliation_real_db.py`

- [ ] **Step 1: Write the failing tests**

```python
# bullforge/tests/integration/test_order_reconciliation_real_db.py
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from shared.models import Order


pytestmark = pytest.mark.asyncio


def _snapshot(**overrides):
    base = {
        "id": "alp-1",
        "symbol": "AAPL",
        "side": "buy",
        "qty": 10.0,
        "order_type": "market",
        "status": "new",
        "submitted_at": datetime.now(timezone.utc),
        "filled_at": None,
        "filled_avg_price": None,
        "filled_qty": None,
    }
    base.update(overrides)
    return base


async def _insert_submitted(db, alpaca_id="alp-1"):
    order = Order(
        symbol="AAPL", side="buy", quantity=10.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.7,
        alpaca_order_id=alpaca_id, status="submitted",
    )
    return await db.insert_order(order)


async def test_upsert_transitions_submitted_to_cancelled(real_db):
    await _insert_submitted(real_db)
    result = await real_db.upsert_order_from_alpaca(
        _snapshot(status="canceled"), source="poll",
    )
    assert result.old_status == "submitted"
    assert result.new_status == "cancelled"
    assert result.order.reconcile_source == "poll"
    assert result.order.last_reconciled_at is not None


async def test_upsert_is_idempotent_under_duplicate_terminal(real_db):
    await _insert_submitted(real_db)
    await real_db.upsert_order_from_alpaca(_snapshot(status="filled",
        filled_avg_price=150.0, filled_qty=10.0), source="stream")
    second = await real_db.upsert_order_from_alpaca(_snapshot(status="filled",
        filled_avg_price=150.0, filled_qty=10.0), source="poll")
    assert second.old_status == "filled"
    assert second.new_status == "filled"


async def test_orphan_order_ingested_as_external(real_db):
    result = await real_db.upsert_order_from_alpaca(
        _snapshot(id="alp-orphan", status="new"), source="poll",
    )
    assert result.inserted is True
    assert result.order.origin == "external"


async def test_unique_partial_index_rejects_duplicate_alpaca_id(real_db):
    await _insert_submitted(real_db, alpaca_id="alp-unique")
    with pytest.raises(Exception):
        await _insert_submitted(real_db, alpaca_id="alp-unique")


async def test_non_terminal_query_excludes_terminal(real_db):
    await _insert_submitted(real_db, alpaca_id="alp-open")
    await real_db.upsert_order_from_alpaca(
        _snapshot(id="alp-open", status="filled",
                  filled_avg_price=150.0, filled_qty=10.0),
        source="stream",
    )
    rows = await real_db.query_non_terminal_orders()
    assert all(r.status not in ("filled","cancelled","rejected","expired")
               for r in rows)
```

- [ ] **Step 2: Run and verify they pass**

Run: `docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm test-runner pytest tests/integration/test_order_reconciliation_real_db.py -v`
Expected: all 5 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add bullforge/tests/integration/test_order_reconciliation_real_db.py
git commit -m "test(reconciler): real-db regression tests"
```

---

## Task 3: `RETURNING`-based upsert (remove post-commit re-read)

**Files:**
- Modify: `bullforge/shared/db.py` — `upsert_order_from_alpaca`
- Modify: `bullforge/tests/test_order_db.py`

- [ ] **Step 1: Update the failing unit test to assert no extra fetch**

Open `bullforge/tests/test_order_db.py` and locate the `upsert_order_from_alpaca` tests. Update the existing test case that mocks both the txn path and the post-commit `get_order_by_alpaca_id` — remove the stub for `get_order_by_alpaca_id` and assert it is NOT awaited. Example assertion to add:

```python
mock_conn.fetchrow.assert_awaited()           # SELECT FOR UPDATE
# new: single RETURNING row from INSERT/UPDATE replaces the post-commit read
assert not any(
    call.args and "FROM orders" in call.args[0]
    and "alpaca_order_id" in call.args[0]
    and "SELECT" in call.args[0].upper()
    and "FOR UPDATE" not in call.args[0]
    for call in mock_conn.fetchrow.await_args_list
)
```

- [ ] **Step 2: Run the tests — expect failure**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_order_db.py -v -k upsert`
Expected: FAIL (current code still calls `get_order_by_alpaca_id`).

- [ ] **Step 3: Rewrite `upsert_order_from_alpaca` to use `RETURNING`**

Replace the method body so both INSERT and UPDATE branches return the full row via `RETURNING *`, then hydrate an `Order` from that row directly. Remove both `order = await self.get_order_by_alpaca_id(alpaca_id)` lines. Re-use the existing row-to-Order helper (or inline the construction — match however other query methods in `db.py` build an `Order`). The transaction now owns the read *and* the write, eliminating the cross-connection race.

- [ ] **Step 4: Run unit and integration tests**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_order_db.py -v`
Expected: PASS.
Run: `docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm test-runner pytest tests/integration/test_order_reconciliation_real_db.py -v`
Expected: PASS (Task 2 tests still green against the new SQL).

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/db.py bullforge/tests/test_order_db.py
git commit -m "perf(db): upsert returns row directly, drop post-commit read"
```

---

## Task 4: `insert_order_if_new` for submit/stream race

**Files:**
- Modify: `bullforge/shared/db.py`
- Modify: `bullforge/services/execution_service/order_executor.py`
- Modify: `bullforge/tests/test_order_db.py`
- Modify: `bullforge/tests/test_order_executor.py`

The race: `OrderExecutor._execute_open` calls `broker.submit_market_order` then `db.insert_order`. If the trade-updates stream wins the race and already upserted the row via `alpaca_order_id`, the plain INSERT violates the unique partial index. Fix by making the submit-path insert a no-op on conflict.

- [ ] **Step 1: Write the failing unit test for `insert_order_if_new`**

In `bullforge/tests/test_order_db.py`, add a test that calls `insert_order_if_new` twice with the same `alpaca_order_id` and asserts the second call returns `None` (or the existing row's id — pick one and document). Run and expect failure because the method doesn't exist.

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_order_db.py -v -k insert_order_if_new`
Expected: FAIL.

- [ ] **Step 2: Implement `insert_order_if_new` in `shared/db.py`**

```python
async def insert_order_if_new(self, order: Order) -> Optional[int]:
    """INSERT that does nothing on alpaca_order_id conflict. Returns
    the inserted row id, or None if a row already existed."""
    async with self.pool.acquire() as conn:
        return await conn.fetchval(
            """
            INSERT INTO orders
              (symbol, side, quantity, order_type, submitted_at,
               signal_strength, alpaca_order_id, status,
               origin, last_reconciled_at, reconcile_source)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,
                    COALESCE($9,'internal'),$10,$11)
            ON CONFLICT (alpaca_order_id) DO NOTHING
            RETURNING id
            """,
            order.symbol, order.side, order.quantity, order.order_type,
            order.submitted_at, order.signal_strength,
            order.alpaca_order_id, order.status,
            order.origin, order.last_reconciled_at, order.reconcile_source,
        )
```

- [ ] **Step 3: Run unit test — expect PASS**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_order_db.py -v -k insert_order_if_new`
Expected: PASS.

- [ ] **Step 4: Swap `OrderExecutor` submit path**

In `bullforge/services/execution_service/order_executor.py`, replace both `await self._db.insert_order(order)` calls in `_execute_open` and `_execute_close` with `await self._db.insert_order_if_new(order)`. If the return value is `None`, fetch the existing row via `await self._db.get_order_by_alpaca_id(order.alpaca_order_id)` and use its id for the subsequent `core.apply_alpaca_order` path. Update the downstream `dataclasses.replace(order, id=order_id)` fallback accordingly — if the row already existed, prefer the fetched row over `replace`.

- [ ] **Step 5: Update executor tests**

In `bullforge/tests/test_order_executor.py`, update any mock that stubs `db.insert_order` → `db.insert_order_if_new`. Add one test where `insert_order_if_new` returns `None` (stream won the race) and assert `get_order_by_alpaca_id` is used and `core.apply_alpaca_order` still receives the snapshot exactly once.

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_order_executor.py -v`
Expected: PASS.

- [ ] **Step 6: Add a real-DB test for the race**

Append to `bullforge/tests/integration/test_order_reconciliation_real_db.py`:

```python
async def test_submit_path_is_idempotent_with_stream_winner(real_db):
    # Stream already upserted the row
    await real_db.upsert_order_from_alpaca(
        _snapshot(id="alp-race", status="new"), source="stream",
    )
    # Submit path runs after — must not raise
    order = Order(
        symbol="AAPL", side="buy", quantity=10.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.7,
        alpaca_order_id="alp-race", status="submitted",
    )
    result = await real_db.insert_order_if_new(order)
    assert result is None
    existing = await real_db.get_order_by_alpaca_id("alp-race")
    assert existing.origin == "external"
```

Run: `docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm test-runner pytest tests/integration/test_order_reconciliation_real_db.py -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add bullforge/shared/db.py bullforge/services/execution_service/order_executor.py \
        bullforge/tests/test_order_db.py bullforge/tests/test_order_executor.py \
        bullforge/tests/integration/test_order_reconciliation_real_db.py
git commit -m "fix(executor): idempotent submit insert via ON CONFLICT DO NOTHING"
```

---

## Task 5: `CHECK (status IN (...))` constraint

**Files:**
- Create: `bullforge/migrations/011_orders_status_check.sql`
- Modify: `bullforge/tests/integration/conftest.py` — no change (runs all `*.sql` in order)
- Test: addition to `test_order_reconciliation_real_db.py`

- [ ] **Step 1: Write the migration**

```sql
-- bullforge/migrations/011_orders_status_check.sql
ALTER TABLE orders
  ADD CONSTRAINT orders_status_check
  CHECK (status IN (
    'pending','submitted','filled','partially_filled',
    'cancelled','rejected','expired'
  ));
```

- [ ] **Step 2: Add a failing test**

Append to `bullforge/tests/integration/test_order_reconciliation_real_db.py`:

```python
async def test_status_check_rejects_garbage(real_db):
    await _insert_submitted(real_db, alpaca_id="alp-check")
    async with real_db.pool.acquire() as conn:
        with pytest.raises(Exception):
            await conn.execute(
                "UPDATE orders SET status='not_a_status' "
                "WHERE alpaca_order_id=$1",
                "alp-check",
            )
```

- [ ] **Step 3: Rebuild test image and run**

Run: `docker compose -f docker-compose.yml -f docker-compose.test.yml build test-runner && \
      docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm test-runner pytest tests/integration/ -v`
Expected: new test PASSES; all prior tests still PASS.

- [ ] **Step 4: Commit**

```bash
git add bullforge/migrations/011_orders_status_check.sql \
        bullforge/tests/integration/test_order_reconciliation_real_db.py
git commit -m "feat(db): add CHECK constraint on orders.status"
```

---

## Task 6: Reconciler observability counters

**Files:**
- Modify: `bullforge/services/execution_service/reconciler_core.py`
- Modify: `bullforge/services/execution_service/trade_updates_stream.py`
- Modify: `bullforge/services/execution_service/polling_sweeper.py`
- Modify: `bullforge/tests/test_reconciler_core.py`

Publish JSON counter events to Redis channel `monitoring:reconciler`. Shape mirrors the spec's intent so a future monitoring-service consumer can bucket by label without schema changes:

```json
{"counter": "reconciler_events_total",
 "labels": {"source": "poll", "transition": "submitted->cancelled"}, "value": 1}
```

- [ ] **Step 1: Add a failing test**

In `bullforge/tests/test_reconciler_core.py`, add a test asserting that calling `apply_alpaca_order` on a status change publishes both the existing `execution:orders` event AND a `monitoring:reconciler` counter event with the right labels. Run and expect failure.

- [ ] **Step 2: Add a `_publish_counter` helper and call sites in `reconciler_core.py`**

```python
async def _publish_counter(self, name: str, **labels) -> None:
    await self._redis.publish("monitoring:reconciler", json.dumps({
        "counter": name, "labels": labels, "value": 1,
    }))
```

Call sites:
- On every `apply_alpaca_order` that changes status: `reconciler_events_total` with `{source, transition}` where transition is `f"{old}->{new}"` (use `"none->{new}"` on insert).
- On orphan ingest (`inserted=True` with `source="poll"`): `reconciler_orphans_ingested_total` with `{}`.
- On `filled → non-filled-terminal`: `reconciler_anomalies_total` with `{kind: "filled_reverted"}`.

Add a public `record_stream_reconnect()` method that publishes `reconciler_stream_reconnects_total`.

- [ ] **Step 3: Wire the stream hook**

In `bullforge/services/execution_service/trade_updates_stream.py`, after a successful reconnect (just before invoking `on_reconnect`), call `await self._core.record_stream_reconnect()`. Pass `core` into the stream via constructor if not already wired.

- [ ] **Step 4: Run unit tests**

Run: `docker compose run --rm -T execution_service pytest bullforge/tests/test_reconciler_core.py bullforge/tests/test_trade_updates_stream.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/execution_service/reconciler_core.py \
        bullforge/services/execution_service/trade_updates_stream.py \
        bullforge/tests/test_reconciler_core.py
git commit -m "feat(reconciler): publish counter events to monitoring:reconciler"
```

---

## Task 7: Live-stack verification runbook (docs only)

**Files:**
- Create: `bullforge/docs/runbooks/order-reconciliation-verification.md`

- [ ] **Step 1: Write the runbook**

```markdown
# Order Reconciliation — Live Verification

## Deploy
1. `cd bullforge && bin/up execution_service` (picks up migrations 010 and 011).
2. Tail logs: `docker compose logs -f execution_service | grep -i reconcil`.

## Confirm the six known-stuck orders reconcile
Within one `EXECUTION_RECONCILER_POLL_INTERVAL_S` cycle (default 60s) after boot:

```sql
SELECT alpaca_order_id, status, reconcile_source, last_reconciled_at
FROM orders
WHERE status NOT IN ('filled','cancelled','rejected','expired')
ORDER BY submitted_at DESC;
```

Expected: the previously-stuck rows now show `status='cancelled'`,
`reconcile_source='startup'` or `'poll'`, and a recent
`last_reconciled_at`.

## Watch counters
`redis-cli SUBSCRIBE monitoring:reconciler` — expect
`reconciler_events_total` on every status transition and
`reconciler_stream_reconnects_total=0` during steady state.

## Rollback escape hatch
Set `EXECUTION_RECONCILER_STREAM_ENABLED=false` if the Alpaca stream
is flapping; the polling sweeper alone still closes the gap.
```

- [ ] **Step 2: Commit**

```bash
git add bullforge/docs/runbooks/order-reconciliation-verification.md
git commit -m "docs(reconciler): live-stack verification runbook"
```

---

## Final Validation

- [ ] **Run full test suite**

Run: `docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm test-runner pytest tests/ -v`
Expected: all tests pass. Note pre-existing torch-dependent failures (not introduced here) — baseline count should match pre-Task-1 baseline.

- [ ] **Skim `git log` on `feature/order-reconciliation`**

Confirm seven new commits land in order: fixture → real-db tests → RETURNING → idempotent submit → CHECK → counters → runbook. Any out-of-order commit is a plan failure, not a squash opportunity.

---

## Self-Review Checklist (for the human driving the plan)

1. **Coverage:** Tasks 1-6 correspond to follow-ups 1-5 plus the user-verification runbook (follow-up 6).
2. **Placeholders:** none — every step has concrete code or a concrete command.
3. **Type consistency:** `insert_order_if_new` returns `Optional[int]` everywhere it's mentioned; `UpsertResult` shape unchanged; `record_stream_reconnect()` signature identical at definition and call site.
4. **Scope:** one spec-level concern (reconciler hardening) broken into bite-sized tasks, each independently committable and reviewable.
