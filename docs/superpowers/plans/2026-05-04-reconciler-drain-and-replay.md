# Reconciler drain semantics + Alpaca replay CLI — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `ReconcilerCore._handle_fill` to drain N open trade rows when one fill aggregates them (so a manual broker-UI position-close cleans up all the underlying per-row trades), and add a host-side `reconcile_from_alpaca.py` CLI that backfills closed Alpaca orders into the reconciler so already-stale rows can be flushed.

**Architecture:** Three surgical code changes plus one new script. (1) `shared/db.py` gets a plural `get_open_trades(symbol)` returning all open rows for a symbol FIFO-ordered. (2) `services/execution_service/reconciler_core.py::_handle_fill` is retargeted at the plural helper and grows three branches — exact-match drains all rows, over-close drains all + emits an anomaly, under-fill closes the FIFO oldest fitting subset + emits a residual anomaly when applicable. (3) `services/execution_service/alpaca_broker.py` factors `_sync_list_open_orders` through a generic `_sync_list_orders(status, after, limit)` and adds an async `list_closed_orders` wrapper. (4) New `bullforge/scripts/reconcile_from_alpaca.py` mirrors `scripts/backtest.py` wiring (live `Database`, `RedisClient`, `AlpacaBroker`), filters Alpaca's closed orders to a target symbol set, and dispatches each through `ReconcilerCore.apply_alpaca_order(snap, source="manual_replay")`. The reconciler's existing `upsert_order_from_alpaca` keying on `alpaca_order_id` makes the script idempotent for free.

**Tech Stack:** Python 3.12, asyncio, asyncpg (mocked via `MagicMock` + `AsyncMock`), pytest with `asyncio_mode = auto`, `unittest.mock.AsyncMock`, alpaca-py `TradingClient` / `GetOrdersRequest` / `QueryOrderStatus`. Working directory throughout: `bullforge/` (under the `.worktrees/reconciler-drain/` worktree, branch `feature/reconciler-drain`).

**Spec:** [`docs/superpowers/specs/2026-05-04-reconciler-drain-and-replay-design.md`](../specs/2026-05-04-reconciler-drain-and-replay-design.md)

---

## File structure

| File | Action | Responsibility |
|------|--------|----------------|
| `bullforge/shared/db.py` | Modify (~30 lines added next to `get_open_trade` at line 1518) | New `get_open_trades(symbol)` returning all open rows FIFO-ordered. |
| `bullforge/services/execution_service/reconciler_core.py` | Modify (`_handle_fill`, lines 87–133) | Retarget to plural helper; add drain branches for exact / over / under fills. |
| `bullforge/services/execution_service/alpaca_broker.py` | Modify (lines 50–51, 104–111; ~25 lines net) | Generalize `_sync_list_orders(status, ...)`; add `async list_closed_orders`. |
| `bullforge/scripts/reconcile_from_alpaca.py` | **Create** (~120 lines) | Host-side CLI: arg parse → fetch closed orders per symbol → dispatch to `apply_alpaca_order` → print summary. |
| `bullforge/tests/test_monitoring_db.py` | Modify (append 1 test) | Cover `get_open_trades` happy path + empty path. |
| `bullforge/tests/test_reconciler_core.py` | Modify (mechanical mock swap on ~5 existing test bodies; append 6 new tests) | Single-row regression after API swap; full-drain, over-close, partial-FIFO, partial-exact, symmetric short, single-row drain regression guard. |
| `bullforge/tests/test_order_reconciliation_integration.py` | Modify (~6 lines added next to `FakeDB.get_open_trade` at line 119) | Add `get_open_trades` sibling so the integration fake satisfies the new code path. |
| `bullforge/tests/test_reconcile_from_alpaca.py` | **Create** (~140 lines) | Unit tests for the CLI: dispatch, dry-run, symbol filtering, default lookback, summary counts. |
| `bullforge/docs/architecture/redis-pubsub.md` | Modify (append a 3-line note to the `monitoring:reconciler` row) | List the three `reconciler_anomalies_total{kind=}` values (`filled_reverted`, `over_close`, `partial_drain_residual`). |

No new files outside the table. No schema changes. No new pub/sub channels. No `RuntimeConfig` schema changes.

---

## Conventions for every commit in this plan

- **Conventional Commits** (see `git log --oneline` on master for style — `fix(...)`, `test(...)`, `feat(...)`, `refactor(...)`, `docs(...)`).
- **No `Co-Authored-By` trailer** (per repo convention; CLAUDE.md / NEXT_PROMPT.md).
- TDD pattern matches recent commits like `e3d09c0 test(risk_manager): cover crypto long-only entry guard (failing)` followed by `51dcb0d fix(risk_manager): reject short signals for crypto symbols` — failing test gets its own commit, then the implementation commit.
- All datetimes UTC (`datetime.now(timezone.utc)`); naive datetimes will fail `Bar`/`FeatureRow`/`Label` invariants and may fail `_require_utc(...)` calls in `shared/db.py`.

---

## Task 1: Verify baseline (sanity check before any change)

**Files:** None (environment only).

- [ ] **Step 1: Confirm you're in the right worktree on the right branch**

```bash
pwd
git rev-parse --abbrev-ref HEAD
git status
```

Expected: `pwd` ends in `.worktrees/reconciler-drain` (or its `bullforge/` subdir); branch is `feature/reconciler-drain`; working tree clean.

- [ ] **Step 2: Run the targeted baseline tests**

From the worktree's `bullforge/` subdirectory:

```bash
cd bullforge
bin/test python -m pytest tests/test_reconciler_core.py tests/test_monitoring_db.py tests/test_order_reconciliation_integration.py -v
```

Expected: all green. The reconciler-core file alone has 17 tests at baseline.

- [ ] **Step 3: Confirm the alpaca-py enum surface this plan depends on**

```bash
bin/test python -c "from alpaca.trading.enums import QueryOrderStatus; print([s.value for s in QueryOrderStatus])"
```

Expected output contains `'open'` and `'closed'`. If it doesn't, the spec assumption about the alpaca-py API is wrong — STOP and surface the issue rather than guessing.

---

## Task 2: Failing tests for `Database.get_open_trades`

**Files:**
- Test: `bullforge/tests/test_monitoring_db.py` (append two tests)

The new helper returns all open rows for a symbol ordered FIFO (`entry_time ASC`), and an empty list when none exist. We mock `conn.fetch` (plural — `get_open_trades` uses `fetch`, not `fetchrow`).

- [ ] **Step 1: Append the two tests**

At the end of `bullforge/tests/test_monitoring_db.py`, append:

```python
@pytest.mark.asyncio
async def test_get_open_trades_returns_all_open_rows_fifo(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    rows = [
        {
            "id": 1, "symbol": "BTC/USD", "side": "long",
            "entry_time": datetime(2026, 5, 5, 0, 57, tzinfo=timezone.utc),
            "entry_price": 60000.0, "quantity": 0.001, "entry_order_id": 10,
            "exit_time": None, "exit_price": None, "pnl": None,
            "hold_duration_seconds": None, "exit_order_id": None,
        },
        {
            "id": 2, "symbol": "BTC/USD", "side": "long",
            "entry_time": datetime(2026, 5, 5, 0, 58, tzinfo=timezone.utc),
            "entry_price": 60050.0, "quantity": 0.001, "entry_order_id": 11,
            "exit_time": None, "exit_price": None, "pnl": None,
            "hold_duration_seconds": None, "exit_order_id": None,
        },
    ]
    conn.fetch = AsyncMock(return_value=rows)
    db = Database(settings)
    db.pool = pool

    trades = await db.get_open_trades("BTC/USD")

    assert len(trades) == 2
    assert all(isinstance(t, Trade) for t in trades)
    assert [t.id for t in trades] == [1, 2]  # FIFO oldest-first
    assert all(t.exit_time is None for t in trades)
    # Verify the SQL ordered ASC, not DESC (this is the FIFO invariant the
    # drain logic relies on).
    sql = conn.fetch.call_args[0][0]
    assert "ORDER BY entry_time ASC" in sql
    assert "BTC/USD" == conn.fetch.call_args[0][1]


@pytest.mark.asyncio
async def test_get_open_trades_empty_when_none_open(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[])
    db = Database(settings)
    db.pool = pool

    trades = await db.get_open_trades("BTC/USD")

    assert trades == []
```

- [ ] **Step 2: Run the new tests and confirm they fail with `AttributeError: ... 'get_open_trades'`**

```bash
bin/test python -m pytest tests/test_monitoring_db.py::test_get_open_trades_returns_all_open_rows_fifo tests/test_monitoring_db.py::test_get_open_trades_empty_when_none_open -v
```

Expected: both fail. The failure mode is `AttributeError: 'Database' object has no attribute 'get_open_trades'`.

- [ ] **Step 3: Commit the failing test**

```bash
git add bullforge/tests/test_monitoring_db.py
git commit -m "test(db): cover get_open_trades plural helper (failing)"
```

---

## Task 3: Implement `Database.get_open_trades`

**Files:**
- Modify: `bullforge/shared/db.py` (insert a method immediately after `get_open_trade` at line 1547)

- [ ] **Step 1: Add the method**

Open `bullforge/shared/db.py`. Find the closing `)` of `get_open_trade` at line 1547. Immediately after it (and before the `async def query_closed_trades(` block at line 1549), insert:

```python
    async def get_open_trades(self, symbol: str) -> list[Trade]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, symbol, side, entry_time, exit_time, entry_price,
                       exit_price, quantity, pnl, hold_duration_seconds,
                       entry_order_id, exit_order_id
                FROM trades
                WHERE symbol = $1 AND exit_time IS NULL
                ORDER BY entry_time ASC
                """,
                symbol,
            )
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
```

- [ ] **Step 2: Run the Task 2 tests and confirm they pass**

```bash
bin/test python -m pytest tests/test_monitoring_db.py -v
```

Expected: all `test_monitoring_db.py` tests pass — both the two new ones and the pre-existing tests for `get_open_trade`, `insert_trade`, etc.

- [ ] **Step 3: Commit**

```bash
git add bullforge/shared/db.py
git commit -m "feat(db): add get_open_trades plural helper for drain semantics"
```

---

## Task 4: Adapt the existing reconciler unit-test fixtures + integration fake to the plural API

**Files:**
- Modify: `bullforge/tests/test_reconciler_core.py` (mock fixture + 6 existing test bodies)
- Modify: `bullforge/tests/test_order_reconciliation_integration.py` (add `get_open_trades` to `FakeDB`)

This task is purely mechanical. After it, the existing 17 reconciler tests still pass against the **unchanged** production code (because `_handle_fill` still calls `get_open_trade`), and the integration fake gains the method `_handle_fill` will start calling in Task 6. We deliberately do NOT touch `_handle_fill` here — the production change rides in Task 6 with the failing-test-first cycle for the new behavior.

To keep the existing tests passing in the meantime, we keep both `mock_db.get_open_trade.return_value = ...` AND set `mock_db.get_open_trades.return_value = [...]` in each test that previously set the singular. After Task 6, the singular line is dead but harmless; we'll prune it in Task 6's cleanup step.

- [ ] **Step 1: Extend the `mock_db` fixture in `bullforge/tests/test_reconciler_core.py`**

Find the `mock_db` fixture at lines 44–51:

```python
@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.upsert_order_from_alpaca = AsyncMock()
    db.insert_trade = AsyncMock(return_value=1)
    db.update_trade_exit = AsyncMock()
    db.get_open_trade = AsyncMock(return_value=None)
    return db
```

Add `get_open_trades` immediately after `get_open_trade`:

```python
@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.upsert_order_from_alpaca = AsyncMock()
    db.insert_trade = AsyncMock(return_value=1)
    db.update_trade_exit = AsyncMock()
    db.get_open_trade = AsyncMock(return_value=None)
    db.get_open_trades = AsyncMock(return_value=[])
    return db
```

- [ ] **Step 2: Mirror every `mock_db.get_open_trade.return_value = ...` with the plural form**

Run this grep to find the current call sites:

```bash
bin/test python -c "
from pathlib import Path
import re
src = Path('tests/test_reconciler_core.py').read_text(encoding='utf-8')
for i, line in enumerate(src.splitlines(), 1):
    if 'mock_db.get_open_trade.return_value' in line:
        print(f'{i}: {line.strip()}')
"
```

You should see 5 matches. For each, **immediately after** the singular line, add a sibling line setting the plural mock to a list of the same content (or `[]` when the singular was `None`):

```python
mock_db.get_open_trade.return_value = None
```
becomes:
```python
mock_db.get_open_trade.return_value = None
mock_db.get_open_trades.return_value = []
```

```python
mock_db.get_open_trade.return_value = open_trade
```
becomes:
```python
mock_db.get_open_trade.return_value = open_trade
mock_db.get_open_trades.return_value = [open_trade]
```

Apply the transformation everywhere the grep showed a match. Use whatever variable was on the right-hand side of the singular line as the single element of the plural list (or `[]` for `None`).

- [ ] **Step 3: Add `get_open_trades` to `FakeDB` in `bullforge/tests/test_order_reconciliation_integration.py`**

Open `bullforge/tests/test_order_reconciliation_integration.py`. Find `get_open_trade` at line 119:

```python
    async def get_open_trade(self, symbol: str) -> Optional[Trade]:
        for t in reversed(self._trades):
            if t.symbol == symbol and getattr(t, "exit_time", None) is None:
                return t
        return None
```

Immediately after the closing `return None` of that method, append:

```python

    async def get_open_trades(self, symbol: str) -> list[Trade]:
        return [
            t for t in self._trades
            if t.symbol == symbol and getattr(t, "exit_time", None) is None
        ]
```

The list comprehension iterates `self._trades` in insertion order, which is FIFO oldest-first by construction (the fake appends on `insert_trade`). That matches the production helper's `ORDER BY entry_time ASC`.

- [ ] **Step 4: Run all three test files and confirm everything is still green**

```bash
bin/test python -m pytest tests/test_reconciler_core.py tests/test_monitoring_db.py tests/test_order_reconciliation_integration.py -v
```

Expected: 17 + (existing test_monitoring_db.py count + 2 from Task 2) + (existing test_order_reconciliation_integration.py count) tests pass. No reds.

- [ ] **Step 5: Commit**

```bash
git add bullforge/tests/test_reconciler_core.py bullforge/tests/test_order_reconciliation_integration.py
git commit -m "test(reconciler): mirror get_open_trade mocks with plural for drain prep"
```

---

## Task 5: Failing test for the multi-row exact-match drain (the actual incident)

**Files:**
- Modify: `bullforge/tests/test_reconciler_core.py` (append one test)

This is the test that captures the BTC/USD incident: 4 open longs, broker fills one sell that aggregates them all, all four rows must close at the same exit price/time with per-row pnl computed from each row's own entry_price and quantity, and four `position_closed` alerts must publish.

- [ ] **Step 1: Append the test**

At the end of `bullforge/tests/test_reconciler_core.py`, append:

```python
@pytest.mark.asyncio
async def test_drain_full_close_multiple_longs_emits_per_row_alerts_and_pnls(
    mock_db, mock_redis, mock_broker,
):
    core = ReconcilerCore(
        db=mock_db, redis_client=mock_redis, broker=mock_broker,
    )
    # Simulate the BTC/USD incident: four open longs at different entry prices,
    # one sell fill that aggregates them all.
    rows = [
        Trade(
            id=150, symbol="BTC/USD", side="long",
            entry_time=datetime(2026, 5, 5, 0, 57, tzinfo=timezone.utc),
            entry_price=60_000.0, quantity=0.001, entry_order_id=100,
        ),
        Trade(
            id=151, symbol="BTC/USD", side="long",
            entry_time=datetime(2026, 5, 5, 0, 58, tzinfo=timezone.utc),
            entry_price=60_050.0, quantity=0.001, entry_order_id=101,
        ),
        Trade(
            id=152, symbol="BTC/USD", side="long",
            entry_time=datetime(2026, 5, 5, 0, 59, tzinfo=timezone.utc),
            entry_price=60_100.0, quantity=0.001, entry_order_id=102,
        ),
        Trade(
            id=153, symbol="BTC/USD", side="long",
            entry_time=datetime(2026, 5, 5, 1, 2, tzinfo=timezone.utc),
            entry_price=60_200.0, quantity=0.001, entry_order_id=103,
        ),
    ]
    mock_db.get_open_trades.return_value = rows
    mock_db.query_closed_trades.return_value = []
    mock_db.upsert_order_from_alpaca.return_value = UpsertResult(
        order=_order(status="filled", side="sell", filled_price=61_000.0),
        old_status="submitted", new_status="filled", inserted=False,
    )
    snap = _snapshot(status="filled", side="sell", symbol="BTC/USD")
    snap["filled_avg_price"] = 61_000.0
    snap["filled_qty"] = 0.004  # exact aggregate of the four rows
    snap["qty"] = 0.004

    await core.apply_alpaca_order(snap, source="manual_replay")

    # Four update_trade_exit calls, one per row, with per-row pnl.
    assert mock_db.update_trade_exit.await_count == 4
    closed_ids = [
        c.kwargs["trade_id"] for c in mock_db.update_trade_exit.await_args_list
    ]
    assert closed_ids == [150, 151, 152, 153]
    pnls = [c.kwargs["pnl"] for c in mock_db.update_trade_exit.await_args_list]
    assert pnls[0] == pytest.approx((61_000.0 - 60_000.0) * 0.001)
    assert pnls[1] == pytest.approx((61_000.0 - 60_050.0) * 0.001)
    assert pnls[2] == pytest.approx((61_000.0 - 60_100.0) * 0.001)
    assert pnls[3] == pytest.approx((61_000.0 - 60_200.0) * 0.001)

    # Four position_closed alerts published.
    alerts = _alert_payloads(mock_redis, "monitoring:alerts")
    assert len(alerts) == 4
    assert {a["trade_id"] for a in alerts} == {150, 151, 152, 153}
    assert all(a["type"] == "position_closed" for a in alerts)
    assert all(a["exit_price"] == 61_000.0 for a in alerts)

    # No anomaly emitted on exact match.
    counter_calls = [
        json.loads(c.args[1])
        for c in mock_redis.publish.await_args_list
        if c.args[0] == "monitoring:reconciler"
    ]
    anomaly_kinds = [
        c["labels"].get("kind") for c in counter_calls
        if c.get("counter") == "reconciler_anomalies_total"
    ]
    assert anomaly_kinds == []
```

- [ ] **Step 2: Run the new test and confirm it fails**

```bash
bin/test python -m pytest tests/test_reconciler_core.py::test_drain_full_close_multiple_longs_emits_per_row_alerts_and_pnls -v
```

Expected: fails. Failure mode is most likely `assert mock_db.update_trade_exit.await_count == 4` evaluating to `1` — the existing `_handle_fill` only closes the most recent open trade (returned by `get_open_trade`).

- [ ] **Step 3: Commit the failing test**

```bash
git add bullforge/tests/test_reconciler_core.py
git commit -m "test(reconciler): cover drain on multi-row aggregate close (failing)"
```

---

## Task 6: Implement the drain in `_handle_fill`

**Files:**
- Modify: `bullforge/services/execution_service/reconciler_core.py` (lines 87–133)
- Cleanup: `bullforge/tests/test_reconciler_core.py` (remove the now-dead `mock_db.get_open_trade.return_value = ...` lines from Task 4)

This is the production change. After it:
- The 17 pre-existing reconciler tests still pass via the single-row path baked into the new logic (their `[open_trade]` mocks behave equivalently to the prior `open_trade` mock).
- The Task 5 failing test passes.
- The exact-match path is the only drain-branch covered yet; over-close and partial-FIFO branches arrive in Tasks 7–9 and will currently no-op (over-close just falls through `fq >= total_open` for our exact match, partial-FIFO is unreachable from any test).

- [ ] **Step 1: Replace the body of `_handle_fill`**

Open `bullforge/services/execution_service/reconciler_core.py`. Find `_handle_fill` at line 87:

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
            await self._publish_position_closed(
                open_trade=open_trade,
                order=order,
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

Replace the entire method with:

```python
    async def _handle_fill(self, order: Order) -> None:
        if order.filled_price is None or order.filled_at is None:
            logger.warning(
                "fill event for %s missing price/time; skipping trade bookkeeping",
                order.alpaca_order_id,
            )
            return

        open_trades = await self._db.get_open_trades(order.symbol)

        is_closing = bool(open_trades) and (
            (open_trades[0].side == "long" and order.side == "sell")
            or (open_trades[0].side == "short" and order.side == "buy")
        )

        if not is_closing:
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
            return

        # Closing fill — drain N rows.
        fq = float(order.filled_quantity or order.quantity)
        total_open = sum(float(t.quantity) for t in open_trades)

        if fq >= total_open:
            rows_to_close = list(open_trades)
            if fq > total_open:
                await self._publish_counter(
                    "reconciler_anomalies_total", kind="over_close",
                )
                await self._publish("execution:orders:anomaly", order)
        else:
            rows_to_close = []
            running = 0.0
            for t in open_trades:
                if running + float(t.quantity) <= fq:
                    rows_to_close.append(t)
                    running += float(t.quantity)
                else:
                    break
            residual = fq - running
            if residual > 0:
                await self._publish_counter(
                    "reconciler_anomalies_total",
                    kind="partial_drain_residual",
                )
                await self._publish("execution:orders:anomaly", order)

        for t in rows_to_close:
            pnl = (order.filled_price - t.entry_price) * t.quantity
            if t.side == "short":
                pnl = -pnl
            hold_secs = int(
                (order.filled_at - t.entry_time).total_seconds()
            )
            await self._db.update_trade_exit(
                trade_id=t.id,
                exit_time=order.filled_at,
                exit_price=order.filled_price,
                exit_order_id=order.id,
                pnl=pnl,
                hold_duration_seconds=hold_secs,
            )
            await self._publish_position_closed(
                open_trade=t,
                order=order,
                pnl=pnl,
                hold_duration_seconds=hold_secs,
            )
```

- [ ] **Step 2: Remove the now-dead `mock_db.get_open_trade.return_value = ...` lines you added in Task 4**

In `bullforge/tests/test_reconciler_core.py`, every place you mirrored the singular into the plural now has a singular line that is no longer read by production code. Remove the singular lines (keep the plural). This isn't strictly required for passing tests, but it removes a foot-gun where a future contributor changes the singular and is surprised it has no effect.

For example:
```python
    mock_db.get_open_trade.return_value = open_trade
    mock_db.get_open_trades.return_value = [open_trade]
```
becomes:
```python
    mock_db.get_open_trades.return_value = [open_trade]
```

You can also drop `db.get_open_trade = AsyncMock(return_value=None)` from the `mock_db` fixture (line 50) for the same reason — production code no longer calls it.

- [ ] **Step 3: Run the full reconciler test suite**

```bash
bin/test python -m pytest tests/test_reconciler_core.py tests/test_order_reconciliation_integration.py -v
```

Expected: 17 pre-existing reconciler tests pass + the new Task 5 test passes + the integration tests stay green.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/execution_service/reconciler_core.py bullforge/tests/test_reconciler_core.py
git commit -m "fix(reconciler): drain N open trade rows when a fill aggregates them"
```

---

## Task 7: Failing test + implementation for over-close anomaly

**Files:**
- Modify: `bullforge/tests/test_reconciler_core.py` (append one test)
- Verify: `bullforge/services/execution_service/reconciler_core.py` (no further code change — Task 6 already wired the over-close branch; this task's "implementation" is just confirming the existing code passes the new test)

The over-close branch in Task 6 emits `reconciler_anomalies_total{kind=over_close}` on `monitoring:reconciler` and an `execution:orders:anomaly` publish, then closes all rows. We assert that combined behavior here.

- [ ] **Step 1: Append the test**

At the end of `bullforge/tests/test_reconciler_core.py`, append:

```python
@pytest.mark.asyncio
async def test_drain_over_close_closes_all_and_emits_anomaly(
    mock_db, mock_redis, mock_broker,
):
    core = ReconcilerCore(
        db=mock_db, redis_client=mock_redis, broker=mock_broker,
    )
    rows = [
        Trade(
            id=200, symbol="ETH/USD", side="long",
            entry_time=datetime(2026, 5, 5, 1, 0, tzinfo=timezone.utc),
            entry_price=2_000.0, quantity=1.0, entry_order_id=200,
        ),
        Trade(
            id=201, symbol="ETH/USD", side="long",
            entry_time=datetime(2026, 5, 5, 1, 1, tzinfo=timezone.utc),
            entry_price=2_010.0, quantity=1.0, entry_order_id=201,
        ),
    ]
    mock_db.get_open_trades.return_value = rows
    mock_db.query_closed_trades.return_value = []
    mock_db.upsert_order_from_alpaca.return_value = UpsertResult(
        order=_order(status="filled", side="sell", filled_price=2_100.0),
        old_status="submitted", new_status="filled", inserted=False,
    )
    snap = _snapshot(status="filled", side="sell", symbol="ETH/USD")
    snap["filled_avg_price"] = 2_100.0
    snap["filled_qty"] = 5.0  # broker reports 5 closed, we only tracked 2
    snap["qty"] = 5.0

    await core.apply_alpaca_order(snap, source="stream")

    # Both rows closed.
    assert mock_db.update_trade_exit.await_count == 2

    # Anomaly counter published with kind=over_close.
    counter_calls = [
        json.loads(c.args[1])
        for c in mock_redis.publish.await_args_list
        if c.args[0] == "monitoring:reconciler"
    ]
    over_close_calls = [
        c for c in counter_calls
        if c.get("counter") == "reconciler_anomalies_total"
        and c["labels"].get("kind") == "over_close"
    ]
    assert len(over_close_calls) == 1

    # Order-level anomaly publish on execution:orders:anomaly.
    anomaly_publishes = [
        c for c in mock_redis.publish.await_args_list
        if c.args[0] == "execution:orders:anomaly"
    ]
    assert len(anomaly_publishes) == 1
```

- [ ] **Step 2: Run the test and confirm it passes (no production change needed)**

```bash
bin/test python -m pytest tests/test_reconciler_core.py::test_drain_over_close_closes_all_and_emits_anomaly -v
```

Expected: PASS. The implementation in Task 6 already covers this branch.

If it fails, the most likely cause is the `kind` label not threading through — re-check `_publish_counter` invocation in `_handle_fill`'s over-close branch.

- [ ] **Step 3: Commit**

```bash
git add bullforge/tests/test_reconciler_core.py
git commit -m "test(reconciler): cover over-close anomaly branch"
```

---

## Task 8: Failing test + implementation for partial-FIFO drain with residual anomaly

**Files:**
- Modify: `bullforge/tests/test_reconciler_core.py` (append one test)
- Verify: `bullforge/services/execution_service/reconciler_core.py` (already covered by Task 6's partial-FIFO branch)

A partial fill closes the FIFO oldest rows whose summed qty fits inside `filled_qty`. If a residual remains (`filled_qty - sum_closed > 0`), we emit `kind=partial_drain_residual` plus `execution:orders:anomaly`. Newer rows past the boundary stay open.

- [ ] **Step 1: Append the test**

```python
@pytest.mark.asyncio
async def test_drain_partial_fifo_closes_oldest_fitting_rows_and_emits_residual_anomaly(
    mock_db, mock_redis, mock_broker,
):
    core = ReconcilerCore(
        db=mock_db, redis_client=mock_redis, broker=mock_broker,
    )
    # Four rows of 10 qty each (total 40). Sell of 25 → close oldest 2 rows
    # (sum 20), residual 5, two newest rows untouched, residual anomaly emitted.
    rows = [
        Trade(
            id=300 + i, symbol="SPY", side="long",
            entry_time=datetime(2026, 5, 5, 14, i, tzinfo=timezone.utc),
            entry_price=400.0 + i, quantity=10.0, entry_order_id=300 + i,
        )
        for i in range(4)
    ]
    mock_db.get_open_trades.return_value = rows
    mock_db.query_closed_trades.return_value = []
    mock_db.upsert_order_from_alpaca.return_value = UpsertResult(
        order=_order(status="filled", side="sell", filled_price=410.0),
        old_status="submitted", new_status="filled", inserted=False,
    )
    snap = _snapshot(status="filled", side="sell")
    snap["filled_avg_price"] = 410.0
    snap["filled_qty"] = 25.0
    snap["qty"] = 25.0

    await core.apply_alpaca_order(snap, source="stream")

    # Two oldest rows closed.
    assert mock_db.update_trade_exit.await_count == 2
    closed_ids = [
        c.kwargs["trade_id"] for c in mock_db.update_trade_exit.await_args_list
    ]
    assert closed_ids == [300, 301]

    # Residual anomaly counter.
    counter_calls = [
        json.loads(c.args[1])
        for c in mock_redis.publish.await_args_list
        if c.args[0] == "monitoring:reconciler"
    ]
    residual_calls = [
        c for c in counter_calls
        if c.get("counter") == "reconciler_anomalies_total"
        and c["labels"].get("kind") == "partial_drain_residual"
    ]
    assert len(residual_calls) == 1

    # Order-level anomaly publish.
    anomaly_publishes = [
        c for c in mock_redis.publish.await_args_list
        if c.args[0] == "execution:orders:anomaly"
    ]
    assert len(anomaly_publishes) == 1
```

- [ ] **Step 2: Run the test and confirm it passes**

```bash
bin/test python -m pytest tests/test_reconciler_core.py::test_drain_partial_fifo_closes_oldest_fitting_rows_and_emits_residual_anomaly -v
```

Expected: PASS. Task 6's partial branch handles this.

- [ ] **Step 3: Commit**

```bash
git add bullforge/tests/test_reconciler_core.py
git commit -m "test(reconciler): cover partial-FIFO drain with residual anomaly"
```

---

## Task 9: Failing test for partial-exact-alignment (no residual anomaly)

**Files:**
- Modify: `bullforge/tests/test_reconciler_core.py` (append one test)

Edge case: `filled_qty` is strictly less than `total_open` but exactly aligns with the sum of N oldest rows. No residual, so no anomaly.

- [ ] **Step 1: Append the test**

```python
@pytest.mark.asyncio
async def test_drain_partial_exact_alignment_no_residual_anomaly(
    mock_db, mock_redis, mock_broker,
):
    core = ReconcilerCore(
        db=mock_db, redis_client=mock_redis, broker=mock_broker,
    )
    # Four rows of 10 qty each (total 40). Sell of 20 → close oldest 2,
    # residual exactly 0, no anomaly.
    rows = [
        Trade(
            id=400 + i, symbol="QQQ", side="long",
            entry_time=datetime(2026, 5, 5, 15, i, tzinfo=timezone.utc),
            entry_price=350.0 + i, quantity=10.0, entry_order_id=400 + i,
        )
        for i in range(4)
    ]
    mock_db.get_open_trades.return_value = rows
    mock_db.query_closed_trades.return_value = []
    mock_db.upsert_order_from_alpaca.return_value = UpsertResult(
        order=_order(status="filled", side="sell", filled_price=360.0),
        old_status="submitted", new_status="filled", inserted=False,
    )
    snap = _snapshot(status="filled", side="sell")
    snap["filled_avg_price"] = 360.0
    snap["filled_qty"] = 20.0
    snap["qty"] = 20.0

    await core.apply_alpaca_order(snap, source="stream")

    assert mock_db.update_trade_exit.await_count == 2
    counter_calls = [
        json.loads(c.args[1])
        for c in mock_redis.publish.await_args_list
        if c.args[0] == "monitoring:reconciler"
    ]
    anomaly_kinds = [
        c["labels"].get("kind") for c in counter_calls
        if c.get("counter") == "reconciler_anomalies_total"
    ]
    assert anomaly_kinds == []
    anomaly_publishes = [
        c for c in mock_redis.publish.await_args_list
        if c.args[0] == "execution:orders:anomaly"
    ]
    assert anomaly_publishes == []
```

- [ ] **Step 2: Run and confirm it passes**

```bash
bin/test python -m pytest tests/test_reconciler_core.py::test_drain_partial_exact_alignment_no_residual_anomaly -v
```

Expected: PASS. The partial-FIFO branch's `if residual > 0:` guard handles this.

- [ ] **Step 3: Commit**

```bash
git add bullforge/tests/test_reconciler_core.py
git commit -m "test(reconciler): cover partial-FIFO exact alignment (no residual)"
```

---

## Task 10: Failing tests for symmetric short drain + single-row regression guard

**Files:**
- Modify: `bullforge/tests/test_reconciler_core.py` (append two tests)

Equities can short. The drain must work symmetrically for `buy + open_short` so the bug we're fixing on the long side doesn't have a parallel ghost on the short side. We also add an explicit single-row regression test that pins `update_trade_exit.await_count == 1` for the trivial case — the existing `test_closing_fill_publishes_position_closed` covers payload shape but doesn't lock down the call count, so a future drain refactor that accidentally double-closes a single row could slip through.

- [ ] **Step 1: Append the symmetric-short test**

```python
@pytest.mark.asyncio
async def test_drain_buy_closes_multiple_shorts_symmetric(
    mock_db, mock_redis, mock_broker,
):
    core = ReconcilerCore(
        db=mock_db, redis_client=mock_redis, broker=mock_broker,
    )
    rows = [
        Trade(
            id=500, symbol="SPY", side="short",
            entry_time=datetime(2026, 5, 5, 16, 0, tzinfo=timezone.utc),
            entry_price=420.0, quantity=5.0, entry_order_id=500,
        ),
        Trade(
            id=501, symbol="SPY", side="short",
            entry_time=datetime(2026, 5, 5, 16, 1, tzinfo=timezone.utc),
            entry_price=421.0, quantity=5.0, entry_order_id=501,
        ),
    ]
    mock_db.get_open_trades.return_value = rows
    mock_db.query_closed_trades.return_value = []
    mock_db.upsert_order_from_alpaca.return_value = UpsertResult(
        order=_order(status="filled", side="buy", filled_price=415.0),
        old_status="submitted", new_status="filled", inserted=False,
    )
    snap = _snapshot(status="filled", side="buy")
    snap["filled_avg_price"] = 415.0
    snap["filled_qty"] = 10.0
    snap["qty"] = 10.0

    await core.apply_alpaca_order(snap, source="stream")

    # Both shorts closed.
    assert mock_db.update_trade_exit.await_count == 2
    pnls = [c.kwargs["pnl"] for c in mock_db.update_trade_exit.await_args_list]
    # Short pnl is negated: (filled - entry) * qty * -1
    assert pnls[0] == pytest.approx(-(415.0 - 420.0) * 5.0)  # +25
    assert pnls[1] == pytest.approx(-(415.0 - 421.0) * 5.0)  # +30
```

- [ ] **Step 2: Append the single-row regression-guard test**

```python
@pytest.mark.asyncio
async def test_single_open_trade_drain_path_is_existing_behavior(
    mock_db, mock_redis, mock_broker,
):
    """Single open row + sell of equal qty must close exactly that one row.

    Complementary to the existing test_closing_fill_publishes_position_closed:
    that one asserts payload shape, this one pins call count so a future
    refactor of the drain can't accidentally double-close.
    """
    core = ReconcilerCore(
        db=mock_db, redis_client=mock_redis, broker=mock_broker,
    )
    open_trade = Trade(
        id=600, symbol="SPY", side="long",
        entry_time=datetime(2026, 5, 5, 17, 0, tzinfo=timezone.utc),
        entry_price=400.0, quantity=2.0, entry_order_id=600,
    )
    mock_db.get_open_trades.return_value = [open_trade]
    mock_db.query_closed_trades.return_value = []
    mock_db.upsert_order_from_alpaca.return_value = UpsertResult(
        order=_order(status="filled", side="sell", filled_price=410.0),
        old_status="submitted", new_status="filled", inserted=False,
    )
    snap = _snapshot(status="filled", side="sell")
    snap["filled_avg_price"] = 410.0
    snap["filled_qty"] = 2.0
    snap["qty"] = 2.0

    await core.apply_alpaca_order(snap, source="stream")

    # Exactly one close, no duplicates.
    assert mock_db.update_trade_exit.await_count == 1
    assert mock_db.update_trade_exit.await_args.kwargs["trade_id"] == 600
    # No anomaly on a clean single-row drain.
    counter_calls = [
        json.loads(c.args[1])
        for c in mock_redis.publish.await_args_list
        if c.args[0] == "monitoring:reconciler"
    ]
    anomaly_kinds = [
        c["labels"].get("kind") for c in counter_calls
        if c.get("counter") == "reconciler_anomalies_total"
    ]
    assert anomaly_kinds == []
    # Exactly one position_closed alert.
    alerts = _alert_payloads(mock_redis, "monitoring:alerts")
    assert len(alerts) == 1
    assert alerts[0]["trade_id"] == 600
```

- [ ] **Step 3: Run both new tests and confirm they pass**

```bash
bin/test python -m pytest tests/test_reconciler_core.py::test_drain_buy_closes_multiple_shorts_symmetric tests/test_reconciler_core.py::test_single_open_trade_drain_path_is_existing_behavior -v
```

Expected: both PASS. The drain's `is_closing` check covers `short + buy`, and the single-row case naturally falls into the `fq == total_open` branch with `len(rows_to_close) == 1`.

- [ ] **Step 4: Run the full reconciler suite to confirm no regression**

```bash
bin/test python -m pytest tests/test_reconciler_core.py -v
```

Expected: 17 pre-existing + 6 new drain tests = 23 tests pass.

- [ ] **Step 5: Commit**

```bash
git add bullforge/tests/test_reconciler_core.py
git commit -m "test(reconciler): cover symmetric short drain + single-row regression guard"
```

---

## Task 11: Refactor `AlpacaBroker._sync_list_orders` + add `list_closed_orders`

**Files:**
- Modify: `bullforge/services/execution_service/alpaca_broker.py` (lines 50–51, 104–111)
- Test: there is no existing unit test file for `alpaca_broker.py` (it talks to the real Alpaca SDK; the existing reconciler/integration tests use a `FakeBroker`). The replay-CLI tests in Task 14 will exercise `list_closed_orders` end-to-end via mock dispatch. We do **not** add a dedicated `test_alpaca_broker.py` here — every change in this task is mechanical refactoring of a method already exercised by the live execution service.

- [ ] **Step 1: Add the generic helper and the new public method**

Open `bullforge/services/execution_service/alpaca_broker.py`. Find the public `list_open_orders` at lines 50–51 and the sync helper at lines 104–111. Replace them with:

```python
    async def list_open_orders(self, after) -> list[dict]:
        return await asyncio.to_thread(self._sync_list_open_orders, after)

    async def list_closed_orders(self, after) -> list[dict]:
        return await asyncio.to_thread(
            self._sync_list_orders, QueryOrderStatus.CLOSED, after, 500,
        )
```

And replace the sync helper at line 104:

```python
    def _sync_list_open_orders(self, after) -> list[dict]:
        return self._sync_list_orders(QueryOrderStatus.OPEN, after, 500)

    def _sync_list_orders(
        self, status: QueryOrderStatus, after, limit: int = 500,
    ) -> list[dict]:
        request = GetOrdersRequest(status=status, after=after, limit=limit)
        orders = self._client.get_orders(request)
        return [self._format_order_snapshot(o) for o in orders]
```

The import line at the top (`from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus`) already includes `QueryOrderStatus`, no import change needed.

- [ ] **Step 2: Confirm the existing reconciler tests + integration tests still pass**

Nothing else calls `_sync_list_open_orders` or `list_open_orders` from inside the test suite, but a syntax error here would break import on every reconciler test (they import `services.execution_service.reconciler_core`, which doesn't pull `alpaca_broker`, but the integration test file may). Run the broader sweep to be safe:

```bash
bin/test python -m pytest tests/test_reconciler_core.py tests/test_order_reconciliation_integration.py -v
```

Expected: green. (None of these tests exercise `list_open_orders` or `list_closed_orders` directly — they're verifying the import didn't break.)

- [ ] **Step 3: Smoke-test the import explicitly**

```bash
bin/test python -c "from services.execution_service.alpaca_broker import AlpacaBroker; print('import ok')"
```

Expected: `import ok`. If you get a SyntaxError or NameError, fix and re-run before committing.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/execution_service/alpaca_broker.py
git commit -m "refactor(broker): factor list_orders, add list_closed_orders for replay CLI"
```

---

## Task 12: Failing tests for the replay CLI — argument parsing

**Files:**
- Create: `bullforge/tests/test_reconcile_from_alpaca.py`

The CLI's `_parse_args` exposes `--symbol` (required, comma-separated), `--since` (ISO datetime, optional), `--dry-run` (flag). When `--since` is omitted, the script computes the default at run time as `now - 24h` — we test that in Task 14's default-lookback test, not here.

- [ ] **Step 1: Create the test file with the import-shape tests**

Create `bullforge/tests/test_reconcile_from_alpaca.py` with:

```python
import json
import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# The script lives at scripts/reconcile_from_alpaca.py; tests import it as a
# module. PYTHONPATH=/app already covers `scripts.` because main packaging
# treats `/app` as the root.
def _import_module():
    import importlib
    return importlib.import_module("scripts.reconcile_from_alpaca")


def test_parse_args_requires_symbol(monkeypatch):
    mod = _import_module()
    monkeypatch.setattr(sys, "argv", ["reconcile_from_alpaca.py"])
    with pytest.raises(SystemExit):
        mod._parse_args()


def test_parse_args_accepts_single_symbol(monkeypatch):
    mod = _import_module()
    monkeypatch.setattr(
        sys, "argv",
        ["reconcile_from_alpaca.py", "--symbol", "BTC/USD"],
    )
    args = mod._parse_args()
    assert args.symbol == "BTC/USD"
    assert args.since is None
    assert args.dry_run is False


def test_parse_args_accepts_comma_separated_symbols(monkeypatch):
    mod = _import_module()
    monkeypatch.setattr(
        sys, "argv",
        ["reconcile_from_alpaca.py", "--symbol", "BTC/USD,ETH/USD"],
    )
    args = mod._parse_args()
    assert args.symbol == "BTC/USD,ETH/USD"


def test_parse_args_parses_since_iso(monkeypatch):
    mod = _import_module()
    monkeypatch.setattr(
        sys, "argv",
        [
            "reconcile_from_alpaca.py",
            "--symbol", "BTC/USD",
            "--since", "2026-05-05T00:00:00+00:00",
        ],
    )
    args = mod._parse_args()
    assert args.since == datetime(2026, 5, 5, 0, 0, 0, tzinfo=timezone.utc)


def test_parse_args_dry_run_flag(monkeypatch):
    mod = _import_module()
    monkeypatch.setattr(
        sys, "argv",
        ["reconcile_from_alpaca.py", "--symbol", "BTC/USD", "--dry-run"],
    )
    args = mod._parse_args()
    assert args.dry_run is True
```

- [ ] **Step 2: Run the new tests and confirm they fail with `ModuleNotFoundError`**

```bash
bin/test python -m pytest tests/test_reconcile_from_alpaca.py -v
```

Expected: every test errors with `ModuleNotFoundError: No module named 'scripts.reconcile_from_alpaca'`. That's the correct red.

- [ ] **Step 3: Commit the failing tests**

```bash
git add bullforge/tests/test_reconcile_from_alpaca.py
git commit -m "test(scripts): cover reconcile_from_alpaca CLI arg parsing (failing)"
```

---

## Task 13: Implement the replay CLI

**Files:**
- Create: `bullforge/scripts/reconcile_from_alpaca.py`

The script imports the same plumbing `scripts/backtest.py` does — `Settings`, `Database` (live, non-sandbox), `RedisClient`, `AlpacaBroker`, `ReconcilerCore` — and wires them into a one-pass loop. It does NOT need `RuntimeConfig` (no tunables) and does NOT need `ModelStore`.

- [ ] **Step 1: Create the script**

Create `bullforge/scripts/reconcile_from_alpaca.py` with:

```python
"""One-shot Alpaca → DB reconciliation for closed orders.

Use when the trade-updates websocket missed events (e.g., a manual broker-UI
position close) and the polling sweeper has already moved past the relevant
window. Idempotent: known alpaca_order_ids are no-op'd by the upsert path.

Example:
    PYTHONPATH=. python scripts/reconcile_from_alpaca.py \\
        --symbol BTC/USD \\
        --since 2026-05-05T00:00:00+00:00 \\
        --dry-run
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient

from services.execution_service.alpaca_broker import AlpacaBroker
from services.execution_service.reconciler_core import ReconcilerCore

logger = logging.getLogger(__name__)


def _parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _resolve_since(
    arg_since: datetime | None,
    now: datetime | None = None,
) -> datetime:
    """Return arg_since if provided, else now - 24h. `now` is injectable for tests."""
    if arg_since is not None:
        return arg_since
    return (now or datetime.now(timezone.utc)) - timedelta(hours=24)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Replay Alpaca closed orders into BullForge.",
    )
    p.add_argument(
        "--symbol", required=True,
        help="comma-separated symbols, e.g. BTC/USD or BTC/USD,ETH/USD",
    )
    p.add_argument(
        "--since", type=_parse_iso, default=None,
        help="ISO datetime (UTC); default = now - 24h",
    )
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


async def reconcile(
    *,
    core: ReconcilerCore,
    broker: AlpacaBroker,
    symbols: list[str],
    since: datetime,
    dry_run: bool,
) -> dict:
    summary = {
        "since": since.isoformat(),
        "symbols": symbols,
        "orders_fetched": 0,
        "orders_dispatched": 0,
        "dry_run": dry_run,
    }
    snaps = await broker.list_closed_orders(since)
    for snap in snaps:
        if snap["symbol"] not in symbols:
            continue
        summary["orders_fetched"] += 1
        if dry_run:
            logger.info(
                "[dry-run] would replay alpaca_id=%s %s qty=%s status=%s filled_at=%s",
                snap["id"], snap["side"], snap["qty"],
                snap["status"], snap.get("filled_at"),
            )
            continue
        await core.apply_alpaca_order(snap, source="manual_replay")
        summary["orders_dispatched"] += 1
    return summary


async def _main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    since = _resolve_since(args.since)
    symbols = [s.strip() for s in args.symbol.split(",") if s.strip()]

    settings = Settings()
    db = Database(settings)
    await db.connect()
    redis = RedisClient(settings)
    await redis.connect()
    broker = AlpacaBroker(settings)
    core = ReconcilerCore(db=db, redis_client=redis, broker=broker)

    summary = await reconcile(
        core=core, broker=broker,
        symbols=symbols, since=since, dry_run=args.dry_run,
    )

    print("\n=== Reconciliation summary ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    asyncio.run(_main())
```

- [ ] **Step 2: Run the Task 12 tests and confirm they pass**

```bash
bin/test python -m pytest tests/test_reconcile_from_alpaca.py -v
```

Expected: the five arg-parsing tests pass.

- [ ] **Step 3: Commit**

```bash
git add bullforge/scripts/reconcile_from_alpaca.py
git commit -m "feat(scripts): add reconcile_from_alpaca CLI for closed-order replay"
```

---

## Task 14: Failing tests + verification for the dispatch / dry-run / filtering / default-lookback / summary behavior

**Files:**
- Modify: `bullforge/tests/test_reconcile_from_alpaca.py` (append five tests)

These tests exercise `reconcile(...)` directly with mock broker + mock core. They do not touch the real Alpaca SDK or the real DB.

- [ ] **Step 1: Append the tests**

At the end of `bullforge/tests/test_reconcile_from_alpaca.py`, append:

```python
def _snap(alpaca_id: str, symbol: str, side: str = "sell",
          qty: float = 0.001, status: str = "filled") -> dict:
    return {
        "id": alpaca_id, "symbol": symbol, "side": side, "qty": qty,
        "order_type": "market",
        "submitted_at": datetime(2026, 5, 5, 1, 0, tzinfo=timezone.utc),
        "status": status,
        "filled_qty": qty,
        "filled_avg_price": 60_000.0,
        "filled_at": datetime(2026, 5, 5, 1, 5, tzinfo=timezone.utc),
    }


@pytest.mark.asyncio
async def test_dispatches_each_closed_snapshot_to_reconciler():
    mod = _import_module()
    broker = MagicMock()
    broker.list_closed_orders = AsyncMock(return_value=[
        _snap("alp-1", "BTC/USD"),
        _snap("alp-2", "BTC/USD"),
        _snap("alp-3", "BTC/USD"),
    ])
    core = MagicMock()
    core.apply_alpaca_order = AsyncMock()

    summary = await mod.reconcile(
        core=core, broker=broker,
        symbols=["BTC/USD"],
        since=datetime(2026, 5, 5, 0, 0, tzinfo=timezone.utc),
        dry_run=False,
    )

    assert summary["orders_fetched"] == 3
    assert summary["orders_dispatched"] == 3
    assert core.apply_alpaca_order.await_count == 3
    sources = {c.kwargs["source"] for c in core.apply_alpaca_order.await_args_list}
    assert sources == {"manual_replay"}


@pytest.mark.asyncio
async def test_dry_run_does_not_dispatch():
    mod = _import_module()
    broker = MagicMock()
    broker.list_closed_orders = AsyncMock(return_value=[
        _snap("alp-1", "BTC/USD"),
        _snap("alp-2", "BTC/USD"),
    ])
    core = MagicMock()
    core.apply_alpaca_order = AsyncMock()

    summary = await mod.reconcile(
        core=core, broker=broker,
        symbols=["BTC/USD"],
        since=datetime(2026, 5, 5, 0, 0, tzinfo=timezone.utc),
        dry_run=True,
    )

    assert summary["orders_fetched"] == 2
    assert summary["orders_dispatched"] == 0
    assert summary["dry_run"] is True
    core.apply_alpaca_order.assert_not_called()


@pytest.mark.asyncio
async def test_filters_to_target_symbols_only():
    mod = _import_module()
    broker = MagicMock()
    broker.list_closed_orders = AsyncMock(return_value=[
        _snap("alp-1", "BTC/USD"),
        _snap("alp-2", "ETH/USD"),
        _snap("alp-3", "SPY"),
        _snap("alp-4", "BTC/USD"),
    ])
    core = MagicMock()
    core.apply_alpaca_order = AsyncMock()

    summary = await mod.reconcile(
        core=core, broker=broker,
        symbols=["BTC/USD"],
        since=datetime(2026, 5, 5, 0, 0, tzinfo=timezone.utc),
        dry_run=False,
    )

    assert summary["orders_fetched"] == 2  # only BTC/USD ones
    assert summary["orders_dispatched"] == 2
    dispatched_ids = [
        c.args[0]["id"] for c in core.apply_alpaca_order.await_args_list
    ]
    assert dispatched_ids == ["alp-1", "alp-4"]


def test_resolve_since_default_is_24h_lookback():
    mod = _import_module()
    fake_now = datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc)
    since = mod._resolve_since(None, now=fake_now)
    assert since == fake_now - timedelta(hours=24)


def test_resolve_since_returns_explicit_arg_when_provided():
    mod = _import_module()
    explicit = datetime(2026, 5, 4, 23, 0, tzinfo=timezone.utc)
    fake_now = datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc)
    since = mod._resolve_since(explicit, now=fake_now)
    assert since == explicit  # explicit arg wins, fake_now is ignored


@pytest.mark.asyncio
async def test_summary_counts_match_dispatch():
    mod = _import_module()
    broker = MagicMock()
    broker.list_closed_orders = AsyncMock(return_value=[
        _snap("alp-1", "BTC/USD"),
        _snap("alp-2", "ETH/USD"),  # filtered out
        _snap("alp-3", "BTC/USD"),
    ])
    core = MagicMock()
    core.apply_alpaca_order = AsyncMock()

    summary = await mod.reconcile(
        core=core, broker=broker,
        symbols=["BTC/USD"],
        since=datetime(2026, 5, 5, 0, 0, tzinfo=timezone.utc),
        dry_run=False,
    )

    assert summary["orders_fetched"] == 2
    assert summary["orders_dispatched"] == 2
    assert summary["since"] == "2026-05-05T00:00:00+00:00"
    assert summary["symbols"] == ["BTC/USD"]
    assert summary["dry_run"] is False
```

You'll need to add `from datetime import timedelta` to the existing import at the top of the file:

```python
from datetime import datetime, timedelta, timezone
```

- [ ] **Step 2: Run the new tests and confirm they pass**

```bash
bin/test python -m pytest tests/test_reconcile_from_alpaca.py -v
```

Expected: 5 arg-parsing tests + 6 behavior tests (3 dispatch/dry-run/filter + 2 resolve_since + 1 summary) = 11 pass.

- [ ] **Step 3: Run a final broad test sweep to confirm nothing else regressed**

```bash
bin/test python -m pytest tests/test_reconciler_core.py tests/test_monitoring_db.py tests/test_order_reconciliation_integration.py tests/test_reconcile_from_alpaca.py -v
```

Expected: green across the board. 23 reconciler-core + monitoring-db count + integration count + 11 reconcile-from-alpaca = all pass.

- [ ] **Step 4: Commit**

```bash
git add bullforge/tests/test_reconcile_from_alpaca.py
git commit -m "test(scripts): cover reconcile_from_alpaca dispatch + dry-run + filter + summary"
```

---

## Task 15: Doc update — list anomaly kinds in `redis-pubsub.md`

**Files:**
- Modify: `bullforge/docs/architecture/redis-pubsub.md` (the `monitoring:reconciler` row, line 19)

CLAUDE.md requires that any change to a `redis_client.publish` shape updates `docs/architecture/redis-pubsub.md` in the same commit. Strictly speaking we didn't change the *publish shape* (still `{counter, labels, value}`) — we added two new label values. Spec says document them anyway.

- [ ] **Step 1: Replace the `monitoring:reconciler` row**

Open `bullforge/docs/architecture/redis-pubsub.md`. Find line 19:

```
| `monitoring:reconciler` | `services/execution_service/reconciler_core.py` | *(none currently)* | `{counter, labels, value}` | Reconciler counter metrics. |
```

Replace it with:

```
| `monitoring:reconciler` | `services/execution_service/reconciler_core.py` | *(none currently)* | `{counter, labels, value}` | Reconciler counter metrics. Anomaly kinds for `reconciler_anomalies_total{kind=...}`: `filled_reverted` (order went `filled → cancelled/rejected/expired`), `over_close` (sell fill `filled_qty > sum(open_qty)` for the symbol — drains all rows), `partial_drain_residual` (sell fill `filled_qty < sum(open_qty)` and didn't align with N oldest rows' qty). |
```

Keep the rest of the file unchanged.

- [ ] **Step 2: Spot-check that the doc still renders as valid markdown**

```bash
bin/test python -c "
from pathlib import Path
text = Path('docs/architecture/redis-pubsub.md').read_text(encoding='utf-8')
# Sanity check: every table row should still have the same column count.
for line in text.splitlines():
    if line.startswith('|') and '---' not in line:
        cols = line.count('|') - 1
        if cols not in (5, 0):
            print(f'BAD ROW ({cols} cols): {line[:80]}')
print('ok')
"
```

Expected: just `ok`. If a `BAD ROW` line appears, you accidentally introduced or removed a `|` — fix and re-run.

- [ ] **Step 3: Commit**

```bash
git add bullforge/docs/architecture/redis-pubsub.md
git commit -m "docs(redis-pubsub): list reconciler anomaly kinds"
```

---

## Task 16: Final verification before handoff to finishing-a-development-branch

**Files:** None (verification only).

- [ ] **Step 1: Run the full reconciler + script test surface**

```bash
bin/test python -m pytest tests/test_reconciler_core.py tests/test_monitoring_db.py tests/test_order_reconciliation_integration.py tests/test_reconcile_from_alpaca.py -v
```

Expected: green. Note the count for the handoff summary.

- [ ] **Step 2: Run the broader unit-test sweep to catch any unexpected regression in adjacent code**

```bash
bin/test python -m pytest tests/ -k "not integration and not meta" -q
```

Expected: green. (We exclude `integration` and `meta` because those require the live test stack and aren't relevant to this change.)

- [ ] **Step 3: Confirm the commit graph looks like the TDD pattern you'd expect**

```bash
git log --oneline master..HEAD
```

Expected: sequence of `test(...)` followed by `fix(...)` / `feat(...)` / `refactor(...)` commits, ending with `docs(...)`. Roughly:

```
docs(redis-pubsub): list reconciler anomaly kinds
test(scripts): cover reconcile_from_alpaca dispatch + dry-run + filter + summary
feat(scripts): add reconcile_from_alpaca CLI for closed-order replay
test(scripts): cover reconcile_from_alpaca CLI arg parsing (failing)
refactor(broker): factor list_orders, add list_closed_orders for replay CLI
test(reconciler): cover symmetric short drain + single-row regression guard
test(reconciler): cover partial-FIFO exact alignment (no residual)
test(reconciler): cover partial-FIFO drain with residual anomaly
test(reconciler): cover over-close anomaly branch
fix(reconciler): drain N open trade rows when a fill aggregates them
test(reconciler): cover drain on multi-row aggregate close (failing)
test(reconciler): mirror get_open_trade mocks with plural for drain prep
feat(db): add get_open_trades plural helper for drain semantics
test(db): cover get_open_trades plural helper (failing)
docs(spec): drain semantics for ReconcilerCore + Alpaca replay CLI
```

(Plus the spec commit `451b4db` from before this plan started — total 15 commits on the branch.)

- [ ] **Step 4: Hand off to `superpowers:finishing-a-development-branch`**

Plan is complete. The next skill in the workflow is `superpowers:finishing-a-development-branch`, which will present merge / PR / cleanup options.

Per CLAUDE.md / NEXT_PROMPT.md: merge to master uses `--no-ff` and requires explicit user approval at the moment of merge — the prior session got blocked when it skipped that confirmation. Don't repeat the mistake.

---

## Out of scope (do NOT include in this plan)

- Schema changes to the `trades` table (per-row vs aggregate redesign).
- `RiskManager` changes (crypto long-only fix already shipped via `89f6daa`).
- New HTTP endpoint on `dashboard_service`.
- Pagination beyond Alpaca's 500-order page (warn-and-require-operator is acceptable for a one-shot recovery tool).
- A docker compose `tools` service for the script (host-side standalone matches `scripts/backtest.py`).
- A runbook in `bullforge/docs/runbooks/` (script `--help` is sufficient until a second incident shows reuse).
- Meta-signal gating (meta is shadow-only per CLAUDE.md).
- Production deploy + replay script execution against production. Those are sequenced *after* merge per the spec's "Production rollout" section and require separate explicit user approval at the moment.
