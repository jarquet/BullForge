# Backtest Run/Fold Scoping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `backtest.model_registry`, `backtest.predictions`, and `backtest.orders` explicitly run/fold-scoped so concurrent backtests can't race on `is_active` or cross-contaminate each other's models/orders/predictions, while live-trading code paths stay byte-identical in behavior.

**Architecture:** Add two nullable columns (`backtest_run_id BIGINT`, `fold_index INTEGER`) to the three tables in both schemas. A `BacktestScope` dataclass flows from `BacktestEngine._run_fold` through `Trainer`, `InferenceEngine`, and `OrderExecutor` into `Database` methods as an optional `scope` kwarg. `scope=None` (live) writes NULL and reads `WHERE backtest_run_id IS NULL`; `scope=S` writes/reads with its ids. A partial unique index on `backtest.model_registry (backtest_run_id, fold_index, symbol, timeframe, horizon_minutes, model_type) WHERE is_active = TRUE` replaces the cross-run `is_active` global singleton.

**Tech Stack:** Python 3.12, asyncpg, Postgres/TimescaleDB, pytest-asyncio.

**Spec:** [docs/superpowers/specs/2026-04-17-backtest-run-scoping-design.md](../specs/2026-04-17-backtest-run-scoping-design.md)

---

## Pre-Task: Worktree

The execution skill (`superpowers:subagent-driven-development` or `superpowers:executing-plans`) must create `.worktrees/backtest-scoping` branched from master. Suggested branch: `feat/backtest-run-scoping`. All subsequent task paths are relative to that worktree.

---

## Task 1: Schema migration + `BacktestScope` dataclass

**Files:**
- Create: `bullforge/migrations/018_backtest_run_scoping.sql`
- Modify: `bullforge/shared/models.py` (add `BacktestScope`)
- Create: `bullforge/tests/test_backtest_scope.py`

- [ ] **Step 1: Write the failing unit test for `BacktestScope`**

Create `bullforge/tests/test_backtest_scope.py` with:

```python
import pytest

from shared.models import BacktestScope


def test_backtest_scope_is_frozen():
    s = BacktestScope(backtest_run_id=1, fold_index=0)
    with pytest.raises((AttributeError, Exception)):
        s.backtest_run_id = 2  # type: ignore[misc]


def test_backtest_scope_equality_and_hash():
    a = BacktestScope(backtest_run_id=1, fold_index=0)
    b = BacktestScope(backtest_run_id=1, fold_index=0)
    c = BacktestScope(backtest_run_id=1, fold_index=1)
    assert a == b
    assert a != c
    assert hash(a) == hash(b)
    assert {a, b, c} == {a, c}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd bullforge && pytest tests/test_backtest_scope.py -v`

Expected: FAIL with `ImportError: cannot import name 'BacktestScope' from 'shared.models'`.

- [ ] **Step 3: Add the `BacktestScope` dataclass**

At the end of `bullforge/shared/models.py`, append:

```python
@dataclass(frozen=True)
class BacktestScope:
    """Identifies which backtest run + fold a sandbox row belongs to.

    Threaded as an optional `scope` kwarg through the engine, trainer,
    inference engine, order executor, and Database methods. `None` means
    the caller is a live code path (rows have NULL scope columns).
    """
    backtest_run_id: int
    fold_index: int
```

- [ ] **Step 4: Run to verify pass**

Run: `cd bullforge && pytest tests/test_backtest_scope.py -v`

Expected: 2 passed.

- [ ] **Step 5: Write migration 018**

Create `bullforge/migrations/018_backtest_run_scoping.sql` with:

```sql
-- 018_backtest_run_scoping.sql
--
-- Scopes backtest-sandbox rows to (backtest_run_id, fold_index) so concurrent
-- backtests can't race on the cross-run `is_active` singleton. Live rows
-- (public schema, written with scope=None) keep both columns NULL.
--
-- Sandbox-side copies of these columns would normally be propagated by the
-- live-pool schema sync on the next connect(), but we add them explicitly
-- here too because the partial unique index below references them.

ALTER TABLE public.model_registry ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE public.model_registry ADD COLUMN IF NOT EXISTS fold_index INTEGER;
ALTER TABLE public.predictions    ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE public.predictions    ADD COLUMN IF NOT EXISTS fold_index INTEGER;
ALTER TABLE public.orders         ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE public.orders         ADD COLUMN IF NOT EXISTS fold_index INTEGER;

ALTER TABLE backtest.model_registry ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE backtest.model_registry ADD COLUMN IF NOT EXISTS fold_index INTEGER;
ALTER TABLE backtest.predictions    ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE backtest.predictions    ADD COLUMN IF NOT EXISTS fold_index INTEGER;
ALTER TABLE backtest.orders         ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE backtest.orders         ADD COLUMN IF NOT EXISTS fold_index INTEGER;

-- Wipe intermediate backtest data. User-visible backtest results live in
-- public.backtest_trades / backtest_equity / backtest_folds (already
-- run_id-scoped) and are NOT touched.
TRUNCATE backtest.model_registry, backtest.predictions, backtest.orders;

CREATE UNIQUE INDEX IF NOT EXISTS idx_backtest_model_registry_scoped_active
    ON backtest.model_registry
    (backtest_run_id, fold_index, symbol, timeframe, horizon_minutes, model_type)
    WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_backtest_model_registry_scope_lookup
    ON backtest.model_registry (backtest_run_id, fold_index);
CREATE INDEX IF NOT EXISTS idx_backtest_predictions_scope_lookup
    ON backtest.predictions (backtest_run_id, fold_index);
CREATE INDEX IF NOT EXISTS idx_backtest_orders_scope_lookup
    ON backtest.orders (backtest_run_id, fold_index);
```

- [ ] **Step 6: Commit**

```bash
git add bullforge/migrations/018_backtest_run_scoping.sql \
        bullforge/shared/models.py \
        bullforge/tests/test_backtest_scope.py
git commit -m "feat(backtest): add run/fold scope columns + BacktestScope type"
```

---

## Task 2: Thread `scope` through `model_registry` DB methods

**Files:**
- Modify: `bullforge/shared/db.py` (functions `insert_model_metadata`, `query_active_model`, `query_all_active_models`, `deactivate_models`)
- Modify: `bullforge/tests/test_backtest_scope.py`

- [ ] **Step 1: Write failing tests**

Append to `bullforge/tests/test_backtest_scope.py`:

```python
import pytest
from datetime import datetime, timezone

from shared.config import Settings
from shared.db import Database
from shared.models import BacktestScope, ModelMetadata


def _meta(**overrides) -> ModelMetadata:
    base = dict(
        symbol="SCOPETEST",
        timeframe="1m",
        horizon_minutes=5,
        model_type="gradient_boost",
        artifact_path="/tmp/fake.joblib",
        metrics={"accuracy": 0.5},
        feature_columns=["a"],
        trained_at=datetime.now(timezone.utc),
        train_samples=1,
        test_samples=1,
        is_active=True,
    )
    base.update(overrides)
    return ModelMetadata(**base)


@pytest.mark.asyncio
async def test_insert_model_metadata_live_writes_nulls():
    settings = Settings()
    live = Database(settings)
    await live.connect()
    try:
        row_id = await live.insert_model_metadata(_meta())
        async with live.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT backtest_run_id, fold_index FROM public.model_registry WHERE id=$1",
                row_id,
            )
        assert row["backtest_run_id"] is None
        assert row["fold_index"] is None
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM public.model_registry WHERE symbol='SCOPETEST'"
            )
        await live.disconnect()


@pytest.mark.asyncio
async def test_insert_model_metadata_sandbox_writes_scope():
    settings = Settings()
    live = Database(settings)
    sandbox = Database(settings, sandbox=True)
    await live.connect()
    await sandbox.connect()
    try:
        scope = BacktestScope(backtest_run_id=999_001, fold_index=3)
        row_id = await sandbox.insert_model_metadata(_meta(), scope=scope)
        async with live.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT backtest_run_id, fold_index FROM backtest.model_registry WHERE id=$1",
                row_id,
            )
        assert row["backtest_run_id"] == 999_001
        assert row["fold_index"] == 3
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM backtest.model_registry WHERE symbol='SCOPETEST'"
            )
        await sandbox.disconnect()
        await live.disconnect()


@pytest.mark.asyncio
async def test_query_all_active_models_scoped_filters_to_run():
    settings = Settings()
    live = Database(settings)
    sandbox = Database(settings, sandbox=True)
    await live.connect()
    await sandbox.connect()
    try:
        scope_a = BacktestScope(backtest_run_id=999_101, fold_index=0)
        scope_b = BacktestScope(backtest_run_id=999_102, fold_index=0)
        await sandbox.insert_model_metadata(_meta(symbol="SCOPEA"), scope=scope_a)
        await sandbox.insert_model_metadata(_meta(symbol="SCOPEB"), scope=scope_b)

        a_models = await sandbox.query_all_active_models(scope=scope_a)
        b_models = await sandbox.query_all_active_models(scope=scope_b)
        assert {m.symbol for m in a_models} == {"SCOPEA"}
        assert {m.symbol for m in b_models} == {"SCOPEB"}
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM backtest.model_registry WHERE symbol IN ('SCOPEA','SCOPEB')"
            )
        await sandbox.disconnect()
        await live.disconnect()


@pytest.mark.asyncio
async def test_deactivate_models_scoped_does_not_touch_other_runs():
    settings = Settings()
    live = Database(settings)
    sandbox = Database(settings, sandbox=True)
    await live.connect()
    await sandbox.connect()
    try:
        scope_a = BacktestScope(backtest_run_id=999_201, fold_index=0)
        scope_b = BacktestScope(backtest_run_id=999_202, fold_index=0)
        await sandbox.insert_model_metadata(_meta(symbol="SCOPEX"), scope=scope_a)
        await sandbox.insert_model_metadata(_meta(symbol="SCOPEX"), scope=scope_b)

        # Deactivate scope A. Scope B's model must remain active.
        await sandbox.deactivate_models("SCOPEX", "1m", 5, scope=scope_a)

        a_active = await sandbox.query_all_active_models(scope=scope_a)
        b_active = await sandbox.query_all_active_models(scope=scope_b)
        assert a_active == []
        assert len(b_active) == 1 and b_active[0].symbol == "SCOPEX"
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM backtest.model_registry WHERE symbol='SCOPEX'"
            )
        await sandbox.disconnect()
        await live.disconnect()
```

- [ ] **Step 2: Run to verify failure**

Run: `cd bullforge && pytest tests/test_backtest_scope.py -v`

Expected: 4 new tests FAIL with TypeError about unexpected keyword `scope` (current methods don't accept it).

- [ ] **Step 3: Add the `BacktestScope` import and the `_scope_filter` helper**

At the top of `bullforge/shared/db.py`, update the existing `shared.models` import to include `BacktestScope`:

```python
from shared.models import Bar, FeatureRow, Label, ModelMetadata, Prediction, Order, Trade, ModelStatus, MetaFeatureRow, MetaSignalShadow, BacktestScope
```

Immediately after the `_normalize_alpaca_status` function near the top of the module (before the `class Database` line), add:

```python
def _scope_filter(
    scope: Optional[BacktestScope], *, next_arg: int,
) -> tuple[str, tuple]:
    """Returns a (' AND backtest_run_id ... AND fold_index ...', args) pair to
    append to an existing parameterized WHERE clause.

    `next_arg` is the 1-based index of the next placeholder. For scope=None,
    the clause pins both columns to NULL so live reads never leak sandbox
    rows; for a set scope, the clause equates both columns to the scope's
    ids via $N and $N+1.
    """
    if scope is None:
        return " AND backtest_run_id IS NULL AND fold_index IS NULL", ()
    return (
        f" AND backtest_run_id = ${next_arg} AND fold_index = ${next_arg + 1}",
        (scope.backtest_run_id, scope.fold_index),
    )
```

- [ ] **Step 4: Update `insert_model_metadata`**

Replace the existing method (around line 558) with:

```python
    async def insert_model_metadata(
        self,
        meta: ModelMetadata,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> int:
        run_id = scope.backtest_run_id if scope else None
        fold = scope.fold_index if scope else None
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO model_registry
                    (symbol, timeframe, horizon_minutes, model_type, artifact_path,
                     metrics, feature_columns, trained_at, train_samples, test_samples,
                     is_active, estimator_type, backtest_run_id, fold_index)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                RETURNING id
                """,
                meta.symbol,
                meta.timeframe,
                meta.horizon_minutes,
                meta.model_type,
                meta.artifact_path,
                json.dumps(meta.metrics),
                json.dumps(meta.feature_columns),
                meta.trained_at,
                meta.train_samples,
                meta.test_samples,
                meta.is_active,
                meta.estimator_type,
                run_id,
                fold,
            )
        return row_id
```

- [ ] **Step 5: Update `query_active_model`**

Replace the existing method (around line 584) with:

```python
    async def query_active_model(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> Optional[ModelMetadata]:
        scope_clause, scope_args = _scope_filter(scope, next_arg=4)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT id, symbol, timeframe, horizon_minutes, model_type,
                       artifact_path, metrics, feature_columns, trained_at,
                       train_samples, test_samples, is_active, estimator_type,
                       backtest_run_id, fold_index
                FROM model_registry
                WHERE symbol = $1 AND timeframe = $2
                    AND horizon_minutes = $3 AND is_active = TRUE
                    {scope_clause}
                ORDER BY trained_at DESC
                LIMIT 1
                """,
                symbol, timeframe, horizon_minutes, *scope_args,
            )
        if row is None:
            return None
        return ModelMetadata(
            id=row["id"],
            symbol=row["symbol"],
            timeframe=row["timeframe"],
            horizon_minutes=row["horizon_minutes"],
            model_type=row["model_type"],
            artifact_path=row["artifact_path"],
            metrics=json.loads(row["metrics"]),
            feature_columns=json.loads(row["feature_columns"]),
            trained_at=row["trained_at"],
            train_samples=row["train_samples"],
            test_samples=row["test_samples"],
            is_active=row["is_active"],
            estimator_type=row["estimator_type"],
        )
```

- [ ] **Step 6: Update `query_all_active_models`**

Replace the existing method (around line 624) with:

```python
    async def query_all_active_models(
        self,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> List[ModelMetadata]:
        scope_clause, scope_args = _scope_filter(scope, next_arg=1)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT id, symbol, timeframe, horizon_minutes, model_type,
                       artifact_path, metrics, feature_columns, trained_at,
                       train_samples, test_samples, is_active, estimator_type,
                       backtest_run_id, fold_index
                FROM model_registry
                WHERE is_active = TRUE
                    {scope_clause}
                ORDER BY symbol, timeframe, horizon_minutes
                """,
                *scope_args,
            )
        return [
            ModelMetadata(
                id=r["id"],
                symbol=r["symbol"],
                timeframe=r["timeframe"],
                horizon_minutes=r["horizon_minutes"],
                model_type=r["model_type"],
                artifact_path=r["artifact_path"],
                metrics=json.loads(r["metrics"]),
                feature_columns=json.loads(r["feature_columns"]),
                trained_at=r["trained_at"],
                train_samples=r["train_samples"],
                test_samples=r["test_samples"],
                is_active=r["is_active"],
                estimator_type=r["estimator_type"],
            )
            for r in rows
        ]
```

- [ ] **Step 7: Update `deactivate_models`**

Replace (around line 655) with:

```python
    async def deactivate_models(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> None:
        scope_clause, scope_args = _scope_filter(scope, next_arg=4)
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE model_registry
                SET is_active = FALSE
                WHERE symbol = $1 AND timeframe = $2
                    AND horizon_minutes = $3 AND is_active = TRUE
                    {scope_clause}
                """,
                symbol, timeframe, horizon_minutes, *scope_args,
            )
```

- [ ] **Step 8: Run tests**

Run: `cd bullforge && pytest tests/test_backtest_scope.py tests/test_model_db.py -v`

Expected: 6 passed (2 type tests + 4 DB tests). Existing `test_model_db.py` tests continue to pass because their no-scope calls hit the `scope=None` path.

- [ ] **Step 9: Commit**

```bash
git add bullforge/shared/db.py bullforge/tests/test_backtest_scope.py
git commit -m "feat(db): scope kwarg on model_registry methods"
```

---

## Task 3: Thread `scope` through predictions DB methods

**Files:**
- Modify: `bullforge/shared/db.py` (`insert_prediction`, `insert_predictions`, `query_predictions`)
- Modify: `bullforge/tests/test_backtest_scope.py`

- [ ] **Step 1: Write failing test**

Append to `bullforge/tests/test_backtest_scope.py`:

```python
from shared.models import Prediction


@pytest.mark.asyncio
async def test_insert_prediction_sandbox_writes_scope():
    settings = Settings()
    live = Database(settings)
    sandbox = Database(settings, sandbox=True)
    await live.connect()
    await sandbox.connect()
    try:
        scope = BacktestScope(backtest_run_id=999_301, fold_index=2)
        pred = Prediction(
            symbol="PREDTEST",
            timestamp=datetime.now(timezone.utc),
            timeframe="1m",
            horizon_minutes=5,
            model_id=1,
            predicted_direction=1,
            confidence=0.6,
        )
        await sandbox.insert_prediction(pred, scope=scope)
        async with live.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT backtest_run_id, fold_index
                   FROM backtest.predictions
                   WHERE symbol='PREDTEST' AND timestamp=$1""",
                pred.timestamp,
            )
        assert row["backtest_run_id"] == 999_301
        assert row["fold_index"] == 2
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM backtest.predictions WHERE symbol='PREDTEST'"
            )
        await sandbox.disconnect()
        await live.disconnect()
```

- [ ] **Step 2: Run to verify failure**

Run: `cd bullforge && pytest tests/test_backtest_scope.py::test_insert_prediction_sandbox_writes_scope -v`
Expected: FAIL with TypeError about unexpected `scope` kwarg.

- [ ] **Step 3: Update `insert_prediction`**

Replace the existing method (around line 674) with:

```python
    async def insert_prediction(
        self,
        pred: Prediction,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> None:
        run_id = scope.backtest_run_id if scope else None
        fold = scope.fold_index if scope else None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO predictions
                    (symbol, timestamp, timeframe, horizon_minutes,
                     model_id, predicted_direction, confidence,
                     backtest_run_id, fold_index)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET model_id = $5, predicted_direction = $6, confidence = $7
                """,
                pred.symbol, pred.timestamp, pred.timeframe,
                pred.horizon_minutes, pred.model_id,
                pred.predicted_direction, pred.confidence,
                run_id, fold,
            )
```

- [ ] **Step 4: Update `insert_predictions` (batch)**

Replace (around line 694) with:

```python
    async def insert_predictions(
        self,
        preds: List[Prediction],
        *,
        scope: Optional[BacktestScope] = None,
    ) -> None:
        if not preds:
            return
        run_id = scope.backtest_run_id if scope else None
        fold = scope.fold_index if scope else None
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO predictions
                    (symbol, timestamp, timeframe, horizon_minutes,
                     model_id, predicted_direction, confidence,
                     backtest_run_id, fold_index)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (symbol, timestamp, timeframe, horizon_minutes)
                DO UPDATE SET model_id = $5, predicted_direction = $6, confidence = $7
                """,
                [
                    (
                        p.symbol, p.timestamp, p.timeframe,
                        p.horizon_minutes, p.model_id,
                        p.predicted_direction, p.confidence,
                        run_id, fold,
                    )
                    for p in preds
                ],
            )
```

- [ ] **Step 5: Update `query_predictions`**

Replace (around line 717) with:

```python
    async def query_predictions(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        limit: int = 100,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> List[Prediction]:
        scope_clause, scope_args = _scope_filter(scope, next_arg=4)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT symbol, timestamp, timeframe, horizon_minutes,
                       model_id, predicted_direction, confidence
                FROM predictions
                WHERE symbol = $1 AND timeframe = $2 AND horizon_minutes = $3
                    {scope_clause}
                ORDER BY timestamp DESC
                LIMIT ${4 + len(scope_args)}
                """,
                symbol, timeframe, horizon_minutes, *scope_args, limit,
            )
        return [
            Prediction(
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                timeframe=r["timeframe"],
                horizon_minutes=r["horizon_minutes"],
                model_id=r["model_id"],
                predicted_direction=r["predicted_direction"],
                confidence=r["confidence"],
            )
            for r in rows
        ]
```

- [ ] **Step 6: Run tests**

Run: `cd bullforge && pytest tests/test_backtest_scope.py -v`
Expected: 7 passed.

- [ ] **Step 7: Commit**

```bash
git add bullforge/shared/db.py bullforge/tests/test_backtest_scope.py
git commit -m "feat(db): scope kwarg on predictions methods"
```

---

## Task 4: Thread `scope` through orders DB methods

**Files:**
- Modify: `bullforge/shared/db.py` (`insert_order`, `insert_order_if_new`, `update_order_status`, `query_orders`, `get_order_by_alpaca_id`)
- Modify: `bullforge/tests/test_backtest_scope.py`

All five order methods get the `scope` kwarg per the spec's defense-in-depth
requirement. `OrderExecutor` exercises `insert_order_if_new` and
`get_order_by_alpaca_id` on the backtest path; the other three are live-only
today but gain the `scope=None` NULL-filter for consistency.

- [ ] **Step 1: Write failing test**

Append to `bullforge/tests/test_backtest_scope.py`:

```python
from shared.models import Order


@pytest.mark.asyncio
async def test_insert_order_sandbox_writes_scope():
    settings = Settings()
    live = Database(settings)
    sandbox = Database(settings, sandbox=True)
    await live.connect()
    await sandbox.connect()
    try:
        scope = BacktestScope(backtest_run_id=999_401, fold_index=1)
        order = Order(
            symbol="ORDERTEST",
            side="buy",
            quantity=1.0,
            order_type="market",
            submitted_at=datetime.now(timezone.utc),
            signal_strength=0.5,
            alpaca_order_id="scope-test-1",
        )
        row_id = await sandbox.insert_order_if_new(order, scope=scope)
        assert row_id is not None
        async with live.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT backtest_run_id, fold_index FROM backtest.orders WHERE id=$1",
                row_id,
            )
        assert row["backtest_run_id"] == 999_401
        assert row["fold_index"] == 1
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM backtest.orders WHERE alpaca_order_id='scope-test-1'"
            )
        await sandbox.disconnect()
        await live.disconnect()
```

- [ ] **Step 2: Run to verify failure**

Run: `cd bullforge && pytest tests/test_backtest_scope.py::test_insert_order_sandbox_writes_scope -v`
Expected: FAIL with TypeError about unexpected `scope` kwarg.

- [ ] **Step 3: Update `insert_order`**

Replace (around line 752) with:

```python
    async def insert_order(
        self,
        order: Order,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> int:
        run_id = scope.backtest_run_id if scope else None
        fold = scope.fold_index if scope else None
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO orders
                    (symbol, side, quantity, order_type, submitted_at,
                     signal_strength, alpaca_order_id, status,
                     filled_at, filled_price, filled_quantity, limit_price,
                     origin, last_reconciled_at, reconcile_source,
                     backtest_run_id, fold_index)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        $13, $14, $15, $16, $17)
                RETURNING id
                """,
                order.symbol, order.side, order.quantity, order.order_type,
                order.submitted_at, order.signal_strength,
                order.alpaca_order_id, order.status,
                order.filled_at, order.filled_price, order.filled_quantity,
                order.limit_price, order.origin, order.last_reconciled_at,
                order.reconcile_source,
                run_id, fold,
            )
        return row_id
```

- [ ] **Step 4: Update `insert_order_if_new`**

Replace (around line 783) with:

```python
    async def insert_order_if_new(
        self,
        order: Order,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> Optional[int]:
        """INSERT that does nothing on alpaca_order_id conflict.
        Returns the inserted row id, or None if a row already existed."""
        run_id = scope.backtest_run_id if scope else None
        fold = scope.fold_index if scope else None
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO orders
                  (symbol, side, quantity, order_type, submitted_at,
                   signal_strength, alpaca_order_id, status,
                   origin, last_reconciled_at, reconcile_source,
                   backtest_run_id, fold_index)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,
                        COALESCE($9,'internal'),$10,$11,$12,$13)
                ON CONFLICT (alpaca_order_id) WHERE alpaca_order_id IS NOT NULL DO NOTHING
                RETURNING id
                """,
                order.symbol, order.side, order.quantity, order.order_type,
                order.submitted_at, order.signal_strength,
                order.alpaca_order_id, order.status,
                order.origin, order.last_reconciled_at, order.reconcile_source,
                run_id, fold,
            )
```

- [ ] **Step 5: Update `update_order_status`**

Replace (around line 804) with:

```python
    async def update_order_status(
        self,
        order_id: int,
        status: str,
        filled_at: Optional[datetime] = None,
        filled_price: Optional[float] = None,
        filled_quantity: Optional[float] = None,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> None:
        scope_clause, scope_args = _scope_filter(scope, next_arg=6)
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE orders
                SET status = $2,
                    filled_at = COALESCE($3, filled_at),
                    filled_price = COALESCE($4, filled_price),
                    filled_quantity = COALESCE($5, filled_quantity)
                WHERE id = $1
                    {scope_clause}
                """,
                order_id, status, filled_at, filled_price, filled_quantity,
                *scope_args,
            )
```

- [ ] **Step 6: Update `query_orders`**

Replace (around line 829) with:

```python
    async def query_orders(
        self,
        symbol: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> list[Order]:
        query = """SELECT id, symbol, side, quantity, order_type, submitted_at,
                          signal_strength, alpaca_order_id, status,
                          filled_at, filled_price, filled_quantity, limit_price,
                          origin, last_reconciled_at, reconcile_source
                   FROM orders WHERE TRUE"""
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

        scope_clause, scope_args = _scope_filter(scope, next_arg=idx)
        query += scope_clause
        args.extend(scope_args)
        idx += len(scope_args)

        query += f" ORDER BY submitted_at DESC LIMIT ${idx}"
        args.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [
            Order(
                id=r["id"],
                symbol=r["symbol"],
                side=r["side"],
                quantity=r["quantity"],
                order_type=r["order_type"],
                submitted_at=r["submitted_at"],
                signal_strength=r["signal_strength"],
                alpaca_order_id=r["alpaca_order_id"],
                status=r["status"],
                filled_at=r["filled_at"],
                filled_price=r["filled_price"],
                filled_quantity=r["filled_quantity"],
                limit_price=r["limit_price"],
                origin=r["origin"],
                last_reconciled_at=r["last_reconciled_at"],
                reconcile_source=r["reconcile_source"],
            )
            for r in rows
        ]
```

- [ ] **Step 7: Update `get_order_by_alpaca_id`**

Replace (around line 880) with:

```python
    async def get_order_by_alpaca_id(
        self,
        alpaca_order_id: str,
        *,
        scope: Optional[BacktestScope] = None,
    ) -> Optional[Order]:
        scope_clause, scope_args = _scope_filter(scope, next_arg=2)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT id, symbol, side, quantity, order_type, submitted_at,
                       signal_strength, alpaca_order_id, status,
                       filled_at, filled_price, filled_quantity, limit_price,
                       origin, last_reconciled_at, reconcile_source
                FROM orders
                WHERE alpaca_order_id = $1
                    {scope_clause}
                """,
                alpaca_order_id, *scope_args,
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
```

- [ ] **Step 8: Run tests**

Run: `cd bullforge && pytest tests/test_backtest_scope.py tests/test_order_db.py -v`
Expected: all pass (existing order tests use no scope → NULL-filtering path).

- [ ] **Step 9: Commit**

```bash
git add bullforge/shared/db.py bullforge/tests/test_backtest_scope.py
git commit -m "feat(db): scope kwarg on order methods"
```

---

## Task 5: Scope-aware artifact paths in `ModelStore`

**Files:**
- Modify: `bullforge/services/model_service/model_store.py`
- Modify: `bullforge/tests/test_model_store.py` (add test)

- [ ] **Step 1: Write failing test**

Append to `bullforge/tests/test_model_store.py` (create file if it doesn't exist — but it does, per the file listing):

```python
import tempfile
import pytest

from shared.models import BacktestScope
from services.model_service.model_store import ModelStore


class _Dummy:
    def __init__(self): self.x = 1


def test_save_live_path_is_flat():
    with tempfile.TemporaryDirectory() as tmp:
        store = ModelStore(tmp)
        path = store.save(_Dummy(), "AAPL", "1m", 5, "gradient_boost")
        # Path is directly in the artifacts dir, no nesting.
        assert "/backtest/" not in path.replace("\\", "/")


def test_save_scoped_path_nests_by_run_and_fold():
    with tempfile.TemporaryDirectory() as tmp:
        store = ModelStore(tmp)
        scope = BacktestScope(backtest_run_id=42, fold_index=3)
        path = store.save(
            _Dummy(), "AAPL", "1m", 5, "gradient_boost", scope=scope,
        )
        p = path.replace("\\", "/")
        assert "/backtest/42/fold_3/" in p
```

- [ ] **Step 2: Run to verify failure**

Run: `cd bullforge && pytest tests/test_model_store.py -v -k scope`
Expected: `test_save_scoped_path_nests_by_run_and_fold` FAILs with TypeError about `scope` kwarg.

- [ ] **Step 3: Update `ModelStore.save`**

Replace the `save` method in `bullforge/services/model_service/model_store.py` with:

```python
    def save(self, model: object, symbol: str, timeframe: str,
             horizon_minutes: int, model_type: str,
             *, scope: Optional["BacktestScope"] = None) -> str:
        now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        if isinstance(model, LSTMClassifier):
            ext = "pt"
        else:
            ext = "joblib"
        safe_symbol = symbol.replace("/", "-")
        filename = f"{model_type}_{safe_symbol}_{timeframe}_{horizon_minutes}m_{now}.{ext}"
        if scope is None:
            path = self._dir / filename
        else:
            path = (self._dir / "backtest"
                    / str(scope.backtest_run_id)
                    / f"fold_{scope.fold_index}"
                    / filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(model, LSTMClassifier):
            torch.save(model, path)
        else:
            joblib.dump(model, path)
        logger.info("Saved model artifact to %s", path)
        return str(path)
```

Add to the imports at the top:

```python
from typing import Optional

from shared.models import BacktestScope
```

- [ ] **Step 4: Run tests**

Run: `cd bullforge && pytest tests/test_model_store.py -v`
Expected: all pass (including the 2 new scope tests).

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/model_store.py bullforge/tests/test_model_store.py
git commit -m "feat(model_store): nest backtest artifacts under run/fold dir"
```

---

## Task 6: Thread scope through `Trainer`

**Files:**
- Modify: `bullforge/services/model_service/trainer.py`
- Modify: `bullforge/tests/test_trainer.py` (add test)

- [ ] **Step 1: Write failing test**

Append to `bullforge/tests/test_trainer.py`:

```python
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.models import BacktestScope
from services.model_service.trainer import Trainer


@pytest.mark.asyncio
async def test_trainer_passes_scope_to_db_and_store():
    db = MagicMock()
    db.deactivate_models = AsyncMock()
    db.insert_model_metadata = AsyncMock(return_value=42)
    store = MagicMock()
    store.save = MagicMock(return_value="/tmp/fake.joblib")

    trainer = Trainer(
        db, store,
        scope=BacktestScope(backtest_run_id=7, fold_index=2),
    )
    trainer._dataset_builder = MagicMock()
    trainer._dataset_builder.build = AsyncMock(return_value=None)

    await trainer.train("AAPL", "1m", 5, as_of=None)  # type: ignore[arg-type]

    # No training happened (dataset None), but scope is stored on the instance.
    assert trainer._scope == BacktestScope(backtest_run_id=7, fold_index=2)


@pytest.mark.asyncio
async def test_trainer_live_mode_has_no_scope():
    db = MagicMock()
    store = MagicMock()
    trainer = Trainer(db, store)
    assert trainer._scope is None
```

- [ ] **Step 2: Run to verify failure**

Run: `cd bullforge && pytest tests/test_trainer.py -v -k scope`
Expected: FAIL (Trainer doesn't accept `scope`; no `_scope` attribute).

- [ ] **Step 3: Update `Trainer`**

In `bullforge/services/model_service/trainer.py`, update imports:

```python
from shared.db import Database
from shared.models import BacktestScope, ModelMetadata
```

Update `__init__` to accept `scope`:

```python
    def __init__(
        self,
        db: Database,
        model_store: ModelStore,
        min_samples: int = 500,
        min_samples_1d: int = 84,
        *,
        scope: Optional[BacktestScope] = None,
    ):
        self._db = db
        self._model_store = model_store
        self._scope = scope
        self._dataset_builder = DatasetBuilder(
            db, min_samples=min_samples, min_samples_1d=min_samples_1d,
        )
```

Add `from typing import Optional` import if not already present.

Update the three scope-aware call sites in `train()`:

Replace `artifact_path = self._model_store.save(...)` (around line 73) with:

```python
        artifact_path = self._model_store.save(
            model, symbol, timeframe, horizon_minutes, model_type,
            scope=self._scope,
        )
```

Replace `await self._db.deactivate_models(symbol, timeframe, horizon_minutes)` (around line 80) with:

```python
        await self._db.deactivate_models(
            symbol, timeframe, horizon_minutes, scope=self._scope,
        )
```

Replace `row_id = await self._db.insert_model_metadata(meta)` (around line 96) with:

```python
        row_id = await self._db.insert_model_metadata(meta, scope=self._scope)
```

- [ ] **Step 4: Run tests**

Run: `cd bullforge && pytest tests/test_trainer.py -v`
Expected: all pass (existing tests unchanged because `scope` defaults to None).

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/trainer.py bullforge/tests/test_trainer.py
git commit -m "feat(trainer): accept BacktestScope, thread through DB + store"
```

---

## Task 7: Thread scope through `InferenceEngine`

**Files:**
- Modify: `bullforge/services/model_service/inference.py`
- Modify: `bullforge/tests/test_inference.py` (add test)

- [ ] **Step 1: Write failing test**

Append to `bullforge/tests/test_inference.py`:

```python
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.models import BacktestScope
from services.model_service.inference import InferenceEngine


@pytest.mark.asyncio
async def test_inference_engine_scoped_load_queries_with_scope():
    db = MagicMock()
    db.query_all_active_models = AsyncMock(return_value=[])
    store = MagicMock()
    scope = BacktestScope(backtest_run_id=11, fold_index=4)

    engine = InferenceEngine(db, store, scope=scope)
    await engine.load_models()

    db.query_all_active_models.assert_awaited_once_with(scope=scope)


@pytest.mark.asyncio
async def test_inference_engine_live_load_queries_without_scope():
    db = MagicMock()
    db.query_all_active_models = AsyncMock(return_value=[])
    store = MagicMock()

    engine = InferenceEngine(db, store)
    await engine.load_models()

    db.query_all_active_models.assert_awaited_once_with(scope=None)
```

- [ ] **Step 2: Run to verify failure**

Run: `cd bullforge && pytest tests/test_inference.py -v -k scope`
Expected: FAIL (InferenceEngine doesn't accept `scope`).

- [ ] **Step 3: Update `InferenceEngine` imports + constructor + `load_models` query call**

In `bullforge/services/model_service/inference.py`, replace lines 1-12 (the import block + `logger` line) with:

```python
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd

from shared.db import Database
from shared.models import BacktestScope, ModelMetadata, Prediction
from services.model_service.model_store import ModelStore

logger = logging.getLogger(__name__)
```

Replace the `__init__` method (lines 16-21) with:

```python
    def __init__(
        self,
        db: Database,
        model_store: ModelStore,
        *,
        scope: Optional[BacktestScope] = None,
    ):
        self._db = db
        self._model_store = model_store
        self._scope = scope
        self._models: Dict[
            Tuple[str, str, int], Tuple[object, ModelMetadata]
        ] = {}
```

In `load_models` (line 23+), change only line 24 from:

```python
        active_models = await self._db.query_all_active_models()
```

to:

```python
        active_models = await self._db.query_all_active_models(scope=self._scope)
```

The remaining body of `load_models` and all of `predict` stay as-is.

- [ ] **Step 4: Run tests**

Run: `cd bullforge && pytest tests/test_inference.py -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/inference.py bullforge/tests/test_inference.py
git commit -m "feat(inference): accept BacktestScope for scoped model loading"
```

---

## Task 8: Thread scope through `OrderExecutor`

**Files:**
- Modify: `bullforge/services/execution_service/order_executor.py`

- [ ] **Step 1: Update `OrderExecutor.__init__`**

In `bullforge/services/execution_service/order_executor.py`, update imports:

```python
from typing import Optional

from shared.db import Database
from shared.models import BacktestScope, Order, TradingSignal
```

Replace `__init__` with:

```python
    def __init__(
        self,
        broker: AlpacaBroker,
        risk_manager: RiskManager,
        position_tracker: PositionTracker,
        db: Database,
        core: ReconcilerCore,
        *,
        scope: Optional[BacktestScope] = None,
    ):
        self._broker = broker
        self._risk = risk_manager
        self._tracker = position_tracker
        self._db = db
        self._core = core
        self._scope = scope
```

- [ ] **Step 2: Thread scope into `_execute_open`**

In the `_execute_open` method (around line 74), change:

```python
        order_id = await self._db.insert_order_if_new(order)
```

to:

```python
        order_id = await self._db.insert_order_if_new(order, scope=self._scope)
```

And change (around line 92):

```python
            return await self._db.get_order_by_alpaca_id(alpaca_id)
```

to:

```python
            return await self._db.get_order_by_alpaca_id(alpaca_id, scope=self._scope)
```

- [ ] **Step 3: Thread scope into `_execute_close`**

In `_execute_close` (around line 95), change:

```python
        order_id = await self._db.insert_order_if_new(order)
```

to:

```python
        order_id = await self._db.insert_order_if_new(order, scope=self._scope)
```

And (around line 129):

```python
            return await self._db.get_order_by_alpaca_id(alpaca_id)
```

to:

```python
            return await self._db.get_order_by_alpaca_id(alpaca_id, scope=self._scope)
```

- [ ] **Step 4: Run existing order-executor tests**

Run: `cd bullforge && pytest tests/test_order_executor.py -v`
Expected: all pass (live path uses `scope=None` which is the default).

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/execution_service/order_executor.py
git commit -m "feat(executor): accept BacktestScope, thread to order DB methods"
```

---

## Task 9: Wire scope into `BacktestEngine`

**Files:**
- Modify: `bullforge/services/backtest_service/engine.py`

- [ ] **Step 1: Create scope per fold and pass it everywhere**

In `bullforge/services/backtest_service/engine.py`, update imports:

```python
from shared.db import Database
from shared.models import BacktestScope
from shared.runtime_config import RuntimeConfig
```

In `_run_fold` (currently around line 115), right after `fold_id = await self._results.insert_fold(...)`, add:

```python
        scope = BacktestScope(backtest_run_id=run_id, fold_index=fold.index)
```

Update the trainer instantiation (around line 125):

```python
        trainer = Trainer(self._db, self._model_store, scope=scope)
```

Update the inference engine instantiation (around line 139):

```python
        inference = InferenceEngine(self._db, self._model_store, scope=scope)
```

Update the OrderExecutor instantiation (around line 161):

```python
        executor = OrderExecutor(
            broker=broker, risk_manager=risk, position_tracker=tracker,
            db=self._db, core=_NoopReconciler(),
            scope=scope,
        )
```

- [ ] **Step 2: Run the backtest-engine test suite**

Run: `cd bullforge && pytest tests/test_backtest_engine_overrides.py tests/test_backtest_worker.py tests/test_engine.py -v`
Expected: all pass.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/backtest_service/engine.py
git commit -m "feat(backtest): construct BacktestScope per fold, thread through engine"
```

---

## Task 10: Integration test — concurrent backtests don't cross-contaminate

**Files:**
- Modify: `bullforge/tests/test_backtest_scope.py`

- [ ] **Step 1: Write the integration test**

Append to `bullforge/tests/test_backtest_scope.py`:

```python
import asyncio


@pytest.mark.asyncio
async def test_concurrent_sandbox_writes_do_not_cross_contaminate():
    """
    Simulates two concurrent backtest runs training a model for the same
    (symbol, timeframe, horizon). Each deactivates models within its own
    scope, inserts its own, then reads back. Neither should see the other's
    model as active.
    """
    settings = Settings()
    live = Database(settings)
    sandbox_a = Database(settings, sandbox=True)
    sandbox_b = Database(settings, sandbox=True)
    await live.connect()
    await sandbox_a.connect()
    await sandbox_b.connect()
    try:
        scope_a = BacktestScope(backtest_run_id=999_501, fold_index=0)
        scope_b = BacktestScope(backtest_run_id=999_502, fold_index=0)

        async def run_one(sandbox: Database, scope: BacktestScope, tag: str):
            await sandbox.deactivate_models("RACE", "1m", 5, scope=scope)
            await sandbox.insert_model_metadata(
                _meta(symbol="RACE", artifact_path=f"/tmp/{tag}.joblib"),
                scope=scope,
            )
            await asyncio.sleep(0)  # yield to let the other coroutine run
            models = await sandbox.query_all_active_models(scope=scope)
            return [m.artifact_path for m in models]

        paths_a, paths_b = await asyncio.gather(
            run_one(sandbox_a, scope_a, "A"),
            run_one(sandbox_b, scope_b, "B"),
        )
        assert paths_a == ["/tmp/A.joblib"], paths_a
        assert paths_b == ["/tmp/B.joblib"], paths_b
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM backtest.model_registry WHERE symbol='RACE'"
            )
        await sandbox_a.disconnect()
        await sandbox_b.disconnect()
        await live.disconnect()
```

- [ ] **Step 2: Run the test**

Run: `cd bullforge && pytest tests/test_backtest_scope.py::test_concurrent_sandbox_writes_do_not_cross_contaminate -v`

Expected: PASS. Each run reads back only its own model.

If this test fails before the implementation tasks are done, it's because prior code didn't thread scope; the test is the end-to-end proof of the fix.

- [ ] **Step 3: Full test suite sanity check**

Run: `cd bullforge && pytest tests/ -v`
Expected: all tests pass. No regressions in live-path code (everything defaults to `scope=None`).

- [ ] **Step 4: Commit**

```bash
git add bullforge/tests/test_backtest_scope.py
git commit -m "test(backtest): concurrent runs on same symbol stay isolated"
```

---

## Task 11: Apply to running environment + smoke test

**Files:** none modified — deployment + verification only.

- [ ] **Step 1: Rebuild and restart backtest worker**

On the host running docker-compose:

```bash
cd ~/BullForge/bullforge
git pull
docker compose up -d --build backtest-worker
```

Migration 018 runs at live-pool `connect()` on startup. `backtest.*` tables gain the new columns, the old intermediate rows are truncated, new indexes are created.

- [ ] **Step 2: Run one normal backtest**

Submit a single-symbol backtest via the dashboard. Watch `docker compose logs -f backtest-worker`.

Expected:
- Training completes.
- Model save path shows `.../artifacts/backtest/<run_id>/fold_0/...`.
- No `UndefinedColumnError`.
- Run status `completed` on the dashboard.

- [ ] **Step 3: Run two concurrent backtests on overlapping symbols**

Submit two backtests simultaneously whose symbol sets overlap (both include AAPL, say).

Expected:
- Both runs complete.
- Both produce independent fold/trade/equity data.
- Inspect:
  ```sql
  SELECT backtest_run_id, fold_index, symbol, COUNT(*)
  FROM backtest.model_registry
  GROUP BY 1,2,3
  ORDER BY 1,2,3;
  ```
  Each `(run_id, fold_index, symbol)` combo should have rows belonging to exactly one run.

- [ ] **Step 4: Report back**

- Commits added
- Verification output from Steps 2 and 3
- Any anomalies
