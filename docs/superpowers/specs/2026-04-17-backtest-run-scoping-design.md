# Backtest Run/Fold Scoping — Design

**Date:** 2026-04-17
**Status:** Approved (pending user spec review)
**Scope:** Python + SQL. Backend only. No UI changes.

## Goal

Enable concurrent backtests (and future parallel fold training within a run) without cross-contamination, by giving every row in `backtest.model_registry`, `backtest.predictions`, and `backtest.orders` an explicit `(backtest_run_id, fold_index)` scope. All sandbox reads and writes filter on that scope.

## Motivation

`backtest.model_registry.is_active` is a global flag shared across all concurrent backtests. Trainer calls `deactivate_models(symbol, tf, horizon)` then inserts a new row with `is_active=TRUE`, and `InferenceEngine.load_models()` reads rows `WHERE is_active = TRUE` without any run context. Two backtests that share any `(symbol, tf, horizon)` combo race on the flag: worker A's inference can silently pick up worker B's model and invalidate its results with no error.

User requirements locked for this design:

- Concurrent backtests must be safe.
- Backtest data must be durable (no truncation at run boundaries).
- Maximize DGX utilization → want parallel fold training within a run as a near-term option.
- Live trading must be untouched.

## Non-Goals

- UI changes. The dashboard reads `public.backtest_runs`/`backtest_trades`/`backtest_equity`/`backtest_folds` (already run-scoped). `backtest.*` tables hold intermediate state, never rendered directly.
- Changes to `public.*` write paths. Live trading runs through the same code with `scope=None` and behaves identically to today.
- Cleanup/retention of old backtest rows. Durability was explicit; deletion is a separate feature.
- Parallelizing fold training within a run. This design *enables* it; the scheduler change is out of scope.

## Design

### 1. Schema

Add two columns to each of `model_registry`, `predictions`, `orders` — in **both** schemas so the sandbox-schema auto-sync keeps them aligned with zero drift:

```sql
ALTER TABLE public.model_registry ADD COLUMN backtest_run_id BIGINT NULL;
ALTER TABLE public.model_registry ADD COLUMN fold_index      INTEGER NULL;
-- (same for public.predictions, public.orders)
```

Live rows have both NULL. Backtest rows have both non-NULL.

`backtest.model_registry` gets a new partial uniqueness index:

```sql
CREATE UNIQUE INDEX idx_backtest_model_registry_scoped_active
    ON backtest.model_registry (
        backtest_run_id, fold_index,
        symbol, timeframe, horizon_minutes, model_type
    )
    WHERE is_active = TRUE;
```

The existing `idx_model_registry_active` on `public.model_registry` is untouched: its partial predicate `WHERE is_active = TRUE` continues to apply to live rows only (whose NULL scope columns don't affect its uniqueness check — but more importantly, no backtest rows land in `public.model_registry`).

Lookup indexes on the sandbox side for the hot-path filter:

```sql
CREATE INDEX ON backtest.model_registry (backtest_run_id, fold_index);
CREATE INDEX ON backtest.predictions    (backtest_run_id, fold_index);
CREATE INDEX ON backtest.orders         (backtest_run_id, fold_index);
```

**Existing sandbox data is TRUNCATEd in the migration.** The three tables hold intermediate run data. User-visible results are in the separate `public.backtest_trades`, `public.backtest_equity`, `public.backtest_folds` tables (already `run_id`-scoped) which are NOT touched by this migration.

### 2. Python types

A small value type that flows through the engine → trainer → inference → db layers:

```python
# bullforge/shared/models.py
@dataclass(frozen=True)
class BacktestScope:
    backtest_run_id: int
    fold_index: int
```

Threaded as an optional kwarg (`scope: BacktestScope | None = None`) on every affected method. `None` = live. Set = backtest.

### 3. Code flow

Current (broken) flow, with the change in brackets:

```
BacktestEngine.run(cfg)
  run_id = results_db.insert_run(...)                 # already exists
  for fold in splitter.split(...):
    _run_fold(run_id, cfg, fold)                       # [pass scope down]
      scope = BacktestScope(run_id, fold.index)        # [new]
      trainer = Trainer(db, store, scope=scope)        # [new kwarg]
      for (symbol, horizon):
        trainer.train(symbol, tf, horizon, as_of=...)
          # deactivate_models(sym, tf, h, scope=scope)  [scoped]
          # insert_model_metadata(meta, scope=scope)    [scoped]
          # store.save(model, ..., scope=scope)         [nested path]
      inference = InferenceEngine(db, store, scope=scope)  # [new kwarg]
      inference.load_models()
        # query_all_active_models(scope=scope)          [scoped]
      executor = OrderExecutor(db=db, scope=scope, ...) # [new kwarg]
      # all order/prediction inserts carry the scope
```

### 4. Database API

Extend existing methods rather than adding parallel `_scoped` variants — one `INSERT` resolves to the right schema via `search_path`:

- `insert_model_metadata(meta, *, scope=None)` — fills `backtest_run_id`/`fold_index` when set; NULL otherwise.
- `deactivate_models(symbol, tf, horizon, *, scope=None)` — adds `AND backtest_run_id = $X AND fold_index = $Y` when scope set; unchanged WHERE when unset.
- `query_active_model(symbol, tf, horizon, *, scope=None)` and `query_all_active_models(*, scope=None)` — scope-aware filters.
- `insert_prediction` / `insert_predictions` / `query_predictions` — `scope` kwarg.
- `insert_order` / `update_order_status` / `query_orders` / `get_order_by_alpaca_id` — `scope` kwarg.

When `scope is None`:
- Inserts use `NULL` for both scope columns.
- Reads add `AND backtest_run_id IS NULL AND fold_index IS NULL` to their WHERE clauses. This is the critical guard: *live reads must not see sandbox rows even if search_path resolution somehow led there.*

When `scope` is set:
- Inserts use the scope values.
- Reads add `AND backtest_run_id = $X AND fold_index = $Y`.

### 5. Model artifact paths

```python
# services/model_service/model_store.py
def save(self, model, symbol, tf, horizon, model_type, *, scope=None) -> str:
    filename = f"{model_type}_{safe_symbol}_{tf}_{horizon}m_{now}.{ext}"
    if scope is None:
        path = self._dir / filename
    else:
        path = (self._dir / "backtest" /
                str(scope.backtest_run_id) /
                f"fold_{scope.fold_index}" /
                filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    ...
```

`load()` takes the stored absolute path from the DB row — no changes needed.

### 6. Migration

Single file `bullforge/migrations/018_backtest_run_scoping.sql`:

```sql
-- Add scope columns to public. These stay NULL on live rows; the sandbox
-- tables track them via the schema sync, but we ALTER backtest.* explicitly
-- below too so that the indexes later in this migration can reference them.
ALTER TABLE public.model_registry ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE public.model_registry ADD COLUMN IF NOT EXISTS fold_index INTEGER;
ALTER TABLE public.predictions    ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE public.predictions    ADD COLUMN IF NOT EXISTS fold_index INTEGER;
ALTER TABLE public.orders         ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE public.orders         ADD COLUMN IF NOT EXISTS fold_index INTEGER;

-- ALTER the sandbox mirror tables explicitly. Needed BEFORE the index
-- statements below (which reference these columns); the sandbox-schema sync
-- would also add them on the next live-pool connect, but that runs AFTER
-- all migrations, so we must add them here.
ALTER TABLE backtest.model_registry ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE backtest.model_registry ADD COLUMN IF NOT EXISTS fold_index INTEGER;
ALTER TABLE backtest.predictions    ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE backtest.predictions    ADD COLUMN IF NOT EXISTS fold_index INTEGER;
ALTER TABLE backtest.orders         ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE backtest.orders         ADD COLUMN IF NOT EXISTS fold_index INTEGER;

-- Wipe intermediate backtest data. User-visible results in public.backtest_trades/
-- backtest_equity/backtest_folds are NOT touched.
TRUNCATE backtest.model_registry, backtest.predictions, backtest.orders;

-- Sandbox-only indexes (these exist ONLY in backtest, not public).
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

On subsequent live-pool `connect()`s, the sandbox-schema sync (shipped in commit `5cbc4f1`) sees the columns already present in both schemas and does nothing — the migration was authoritative.

### 7. Error Handling

- Scope mismatch between writer and reader is a programming error. Methods use explicit kwargs; misuse shows up in type-checking or loud runtime errors, not silent wrong data.
- `scope=None` reads on `backtest.*` schema (via an accidentally-sandbox connection) filter on `backtest_run_id IS NULL`, which matches no sandbox rows (they're all non-NULL). Live code on a live connection works as before.
- Concurrent `deactivate_models(..., scope=S)` from two workers on the same scope is impossible — a scope is owned by exactly one `(run_id, fold_index)`, which belongs to exactly one worker at a time.
- Failed backtests leave stale rows. That's fine (durability was a requirement); a future "delete run" feature can cascade-clean.

### 8. Testing

**Unit (no DB):**
- `BacktestScope` equality, hashing, immutability.
- Trainer passes the scope into `insert_model_metadata` and `deactivate_models` (mock DB; assert call args).
- InferenceEngine passes the scope into `query_all_active_models` (mock DB).
- ModelStore path construction: live → flat, scope → `backtest/<run>/fold_<i>/...`.
- Order executor passes scope into `insert_order`.

**Integration (real Postgres):**
- Two concurrent backtests on overlapping `(symbol, tf, horizon)` both succeed; each sees only its own model in inference.
- Sequential backtests with overlapping combos still work.
- Live inference on `public.model_registry` still works; NULL-scope path verified.
- Migration is idempotent (safe to re-run after sync has already added the columns to backtest).
- Sandbox-schema sync continues to work: drop the new columns from `backtest.*`, reconnect, verify they are re-added.

### 9. Rollout

1. Merge migration 018 + code change.
2. Restart backtest worker → `connect()` runs migration → sync propagates columns to backtest → sandbox tables TRUNCATEd.
3. Run one backtest, confirm normal completion with non-NULL scope columns populated.
4. Run two concurrent backtests on overlapping symbols; verify both complete with distinct models.

Rollback: revert the code. The schema columns are nullable and harmless if unused. Old rows (post-migration) have the scope populated; new rows under old code would have NULL, which is technically drift but doesn't corrupt anything.

## Risks & Trade-offs

- **Backward-compatibility sentinel.** Three tables now have `backtest_run_id IS NULL` meaning "live" and non-NULL meaning "backtest". This convention is load-bearing — any future query forgetting the NULL filter on a live read path could leak sandbox rows. Mitigation: `scope=None` path in every Database method explicitly adds `AND backtest_run_id IS NULL`. One place to get right, not per-call-site discipline.
- **TRUNCATE loses any in-flight backtest sandbox state.** Acceptable: the migration runs at service startup before any worker processes a new job. If the live pool connects while a worker is mid-write, that worker would already be affected by service restart (process kill). No additional data loss.
- **Scope columns added to `public.*` for tables where they'll always be NULL.** Minor storage cost (bigint + int per row, nullable → one byte each in null bitmap when NULL). Trade for keeping one INSERT statement across both schemas. Preferred over bifurcating DB methods into live and backtest variants.

## Files Changed

| File | Purpose |
|---|---|
| `bullforge/migrations/018_backtest_run_scoping.sql` | Schema migration |
| `bullforge/shared/models.py` | `BacktestScope` dataclass |
| `bullforge/shared/db.py` | Add `scope` kwarg to ~10 methods |
| `bullforge/services/model_service/trainer.py` | Accept + thread `scope` |
| `bullforge/services/model_service/inference.py` | Accept + thread `scope` |
| `bullforge/services/model_service/model_store.py` | Scope-aware artifact paths |
| `bullforge/services/backtest_service/engine.py` | Create scope per fold, pass down |
| `bullforge/services/execution_service/order_executor.py` | Accept + thread `scope` |
| `bullforge/tests/test_backtest_scope.py` | New test file (unit + integration) |
| existing `tests/test_trainer.py`, `test_inference.py`, etc. | Updates to cover new kwargs |
