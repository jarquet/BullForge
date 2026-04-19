# Backtest Cancel Feature — Design

**Date:** 2026-04-19
**Status:** Approved (pending user spec review)
**Scope:** Python + SQL + JS. Backtest service, dashboard API, dashboard UI.

## Goal

Let a user cancel a backtest — whether queued or already running — with near-instant effect (sub-second for a running job in bar-replay) from the dashboard, the HTTP API, or a CLI. Partial results from a cancelled run are preserved and distinguishable from completed or failed runs.

## Motivation

Today a backtest runs to completion once a worker claims it. The only way to stop one is `docker kill` on the worker container, which leaves the `backtest_jobs` row in `running` status indefinitely and disrupts every other job that worker was about to pick up. A proper cancel surface is a small feature (~200 LOC Python + 30 LOC JS) that removes a real operational foot-gun.

## Non-Goals

- **Pause/resume.** A cancelled run stays cancelled. Resuming would require checkpointing state the engine doesn't currently persist.
- **Orphan recovery.** If a worker crashes mid-backtest, its job row remains `running` forever — pre-existing issue, separate feature.
- **Cancel-in-progress safety for meta-model training flows.** If/when meta-models train inside backtests, the same poll hooks will work but this spec doesn't add specific hooks inside meta training.

## Design

### 1. Schema

Migration `bullforge/migrations/020_backtest_cancel_status.sql`:

```sql
-- backtest_jobs: extend status enum
ALTER TABLE backtest_jobs DROP CONSTRAINT IF EXISTS backtest_jobs_status_check;
ALTER TABLE backtest_jobs ADD CONSTRAINT backtest_jobs_status_check
    CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled'));

-- backtest_runs: extend status enum so a partially-run run is distinguishable
ALTER TABLE backtest_runs DROP CONSTRAINT IF EXISTS backtest_runs_status_check;
ALTER TABLE backtest_runs ADD CONSTRAINT backtest_runs_status_check
    CHECK (status IN ('running', 'completed', 'failed', 'cancelled'));
```

The implementer should verify the existing CHECK constraint names via `\d backtest_jobs` / `\d backtest_runs` and adjust the `DROP CONSTRAINT` target if different. The migration is idempotent via `IF EXISTS`.

### 2. DB API — `BacktestJobsDB`

Two new methods in `bullforge/services/backtest_service/jobs_db.py`:

```python
async def cancel(self, job_id: int) -> str:
    """Flip a job to 'cancelled' iff currently queued or running.

    CAS semantics: the WHERE clause prevents a race with a worker calling
    `mark_completed`/`mark_failed` at the same instant. Whichever UPDATE runs
    first wins; the other matches zero rows and becomes a no-op.

    Returns:
        'cancelled'         — the state actually flipped to cancelled
        'already_terminal'  — row was completed/failed/cancelled already
    """
    async with self._db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE backtest_jobs
            SET status = 'cancelled', finished_at = NOW()
            WHERE id = $1 AND status IN ('queued', 'running')
            RETURNING id
            """,
            job_id,
        )
    return 'cancelled' if row else 'already_terminal'

async def is_cancelled(self, job_id: int) -> bool:
    """Fast PK lookup used by the engine's in-run cancellation poll."""
    async with self._db.pool.acquire() as conn:
        status = await conn.fetchval(
            "SELECT status FROM backtest_jobs WHERE id = $1", job_id,
        )
    return status == 'cancelled'
```

`mark_completed` and `mark_failed` must also be CAS-guarded so that a cancel that arrives at the exact wrong moment wins deterministically. Current implementations do not guard — adjust them:

```python
# In mark_completed:
WHERE id = $1 AND status IN ('queued', 'running')

# In mark_failed:
WHERE id = $1 AND status IN ('queued', 'running')
```

A worker call to `mark_completed` for a job the user just cancelled will match zero rows and be a no-op. The worker logs "cancelled during run" (handled at the exception layer — see §4).

### 3. Engine — cancellation via injected poll

`bullforge/services/backtest_service/engine.py` gets a new optional `check_cancelled` callable parameter. The engine stays decoupled from `BacktestJobsDB`; the caller (worker) builds the closure.

```python
async def run(
    self,
    cfg: BacktestConfig,
    *,
    on_run_created: Optional[Callable[[int], Awaitable[None]]] = None,
    check_cancelled: Optional[Callable[[], Awaitable[bool]]] = None,
) -> int:
    run_id = await self._results.insert_run(...)
    if on_run_created is not None:
        await on_run_created(run_id)
    try:
        splitter = WalkForwardSplitter(...)
        for fold in splitter.split(cfg.range_start, cfg.range_end):
            logger.info("Fold %d: train [%s..%s] test [%s..%s]", ...)
            await self._run_fold(
                run_id, cfg, fold, check_cancelled=check_cancelled,
            )
        await self._results.finish_run(run_id=run_id, status="completed")
    except BacktestCancelled:
        await self._results.finish_run(run_id=run_id, status="cancelled")
        raise
    except Exception:
        await self._results.finish_run(run_id=run_id, status="failed")
        raise
    return run_id
```

`_run_fold` gets the checker threaded in too, with two check points:

```python
async def _run_fold(
    self, run_id: int, cfg: BacktestConfig, fold: Fold,
    *, check_cancelled: Optional[Callable[[], Awaitable[bool]]] = None,
) -> None:
    # Check point 1: top of fold (before the expensive training phase)
    if check_cancelled and await check_cancelled():
        raise BacktestCancelled()

    fold_id = await self._results.insert_fold(...)
    scope = BacktestScope(backtest_run_id=run_id, fold_index=fold.index)
    # ... training phase (unchanged) ...

    # Check point 2: inside the bar-replay loop.
    for i, (ts, symbol, bar) in enumerate(all_bars):
        if check_cancelled and i % 100 == 0 and await check_cancelled():
            raise BacktestCancelled()
        # ... existing replay body ...
```

**Two hook points, not three.** A per-training hook inside the parallel symbol×horizon gather was considered and dropped: the training phase is fast enough with HistGBC + 10 threads that waiting ~5-30 s for the current fold's trainings to finish before the next poll lands is acceptable, and adding a check inside the gather complicates the closure-scoping with no throughput benefit.

**Poll cost:** one `SELECT status FROM backtest_jobs WHERE id = $1` PK lookup per 100 bars. A 2000-bar fold yields ~20 extra queries. Negligible.

New exception class in a new small module `bullforge/services/backtest_service/exceptions.py`:

```python
class BacktestCancelled(Exception):
    """Raised inside BacktestEngine.run when a cancellation has been requested."""
```

Kept in its own module so neither engine.py nor worker.py depends on the other to import it.

### 4. Worker wiring

`bullforge/services/backtest_service/worker.py::run_one` builds the checker closure from the claimed `job_id`, passes it to the engine, and catches the exception cleanly:

```python
async def run_one(
    jobs: BacktestJobsDB,
    *,
    engine_factory: Callable[[BacktestConfig], BacktestEngine],
    worker_pool: str,
) -> bool:
    job = await jobs.claim_next(worker_pool=worker_pool)
    if job is None:
        return False

    job_id = job["id"]

    async def _check_cancelled() -> bool:
        return await jobs.is_cancelled(job_id)

    try:
        cfg = _build_config({...})
        engine = engine_factory(cfg)

        async def _link(run_id: int) -> None:
            await jobs.link_run(job_id=job_id, run_id=run_id)

        run_id = await engine.run(
            cfg,
            on_run_created=_link,
            check_cancelled=_check_cancelled,
        )
        await jobs.mark_completed(job_id=job_id, run_id=int(run_id))
        logger.info("backtest job %s -> run %s completed", job_id, run_id)
    except BacktestCancelled:
        # Status row was already flipped by the API/CLI; don't touch it.
        logger.info("backtest job %s cancelled during run", job_id)
    except Exception as exc:
        tb = traceback.format_exc()
        logger.exception("backtest job %s failed", job_id)
        await jobs.mark_failed(job_id=job_id, error=f"{exc}\n{tb}")
    return True
```

### 5. Dashboard HTTP API

New endpoint `POST /api/backtest-jobs/{job_id}/cancel` in `bullforge/services/dashboard_service/backtests_api.py`:

```python
async def _cancel_job(request: web.Request) -> web.Response:
    try:
        job_id = int(request.match_info["job_id"])
    except (KeyError, ValueError):
        raise web.HTTPBadRequest(reason="job_id must be an integer")
    jobs = BacktestJobsDB(request.app["db"])
    result = await jobs.cancel(job_id)
    if result == "already_terminal":
        return web.json_response(
            {"error": "job is not queued or running", "job_id": job_id},
            status=409,
        )
    return web.json_response({"job_id": job_id, "status": "cancelled"})
```

Register via:

```python
app.router.add_post("/api/backtest-jobs/{job_id}/cancel", _cancel_job)
```

**Method choice:** `POST` rather than `DELETE` because cancel is not idempotent in the REST sense — it produces distinct outcomes for different input states (200 for "was queued/running", 409 for "already terminal"). A second POST still returns 409 cleanly.

### 6. Dashboard UI

In `bullforge/services/dashboard_service/static/backtests.js`:

**Backtests list (`renderList`):** for rows whose status is `queued` or `running`, append a Cancel button to the status cell (or rightmost cell). On click:

```js
async function cancelJob(jobId) {
    if (!confirm('Cancel backtest ' + jobId + '?')) return;
    const resp = await fetch('/api/backtest-jobs/' + jobId + '/cancel', {
        method: 'POST',
    });
    if (!resp.ok) {
        const body = await resp.json().catch(() => ({}));
        alert('Cancel failed: ' + (body.error || resp.status));
        return;
    }
    await refreshList();  // existing list-refresh helper
}
```

**Run detail page (`renderDetail`):** when the run's status is `running`, show the same Cancel button in the header. When status is `cancelled`, show the status badge with a distinct style (e.g., amber, alongside existing red "failed" and green "completed").

No new CSS framework; reuse existing `.btn` or equivalent button class.

### 7. CLI affordance

New script `bullforge/services/backtest_service/cancel.py`:

```python
"""One-shot cancel entrypoint.

Usage:
    docker compose exec backtest-worker-cpu python -m services.backtest_service.cancel <job_id>

Prints 'cancelled' or 'already_terminal' and exits 0/1 respectively.
"""
from __future__ import annotations

import asyncio
import logging
import sys

from shared.config import Settings
from shared.db import Database
from services.backtest_service.jobs_db import BacktestJobsDB


async def _main(job_id: int) -> int:
    settings = Settings()
    db = Database(settings)
    await db.connect()
    try:
        result = await BacktestJobsDB(db).cancel(job_id)
        print(result)
        return 0 if result == "cancelled" else 1
    finally:
        await db.disconnect()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python -m services.backtest_service.cancel <job_id>",
              file=sys.stderr)
        sys.exit(2)
    try:
        job_id = int(sys.argv[1])
    except ValueError:
        print("job_id must be an integer", file=sys.stderr)
        sys.exit(2)
    logging.basicConfig(level=logging.INFO)
    sys.exit(asyncio.run(_main(job_id)))
```

### 8. Partial results behavior

A cancelled run keeps whatever was persisted up to the cancel moment:
- `backtest_folds` rows for folds completed before cancellation
- `backtest_trades` / `backtest_equity` rows for any bars replayed
- `backtest.model_registry` / `backtest.predictions` / `backtest.orders` rows scoped to the run's `(run_id, fold_index)`

The run row's `status` is set to `'cancelled'` by `engine.run`'s exception handler. The dashboard renders this distinctly from `completed` and `failed`.

No data is deleted on cancellation — aligned with the user's "no ephemeral backtest results" constraint. If a subsequent cleanup pass wants to reclaim that space it's a separate feature.

### 9. Error Handling

- **Invalid `job_id`** (not an integer): HTTP 400 from API; `sys.exit(2)` from CLI.
- **`job_id` does not exist**: `cancel()` returns `'already_terminal'` (no row matched). API returns 409. That's correct behavior from the user's perspective: the job can't be cancelled because it effectively doesn't exist in a cancellable state.
- **Job was just claimed between user click and cancel call**: `cancel()`'s CAS handles it. Status flips from `running` → `cancelled`. Worker's next poll catches it.
- **Worker race with cancellation (completion vs cancel simultaneous)**: §2's CAS on both sides resolves deterministically. Either the cancel wins (status `cancelled`, worker's `mark_completed` is a no-op, worker logs the cancellation) or completion wins (status `completed`, user's `cancel` gets `already_terminal`, API returns 409). Consistent state either way.
- **DB unavailable during poll**: `is_cancelled` raises an asyncpg error; engine treats this as unhandled → falls into generic `except Exception` → run marked `failed`. Acceptable — a DB outage during a backtest is a larger problem than cancellation.

### 10. Testing

**Unit (no DB, mock jobs_db):**
- Worker's `run_one` catches `BacktestCancelled` and does NOT call `mark_failed`.
- Engine's `run()` finishes the run row with status `'cancelled'` when a cancellation is raised.

**DB integration (real Postgres):**
- `BacktestJobsDB.cancel(id)` transitions: queued→cancelled returns `'cancelled'`, running→cancelled returns `'cancelled'`, completed→no-op returns `'already_terminal'`, failed→no-op returns `'already_terminal'`, cancelled→no-op returns `'already_terminal'`, nonexistent_id returns `'already_terminal'`.
- `BacktestJobsDB.mark_completed` / `mark_failed` now no-op when status is already `'cancelled'` (CAS guards).
- Worker's `claim_next(worker_pool="cpu")` does NOT return a cancelled job.

**Engine integration (mock DB, tiny synthetic backtest):**
- Engine's `_run_fold` raises `BacktestCancelled` at the top when the injected `check_cancelled` returns True.
- Engine's bar-replay loop raises `BacktestCancelled` within 100 bars of the flag flipping.
- On `BacktestCancelled`, the run row's status is set to `'cancelled'`.

**Skipped:**
- End-to-end test with a real multi-fold backtest cancellation. Too slow and data-dependent for CI. Manual smoke test during implementation (start a backtest, click Cancel, verify status flips within a few seconds) covers the path.

### 11. Migration / Rollout

1. Merge code + migration 020 to master.
2. On the DGX: `git pull && docker compose up -d --build dashboard_service backtest-worker-cpu backtest-worker-gpu`. The migration runs at live-pool `connect()` on startup.
3. Smoke test: start a small backtest from the dashboard, hit Cancel, watch `docker compose logs -f backtest-worker-cpu` for "cancelled during run". Verify the list shows the run with the new `cancelled` status.

Rollback: revert the commits. The CHECK constraints allow `'cancelled'` values to exist harmlessly in the DB after revert; no application will produce them once the code is gone.

## Risks & Trade-offs

- **100-bar granularity.** If a single bar-replay iteration takes a long time (e.g., many symbols with heavy signal logic), cancel latency could exceed sub-second. For typical 1-5 symbol backtests on 1m bars, 100 iterations ≈ well under a second. Tunable via a constant if needed.
- **Checker coupling.** The engine gains an optional async callable parameter — small surface-area addition but a new way the engine can "know" about external state. Mitigated by injecting a callable (not a `BacktestJobsDB` handle), keeping engine's direct dependencies unchanged.
- **`mark_completed` / `mark_failed` signature unchanged but semantics tighten** (CAS WHERE). Existing callers pass `job_id=...`; nothing breaks. If a future caller assumes these always update the row, they should read the new return value (or the tests will fail). Worth a comment on each method.
- **UI confirm dialog vs immediate click.** A `confirm()` is used in the UI spec above to prevent accidental cancels. Not everyone loves them; replace with a double-click-to-confirm button if preferred. Spec default is `confirm()`.

## Files Changed

| File | Purpose |
|---|---|
| `bullforge/migrations/020_backtest_cancel_status.sql` | Extend status CHECK to include `'cancelled'` on both tables |
| `bullforge/services/backtest_service/exceptions.py` | New module with `BacktestCancelled` |
| `bullforge/services/backtest_service/jobs_db.py` | New `cancel` + `is_cancelled`; CAS guards on `mark_completed` / `mark_failed` |
| `bullforge/services/backtest_service/engine.py` | `check_cancelled` param on `run` and `_run_fold`; two poll hooks; `BacktestCancelled`-aware status dispatch |
| `bullforge/services/backtest_service/worker.py` | Build checker closure; catch `BacktestCancelled` |
| `bullforge/services/backtest_service/cancel.py` | New CLI entry point |
| `bullforge/services/dashboard_service/backtests_api.py` | `POST /api/backtest-jobs/{id}/cancel` |
| `bullforge/services/dashboard_service/static/backtests.js` | Cancel button in list + detail; `cancelled` status styling |
| `bullforge/tests/test_backtest_jobs_db.py` | `cancel()` + CAS-guard transition tests |
| `bullforge/tests/test_backtest_worker.py` | Worker catches `BacktestCancelled` cleanly |
| `bullforge/tests/test_backtest_engine_cancel.py` | New — fold-start + bar-loop poll trips and sets run status to `cancelled` |

**Estimated LOC:** ~200 Python + ~30 JS + ~10 SQL.
