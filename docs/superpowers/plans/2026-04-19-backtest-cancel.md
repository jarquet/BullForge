# Backtest Cancel Feature Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let users cancel backtests — queued or running — from the dashboard, HTTP API, or CLI, with sub-second latency during bar-replay. Partial results are preserved and distinguishable from completed/failed runs.

**Architecture:** Add `'cancelled'` to the CHECK constraints on `backtest_jobs.status` and `backtest_runs.status`. Add `BacktestJobsDB.cancel()` (CAS `WHERE status IN ('queued','running')`) and `is_cancelled()` (fast PK lookup). Inject an optional `check_cancelled` async callable into `BacktestEngine.run()`; poll at the top of each fold and every 100 bars in the replay loop; raise `BacktestCancelled` on true. Worker builds the closure from the claimed `job_id` and catches the exception cleanly. Dashboard exposes POST endpoints (cancel by job_id and cancel by run_id) and wires a Cancel button into list rows + run detail page.

**Tech Stack:** Python 3.12, asyncpg, aiohttp, Postgres/TimescaleDB, vanilla JS.

**Spec:** [docs/superpowers/specs/2026-04-19-backtest-cancel-design.md](../specs/2026-04-19-backtest-cancel-design.md)

---

## Pre-Task: Worktree

The execution skill creates `.worktrees/backtest-cancel` branched from master as `feat/backtest-cancel`. All paths below are relative to that worktree.

---

## Task 1: Schema migration + `BacktestCancelled` exception

**Files:**
- Create: `bullforge/migrations/020_backtest_cancel_status.sql`
- Create: `bullforge/services/backtest_service/exceptions.py`

- [ ] **Step 1: Write migration 020**

Create `bullforge/migrations/020_backtest_cancel_status.sql` with EXACTLY:

```sql
-- 020_backtest_cancel_status.sql
--
-- Extends the status CHECK constraint on backtest_jobs and backtest_runs
-- to include 'cancelled'. backtest_jobs rows flip to 'cancelled' when a
-- user cancels via the dashboard/API/CLI; backtest_runs rows flip to
-- 'cancelled' when the engine catches a BacktestCancelled mid-run and
-- returns a distinct status so partial results can be rendered
-- differently from 'completed' or 'failed'.
--
-- Existing data is unchanged. Constraint names follow Postgres's default
-- <table>_<col>_check convention; both already exist as inline CHECKs in
-- migrations 013 (jobs) and 012 (runs).

ALTER TABLE backtest_jobs DROP CONSTRAINT IF EXISTS backtest_jobs_status_check;
ALTER TABLE backtest_jobs ADD CONSTRAINT backtest_jobs_status_check
    CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled'));

ALTER TABLE backtest_runs DROP CONSTRAINT IF EXISTS backtest_runs_status_check;
ALTER TABLE backtest_runs ADD CONSTRAINT backtest_runs_status_check
    CHECK (status IN ('running', 'completed', 'failed', 'cancelled'));
```

- [ ] **Step 2: Write exceptions module**

Create `bullforge/services/backtest_service/exceptions.py` with EXACTLY:

```python
"""Custom exceptions for the backtest service."""
from __future__ import annotations


class BacktestCancelled(Exception):
    """Raised inside BacktestEngine.run when a cancellation has been requested.

    The engine catches this, marks the backtest_runs row as status='cancelled',
    then re-raises so the worker can skip `mark_completed` / `mark_failed`
    (the backtest_jobs row was already flipped to 'cancelled' by whoever
    requested cancellation).
    """
```

- [ ] **Step 3: Syntax check**

```bash
cd <repo>/.worktrees/backtest-cancel/bullforge
python -c "import ast; ast.parse(open('services/backtest_service/exceptions.py').read()); print('OK')"
```

Expected: `OK`.

- [ ] **Step 4: Commit**

```bash
git add bullforge/migrations/020_backtest_cancel_status.sql \
        bullforge/services/backtest_service/exceptions.py
git commit -m "feat(backtest): extend status enums + BacktestCancelled exception"
```

---

## Task 2: `BacktestJobsDB.cancel` + `is_cancelled` + CAS guards

**Files:**
- Modify: `bullforge/services/backtest_service/jobs_db.py`
- Modify: `bullforge/tests/test_backtest_jobs_db.py`

### Step 1: Write failing tests

Append to the END of `bullforge/tests/test_backtest_jobs_db.py`:

```python
async def test_cancel_queued_job_returns_cancelled(db):
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(name="qcancel", config={})
    result = await jobs.cancel(job_id)
    assert result == "cancelled"
    row = await jobs.get(job_id)
    assert row["status"] == "cancelled"
    assert row["finished_at"] is not None


async def test_cancel_running_job_returns_cancelled(db):
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(name="rcancel", config={})
    await jobs.claim_next(worker_pool="cpu")
    result = await jobs.cancel(job_id)
    assert result == "cancelled"
    row = await jobs.get(job_id)
    assert row["status"] == "cancelled"


async def test_cancel_completed_job_returns_already_terminal(db):
    jobs = BacktestJobsDB(db)
    run_id = await _seed_run(db)
    job_id = await jobs.insert_job(name="ccompl", config={})
    await jobs.claim_next(worker_pool="cpu")
    await jobs.mark_completed(job_id=job_id, run_id=run_id)
    result = await jobs.cancel(job_id)
    assert result == "already_terminal"
    row = await jobs.get(job_id)
    assert row["status"] == "completed"


async def test_cancel_nonexistent_job_returns_already_terminal(db):
    jobs = BacktestJobsDB(db)
    result = await jobs.cancel(999_999_999)
    assert result == "already_terminal"


async def test_is_cancelled_flag(db):
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(name="flag", config={})
    assert await jobs.is_cancelled(job_id) is False
    await jobs.cancel(job_id)
    assert await jobs.is_cancelled(job_id) is True


async def test_mark_completed_is_noop_when_already_cancelled(db):
    """Race guard: if user cancelled just before worker finished,
    mark_completed must not flip the status back to 'completed'."""
    jobs = BacktestJobsDB(db)
    run_id = await _seed_run(db)
    job_id = await jobs.insert_job(name="race1", config={})
    await jobs.claim_next(worker_pool="cpu")
    await jobs.cancel(job_id)
    await jobs.mark_completed(job_id=job_id, run_id=run_id)
    row = await jobs.get(job_id)
    assert row["status"] == "cancelled"


async def test_mark_failed_is_noop_when_already_cancelled(db):
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(name="race2", config={})
    await jobs.claim_next(worker_pool="cpu")
    await jobs.cancel(job_id)
    await jobs.mark_failed(job_id=job_id, error="boom")
    row = await jobs.get(job_id)
    assert row["status"] == "cancelled"
```

### Step 2: Run to verify failure

```bash
python -m pytest tests/test_backtest_jobs_db.py -v
```

Expected: 7 new tests FAIL (AttributeError: no `cancel` / `is_cancelled`, or the race tests fail because `mark_completed`/`mark_failed` currently overwrite unconditionally).

### Step 3: Add `cancel` method

In `bullforge/services/backtest_service/jobs_db.py`, add these two new methods to the `BacktestJobsDB` class (insert anywhere among the existing methods; a good spot is right after `claim_next`):

```python
    async def cancel(self, job_id: int) -> str:
        """Flip a job to 'cancelled' iff currently queued or running.

        CAS semantics: the `WHERE status IN (...)` clause prevents a race
        with a worker's mark_completed/mark_failed running at the same
        instant. Whichever UPDATE runs first wins; the other matches zero
        rows and becomes a no-op.

        Returns:
            'cancelled'         — the row actually flipped to cancelled
            'already_terminal'  — row was completed/failed/cancelled, or
                                  no such job_id exists
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
        return "cancelled" if row else "already_terminal"

    async def is_cancelled(self, job_id: int) -> bool:
        """Fast PK lookup used by the engine's in-run cancellation poll."""
        async with self._db.pool.acquire() as conn:
            status = await conn.fetchval(
                "SELECT status FROM backtest_jobs WHERE id = $1", job_id,
            )
        return status == "cancelled"
```

### Step 4: CAS-guard `mark_completed`

Replace the existing `mark_completed` method with:

```python
    async def mark_completed(self, *, job_id: int, run_id: int) -> None:
        """No-op if the row is already terminal (e.g., cancelled mid-run)."""
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE backtest_jobs
                SET status = 'completed', run_id = $2, finished_at = NOW()
                WHERE id = $1 AND status IN ('queued', 'running')
                """,
                job_id, run_id,
            )
```

### Step 5: CAS-guard `mark_failed`

Replace the existing `mark_failed` method with:

```python
    async def mark_failed(self, *, job_id: int, error: str) -> None:
        """No-op if the row is already terminal (e.g., cancelled mid-run)."""
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE backtest_jobs
                SET status = 'failed', error = $2, finished_at = NOW()
                WHERE id = $1 AND status IN ('queued', 'running')
                """,
                job_id, error,
            )
```

### Step 6: Run tests

```bash
python -m pytest tests/test_backtest_jobs_db.py -v
```

Expected: all tests pass (existing + 7 new). If any existing test that calls `mark_completed` / `mark_failed` on an already-terminal row was relying on the unconditional overwrite, update it — but the existing suite doesn't have such tests, so no changes needed.

### Step 7: Commit

```bash
git add bullforge/services/backtest_service/jobs_db.py \
        bullforge/tests/test_backtest_jobs_db.py
git commit -m "feat(backtest): BacktestJobsDB.cancel/is_cancelled + CAS guards"
```

---

## Task 3: Engine — `check_cancelled` param + two poll hooks

**Files:**
- Modify: `bullforge/services/backtest_service/engine.py`
- Create: `bullforge/tests/test_backtest_engine_cancel.py`

### Step 1: Write failing tests

Create `bullforge/tests/test_backtest_engine_cancel.py` with EXACTLY:

```python
"""Tests that BacktestEngine honors the check_cancelled callback at the
two poll points: start of each fold, and every 100 bars in the replay loop."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.backtest_service.engine import BacktestConfig, BacktestEngine
from services.backtest_service.exceptions import BacktestCancelled
from services.backtest_service.splitter import Fold


def _cfg() -> BacktestConfig:
    return BacktestConfig(
        name="cancel-test",
        symbols=["AAA"],
        timeframe="1m",
        horizons=[5],
        range_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        range_end=datetime(2026, 1, 10, tzinfo=timezone.utc),
        train_days=3, test_days=1, embargo_minutes=0, mode="rolling",
    )


def _fold() -> Fold:
    return Fold(
        index=0,
        train_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        train_end=datetime(2026, 1, 4, tzinfo=timezone.utc),
        test_start=datetime(2026, 1, 4, tzinfo=timezone.utc),
        test_end=datetime(2026, 1, 5, tzinfo=timezone.utc),
    )


def _make_engine() -> BacktestEngine:
    results = MagicMock()
    results.insert_run = AsyncMock(return_value=1)
    results.insert_fold = AsyncMock(return_value=1)
    results.finish_run = AsyncMock()
    return BacktestEngine(
        db=MagicMock(),
        runtime_config=MagicMock(),
        model_store=MagicMock(),
        results_db=results,
    )


@pytest.mark.asyncio
async def test_run_fold_raises_on_fold_start_cancellation():
    engine = _make_engine()
    check = AsyncMock(return_value=True)
    with pytest.raises(BacktestCancelled):
        await engine._run_fold(
            run_id=1, cfg=_cfg(), fold=_fold(), check_cancelled=check,
        )
    check.assert_awaited()


@pytest.mark.asyncio
async def test_run_marks_run_cancelled_on_cancellation():
    engine = _make_engine()
    check = AsyncMock(return_value=True)
    # Patch _run_fold to raise immediately on first call.
    engine._run_fold = AsyncMock(side_effect=BacktestCancelled())
    with pytest.raises(BacktestCancelled):
        await engine.run(_cfg(), check_cancelled=check)
    engine._results.finish_run.assert_awaited_once_with(
        run_id=1, status="cancelled",
    )


@pytest.mark.asyncio
async def test_run_marks_run_failed_on_generic_exception():
    """Regression: the cancel handling doesn't swallow other exceptions."""
    engine = _make_engine()
    engine._run_fold = AsyncMock(side_effect=RuntimeError("boom"))
    with pytest.raises(RuntimeError):
        await engine.run(_cfg())
    engine._results.finish_run.assert_awaited_once_with(
        run_id=1, status="failed",
    )


@pytest.mark.asyncio
async def test_run_no_checker_does_not_call_it():
    """When check_cancelled is None, the engine must not crash on the poll."""
    engine = _make_engine()
    engine._run_fold = AsyncMock(return_value=None)
    # Pretend the splitter yielded one fold.
    from services.backtest_service import engine as engine_mod
    engine_mod.WalkForwardSplitter = MagicMock(
        return_value=MagicMock(split=MagicMock(return_value=[_fold()])),
    )
    run_id = await engine.run(_cfg())
    assert run_id == 1
    engine._results.finish_run.assert_awaited_once_with(
        run_id=1, status="completed",
    )
```

### Step 2: Verify failure

```bash
python -m pytest tests/test_backtest_engine_cancel.py -v
```

Expected: all 4 tests FAIL (ImportError: `BacktestCancelled`, TypeError: `check_cancelled` kwarg unexpected, or `finish_run` not called with `status='cancelled'`).

### Step 3: Update engine imports

In `bullforge/services/backtest_service/engine.py`, update the imports block. Find the existing imports (top of the file, ~lines 1-33) and insert this line after `from services.backtest_service.splitter import Fold, WalkForwardSplitter`:

```python
from services.backtest_service.exceptions import BacktestCancelled
```

### Step 4: Update `run()` signature + exception dispatch

Replace the entire `async def run(...)` method (currently at line 75-114) with:

```python
    async def run(
        self,
        cfg: BacktestConfig,
        *,
        on_run_created: Optional[Callable[[int], Awaitable[None]]] = None,
        check_cancelled: Optional[Callable[[], Awaitable[bool]]] = None,
    ) -> int:
        run_id = await self._results.insert_run(
            name=cfg.name,
            custom_name=cfg.custom_name,
            symbols=cfg.symbols, timeframe=cfg.timeframe, horizons=cfg.horizons,
            range_start=cfg.range_start, range_end=cfg.range_end,
            train_days=cfg.train_days, test_days=cfg.test_days,
            embargo_minutes=cfg.embargo_minutes, mode=cfg.mode,
            config={
                "starting_cash": cfg.starting_cash,
                "slippage_bps": cfg.slippage_bps,
                "max_volume_pct": cfg.max_volume_pct,
            },
        )
        if on_run_created is not None:
            await on_run_created(run_id)
        try:
            splitter = WalkForwardSplitter(
                train_days=cfg.train_days,
                test_days=cfg.test_days,
                embargo_minutes=cfg.embargo_minutes,
                mode=cfg.mode,
            )
            for fold in splitter.split(cfg.range_start, cfg.range_end):
                logger.info(
                    "Fold %d: train [%s..%s] test [%s..%s]",
                    fold.index, fold.train_start, fold.train_end,
                    fold.test_start, fold.test_end,
                )
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

### Step 5: Update `_run_fold` signature + add fold-start poll

Replace the `_run_fold` method signature (currently at line 116-118) and add the fold-start poll as the very first statement:

```python
    async def _run_fold(
        self, run_id: int, cfg: BacktestConfig, fold: Fold,
        *,
        check_cancelled: Optional[Callable[[], Awaitable[bool]]] = None,
    ) -> None:
        if check_cancelled is not None and await check_cancelled():
            raise BacktestCancelled()

        fold_id = await self._results.insert_fold(
            run_id=run_id, fold_index=fold.index,
            train_start=fold.train_start, train_end=fold.train_end,
            test_start=fold.test_start, test_end=fold.test_end,
        )
```

(Leave the body of `_run_fold` from `scope = BacktestScope(...)` onward unchanged except for the loop in Step 6.)

### Step 6: Add bar-replay poll

In `_run_fold`, find the bar-replay loop (currently at line 195: `for ts, symbol, bar in all_bars:`). Replace the loop header with:

```python
        for i, (ts, symbol, bar) in enumerate(all_bars):
            if check_cancelled is not None and i % 100 == 0 and await check_cancelled():
                raise BacktestCancelled()
            if last_ts is not None and ts != last_ts and last_account is not None:
                equity.record(
                    last_ts,
                    equity=last_account["equity"],
                    cash=last_account["cash"],
                )
```

The rest of the loop body (from `broker.set_clock(ts)` onward) stays unchanged.

### Step 7: Run tests

```bash
python -m pytest tests/test_backtest_engine_cancel.py tests/test_engine.py tests/test_backtest_engine_overrides.py -v
```

Expected: all new tests pass + pre-existing engine tests continue to pass (since `check_cancelled=None` path is a no-op).

### Step 8: Commit

```bash
git add bullforge/services/backtest_service/engine.py \
        bullforge/tests/test_backtest_engine_cancel.py
git commit -m "feat(backtest): BacktestEngine poll check_cancelled at fold start + every 100 bars"
```

---

## Task 4: Worker — build checker closure + catch `BacktestCancelled`

**Files:**
- Modify: `bullforge/services/backtest_service/worker.py`
- Modify: `bullforge/tests/test_backtest_worker.py`

### Step 1: Write failing test

Append to the END of `bullforge/tests/test_backtest_worker.py`:

```python
from services.backtest_service.exceptions import BacktestCancelled


@pytest.mark.asyncio
async def test_run_one_catches_backtest_cancelled_without_marking_failed():
    """When the engine raises BacktestCancelled, the worker must log and
    return without calling mark_failed (the jobs row is already cancelled)."""
    from unittest.mock import AsyncMock, MagicMock

    class _FakeJobs:
        def __init__(self):
            self.claim_next = AsyncMock(return_value={
                "id": 42,
                "name": "cancelled-job",
                "custom_name": None,
                "config": {
                    "symbols": ["AAA"],
                    "timeframe": "1m",
                    "horizons": [5],
                    "range_start": "2026-01-01T00:00:00+00:00",
                    "range_end": "2026-01-10T00:00:00+00:00",
                    "train_days": 3, "test_days": 1, "embargo_min": 0,
                    "mode": "rolling",
                },
            })
            self.link_run = AsyncMock()
            self.mark_completed = AsyncMock()
            self.mark_failed = AsyncMock()
            self.is_cancelled = AsyncMock(return_value=True)

    jobs = _FakeJobs()

    engine = MagicMock()
    engine.run = AsyncMock(side_effect=BacktestCancelled())

    from services.backtest_service.worker import run_one
    did = await run_one(
        jobs, engine_factory=lambda _cfg: engine, worker_pool="cpu",
    )
    assert did is True
    jobs.mark_failed.assert_not_called()
    jobs.mark_completed.assert_not_called()
```

### Step 2: Verify failure

```bash
python -m pytest tests/test_backtest_worker.py -v -k cancelled
```

Expected: the new test FAILS (worker's current `except Exception` catches `BacktestCancelled` and calls `mark_failed`).

### Step 3: Update worker imports

In `bullforge/services/backtest_service/worker.py`, update the imports at the top to include the exception. Find this existing line:

```python
from services.backtest_service.jobs_db import BacktestJobsDB
```

And add immediately after it:

```python
from services.backtest_service.exceptions import BacktestCancelled
```

### Step 4: Update `run_one` body

Replace the entire `run_one` function (currently lines 49-79) with:

```python
async def run_one(
    jobs: BacktestJobsDB,
    *,
    engine_factory: Callable[[BacktestConfig], BacktestEngine],
    worker_pool: str,
) -> bool:
    """Claim and run exactly one job from `worker_pool`. Returns True iff a job was processed."""
    job = await jobs.claim_next(worker_pool=worker_pool)
    if job is None:
        return False

    job_id = job["id"]

    async def _check_cancelled() -> bool:
        return await jobs.is_cancelled(job_id)

    try:
        cfg = _build_config({
            "name": job["name"],
            "custom_name": job.get("custom_name"),
            **job["config"],
        })
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
        # backtest_jobs row was already flipped to 'cancelled' by the API/CLI;
        # backtest_runs row was marked 'cancelled' by engine.run.
        logger.info("backtest job %s cancelled during run", job_id)
    except Exception as exc:
        tb = traceback.format_exc()
        logger.exception("backtest job %s failed", job_id)
        await jobs.mark_failed(job_id=job_id, error=f"{exc}\n{tb}")
    return True
```

### Step 5: Run tests

```bash
python -m pytest tests/test_backtest_worker.py -v
```

Expected: all pass (existing 5 tests + 1 new).

### Step 6: Commit

```bash
git add bullforge/services/backtest_service/worker.py \
        bullforge/tests/test_backtest_worker.py
git commit -m "feat(backtest): worker builds check_cancelled closure + catches BacktestCancelled"
```

---

## Task 5: Dashboard HTTP API — two cancel endpoints

Two endpoints are needed because the list + detail views expose different id types:
- `POST /api/backtest-jobs/{job_id}/cancel` — direct cancel by job_id (used for "job" kind rows)
- `POST /api/backtests/{run_id}/cancel` — cancel by run_id (looks up the linked job, cancels it; used for "run" kind rows and the run detail page)

**Files:**
- Modify: `bullforge/services/dashboard_service/backtests_api.py`
- Modify: `bullforge/tests/test_dashboard_backtests_api.py`

### Step 1: Write failing tests

Append to the END of `bullforge/tests/test_dashboard_backtests_api.py`:

```python
async def test_cancel_job_by_job_id_returns_200(db, http_client):
    """POST /api/backtest-jobs/{job_id}/cancel on a queued job returns 200."""
    from services.backtest_service.jobs_db import BacktestJobsDB
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(name="api-cancel-1", config={})

    resp = await http_client.post(f"/api/backtest-jobs/{job_id}/cancel")
    assert resp.status == 200
    body = await resp.json()
    assert body["job_id"] == job_id
    assert body["status"] == "cancelled"

    row = await jobs.get(job_id)
    assert row["status"] == "cancelled"


async def test_cancel_job_already_terminal_returns_409(db, http_client):
    from services.backtest_service.jobs_db import BacktestJobsDB
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(name="api-cancel-2", config={})
    await jobs.cancel(job_id)

    resp = await http_client.post(f"/api/backtest-jobs/{job_id}/cancel")
    assert resp.status == 409


async def test_cancel_job_by_run_id_finds_and_cancels(db, http_client):
    """POST /api/backtests/{run_id}/cancel looks up the job and cancels it."""
    from services.backtest_service.jobs_db import BacktestJobsDB
    from services.backtest_service.results_db import BacktestResultsDB
    jobs = BacktestJobsDB(db)
    results = BacktestResultsDB(db)

    from datetime import datetime, timezone
    run_id = await results.insert_run(
        name="api-cancel-3", symbols=["AAA"], timeframe="1m", horizons=[5],
        range_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        range_end=datetime(2026, 1, 10, tzinfo=timezone.utc),
        train_days=3, test_days=1, embargo_minutes=0, mode="rolling",
        config={},
    )
    job_id = await jobs.insert_job(name="api-cancel-3", config={})
    await jobs.claim_next(worker_pool="cpu")
    await jobs.link_run(job_id=job_id, run_id=run_id)

    resp = await http_client.post(f"/api/backtests/{run_id}/cancel")
    assert resp.status == 200
    row = await jobs.get(job_id)
    assert row["status"] == "cancelled"


async def test_cancel_by_run_id_nonexistent_returns_404(db, http_client):
    resp = await http_client.post("/api/backtests/999999/cancel")
    assert resp.status == 404


async def test_cancel_job_id_malformed_returns_400(http_client):
    resp = await http_client.post("/api/backtest-jobs/abc/cancel")
    assert resp.status == 400
```

(If `http_client` fixture isn't present in this file, reuse whatever aiohttp test fixture the other `test_cancel_*` tests use — look at the existing `test_dashboard_backtests_api.py` tests to see the pattern.)

### Step 2: Verify failure

```bash
python -m pytest tests/test_dashboard_backtests_api.py -v -k cancel
```

Expected: all 5 new tests FAIL (404 from router — endpoints not registered).

### Step 3: Add cancel handlers

In `bullforge/services/dashboard_service/backtests_api.py`, add these two handler functions. Place them immediately after the existing `_trades` handler (before the trailing helpers):

```python
async def _cancel_job(request: web.Request) -> web.Response:
    """POST /api/backtest-jobs/{job_id}/cancel — cancel by direct job_id."""
    try:
        job_id = int(request.match_info["job_id"])
    except (KeyError, ValueError):
        return web.json_response(
            {"error": "job_id must be an integer"}, status=400,
        )
    jobs = request.app["jobs_db"]
    result = await jobs.cancel(job_id)
    if result == "already_terminal":
        return web.json_response(
            {"error": "job is not queued or running", "job_id": job_id},
            status=409,
        )
    return web.json_response({"job_id": job_id, "status": "cancelled"})


async def _cancel_run(request: web.Request) -> web.Response:
    """POST /api/backtests/{run_id}/cancel — find linked job, cancel it."""
    try:
        run_id = int(request.match_info["id"])
    except (KeyError, ValueError):
        return web.json_response(
            {"error": "run_id must be an integer"}, status=400,
        )
    db = request.app["db"]
    async with db.pool.acquire() as conn:
        job_id = await conn.fetchval(
            """
            SELECT id FROM backtest_jobs
            WHERE run_id = $1 AND status IN ('queued', 'running')
            ORDER BY id DESC LIMIT 1
            """,
            run_id,
        )
    if job_id is None:
        return web.json_response(
            {"error": "no cancellable job for run", "run_id": run_id},
            status=404,
        )
    jobs = request.app["jobs_db"]
    result = await jobs.cancel(job_id)
    if result == "already_terminal":
        return web.json_response(
            {"error": "job is not queued or running", "run_id": run_id},
            status=409,
        )
    return web.json_response(
        {"run_id": run_id, "job_id": job_id, "status": "cancelled"},
    )
```

### Step 4: Register the routes

Find the `register_backtests_routes` function (around line 59). Replace it with:

```python
def register_backtests_routes(app: web.Application) -> None:
    app.router.add_get("/api/backtests", _list)
    app.router.add_post("/api/backtests", _create)
    app.router.add_get("/api/backtests/{id}", _detail)
    app.router.add_get("/api/backtests/{id}/folds", _folds)
    app.router.add_get("/api/backtests/{id}/equity", _equity)
    app.router.add_get("/api/backtests/{id}/trades", _trades)
    app.router.add_post("/api/backtests/{id}/cancel", _cancel_run)
    app.router.add_post("/api/backtest-jobs/{job_id}/cancel", _cancel_job)
```

### Step 5: Run tests

```bash
python -m pytest tests/test_dashboard_backtests_api.py -v
```

Expected: all pass (existing + 5 new).

### Step 6: Commit

```bash
git add bullforge/services/dashboard_service/backtests_api.py \
        bullforge/tests/test_dashboard_backtests_api.py
git commit -m "feat(dashboard): POST /api/{backtests,backtest-jobs} cancel endpoints"
```

---

## Task 6: Dashboard UI — Cancel button + `cancelled` status styling

**Files:**
- Modify: `bullforge/services/dashboard_service/static/backtests.js`
- Modify: `bullforge/services/dashboard_service/static/backtests.css`

### Step 1: Add `cancelBacktest` helper to backtests.js

In `bullforge/services/dashboard_service/static/backtests.js`, near the top of the IIFE (next to other top-level helpers like `fetchJSON`), add:

```javascript
async function cancelBacktest(row) {
  if (!confirm('Cancel backtest "' + (row.name || row.id) + '"?')) return;
  const url = row.kind === 'run'
    ? '/api/backtests/' + row.id + '/cancel'
    : '/api/backtest-jobs/' + row.id + '/cancel';
  try {
    await fetchJSON(url, { method: 'POST' });
    reload();
  } catch (e) {
    alert('Cancel failed: ' + ((e.body && e.body.error) || e.message || 'unknown'));
  }
}
```

(`reload()` is the existing list-reload helper; `fetchJSON` is the existing wrapper. Verify both are in scope at the top of the file — if they're named differently, match the actual names.)

### Step 2: Add Cancel button to list rows

In `renderRows` (around line 332), find this line inside the `.map`:

```javascript
      return `<tr class="clickable" onclick="location.hash='${target}'">
        <td>${r.name}</td><td>${symsDisplay}</td><td>${range}</td>
        <td>${pnl}</td><td>${sharpe}</td><td>${dd}</td>
        <td>${ntr}</td><td>${hit}</td><td>${r.status}</td>
        <td>${tagChips}</td>
        <td>${fmtTime(r.finished_at)}</td>
      </tr>`;
```

Replace with:

```javascript
      const canCancel = (r.kind === 'job')
        || (r.kind === 'run' && r.status === 'running');
      const cancelBtn = canCancel
        ? `<button class="bt-cancel-btn" onclick="event.stopPropagation();window.__btCancel(${JSON.stringify({kind: r.kind, id: r.id, name: r.name})
             .replace(/"/g, '&quot;')})">Cancel</button>`
        : '';
      const statusCell = r.status === 'cancelled'
        ? `<span class="bt-status-cancelled">${r.status}</span>`
        : r.status;
      return `<tr class="clickable" onclick="location.hash='${target}'">
        <td>${r.name}</td><td>${symsDisplay}</td><td>${range}</td>
        <td>${pnl}</td><td>${sharpe}</td><td>${dd}</td>
        <td>${ntr}</td><td>${hit}</td>
        <td>${statusCell} ${cancelBtn}</td>
        <td>${tagChips}</td>
        <td>${fmtTime(r.finished_at)}</td>
      </tr>`;
```

Then, at module init (after `cancelBacktest` is defined), expose it on the window for the onclick handler:

```javascript
window.__btCancel = cancelBacktest;
```

### Step 3: Add Cancel button to detail page

In `renderDetail` (around line 364), find the header rendering (the `<h2 id="bt-detail-title">Run ' + id + '</h2>'` line). Add a cancel-button placeholder after the title and populate it once the detail payload arrives.

Locate this existing block:

```javascript
    root().innerHTML =
      '<div class="bt-detail">' +
      '<a class="bt-back" href="#backtests">← Back to list</a>' +
      '<h2 id="bt-detail-title">Run ' + id + '</h2>' +
      '<div class="subtab-bar" id="bt-detail-subtabs">' +
```

Change the `<h2>` line to include a span for the cancel button:

```javascript
      '<h2 id="bt-detail-title">Run ' + id +
      ' <span id="bt-detail-cancel"></span></h2>' +
```

Then, wherever the detail payload is rendered (find `document.getElementById('bt-detail-title')` or the block that populates run metadata — search `renderDetail` for where `data.status` / run status is used to update DOM), add:

```javascript
const cancelSlot = document.getElementById('bt-detail-cancel');
if (cancelSlot && data && data.status === 'running') {
  cancelSlot.innerHTML =
    '<button class="bt-cancel-btn" onclick="window.__btCancel({kind:\'run\',id:' +
    id + ',name:\'' + (data.name || id) + '\'})">Cancel</button>';
} else if (cancelSlot) {
  cancelSlot.innerHTML = '';
}
```

(Adjust the location based on the existing structure — if the detail renderer doesn't have a clean hook for post-fetch DOM updates, the safest spot is immediately after the `root().innerHTML = '...';` assignment's subsequent data-fetch completes. The implementer should verify against the actual file flow.)

### Step 4: Add CSS for the button and cancelled-status badge

In `bullforge/services/dashboard_service/static/backtests.css`, append:

```css
.bt-cancel-btn {
  margin-left: 0.5em;
  padding: 0.1em 0.5em;
  font-size: 0.85em;
  background: #b94b4b;
  color: white;
  border: none;
  border-radius: 3px;
  cursor: pointer;
}
.bt-cancel-btn:hover {
  background: #d25959;
}
.bt-status-cancelled {
  color: #b58900;
  font-weight: 600;
}
```

### Step 5: Syntax check the JS

```bash
node --check bullforge/services/dashboard_service/static/backtests.js 2>&1 || \
  python -c "import esprima" 2>&1 || \
  echo "syntax check requires node or esprima; skip if unavailable"
```

If neither is available, manually scan for balanced braces/parens in the changed sections. The test for functional correctness is Task 8's browser smoke test.

### Step 6: Commit

```bash
git add bullforge/services/dashboard_service/static/backtests.js \
        bullforge/services/dashboard_service/static/backtests.css
git commit -m "feat(dashboard): Cancel button + cancelled-status styling"
```

---

## Task 7: CLI entrypoint

**Files:**
- Create: `bullforge/services/backtest_service/cancel.py`

### Step 1: Write the CLI script

Create `bullforge/services/backtest_service/cancel.py` with EXACTLY:

```python
"""One-shot cancel entrypoint.

Usage:
    docker compose exec backtest-worker-cpu python -m services.backtest_service.cancel <job_id>

Prints 'cancelled' on success (exit 0) or 'already_terminal' if the job was
no longer queued/running (exit 1). Exits 2 on argv error.
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
        print(
            "usage: python -m services.backtest_service.cancel <job_id>",
            file=sys.stderr,
        )
        sys.exit(2)
    try:
        job_id_arg = int(sys.argv[1])
    except ValueError:
        print("job_id must be an integer", file=sys.stderr)
        sys.exit(2)
    logging.basicConfig(level=logging.INFO)
    sys.exit(asyncio.run(_main(job_id_arg)))
```

### Step 2: Syntax check

```bash
python -c "import ast; ast.parse(open('bullforge/services/backtest_service/cancel.py').read()); print('OK')"
```

Expected: `OK`.

### Step 3: Commit

```bash
git add bullforge/services/backtest_service/cancel.py
git commit -m "feat(backtest): CLI entry point for cancel"
```

---

## Task 8: Deploy + smoke test on DGX

**Files:** none modified — deployment + verification only.

### Step 1: Pull + rebuild

On the DGX:

```bash
cd ~/BullForge/bullforge
git pull
docker compose up -d --build --remove-orphans \
  dashboard_service backtest-worker-cpu backtest-worker-gpu
```

Migration 020 applies on live-pool `connect()`. `--remove-orphans` per the prior lesson.

### Step 2: Submit a long backtest

From the dashboard, submit a multi-fold backtest (e.g., 5 symbols × 12 months × 5 horizons). It should show status `running` within a few seconds.

### Step 3: Cancel via the dashboard

On the backtests list, click **Cancel** next to the running run. Confirm the dialog. Within a few seconds (at most ~100 bars of replay, typically <1 s):

- Worker log: `backtest job <id> cancelled during run`
- Dashboard list: status flips to `cancelled` (amber badge)

### Step 4: Verify DB state

```bash
docker compose exec -T timescaledb psql -U bullforge -d bullforge -c \
  "SELECT id, status, started_at, finished_at FROM backtest_jobs WHERE status='cancelled' ORDER BY id DESC LIMIT 5;"
docker compose exec -T timescaledb psql -U bullforge -d bullforge -c \
  "SELECT id, name, status FROM backtest_runs WHERE status='cancelled' ORDER BY id DESC LIMIT 5;"
```

Expected: the job row is `cancelled` with `finished_at` populated; the run row is `cancelled`.

### Step 5: Test CLI cancel

Submit another backtest from the dashboard. Note its job id from the list (or `SELECT id FROM backtest_jobs WHERE status='running' ORDER BY id DESC LIMIT 1;`). Then:

```bash
docker compose exec backtest-worker-cpu python -m services.backtest_service.cancel <job_id>
```

Expected: `cancelled\n` on stdout, exit code 0. Re-running should print `already_terminal` and exit 1.

### Step 6: Test 409 on already-terminal

```bash
curl -s -o /dev/null -w "%{http_code}\n" -X POST \
  http://localhost:18084/api/backtest-jobs/<completed_or_cancelled_job_id>/cancel
```

Expected: `409`.

### Step 7: Report back

- Commits pushed
- Time from click → worker log line
- Any anomalies in dashboard rendering or DB state
