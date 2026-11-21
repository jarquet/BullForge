# bullforge/tests/test_backtest_jobs_db.py
import json
from datetime import datetime, timezone

import pytest
from shared.config import Settings
from shared.db import Database
from services.backtest_service.jobs_db import BacktestJobsDB
from services.backtest_service.results_db import BacktestResultsDB

pytestmark = [pytest.mark.asyncio, pytest.mark.services]

UTC = timezone.utc


@pytest.fixture
async def db():
    d = Database(Settings())
    await d.connect()
    # Isolate: clear queue so claim_next is deterministic, and scrub any
    # _seed_run leftovers so they don't accumulate as 'running' rows in the
    # live backtests list. Scoped DELETE on backtest_runs (not TRUNCATE
    # CASCADE) because runtime_config_audit.source_backtest_run_id is FK-
    # linked and CASCADE would wipe the whole audit log on a dev DB.
    async with d.pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE backtest_jobs RESTART IDENTITY CASCADE")
        await conn.execute("DELETE FROM backtest_runs WHERE name = 'jobs-db-test-run'")
    yield d
    async with d.pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE backtest_jobs RESTART IDENTITY CASCADE")
        await conn.execute("DELETE FROM backtest_runs WHERE name = 'jobs-db-test-run'")
    await d.disconnect()


async def _seed_run(db) -> int:
    rdb = BacktestResultsDB(db)
    return await rdb.insert_run(
        name="jobs-db-test-run",
        symbols=["SPY"],
        timeframe="1m",
        horizons=[5],
        range_start=datetime(2025, 1, 1, tzinfo=UTC),
        range_end=datetime(2025, 1, 31, tzinfo=UTC),
        train_days=10, test_days=5, embargo_minutes=30, mode="rolling",
        config={},
    )


async def test_insert_and_claim_roundtrip(db):
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(name="t1", config={"symbols": ["SPY"]})
    assert job_id > 0

    claimed = await jobs.claim_next(worker_pool="cpu")
    assert claimed is not None
    assert claimed["id"] == job_id
    assert claimed["status"] == "running"
    assert claimed["started_at"] is not None

    second = await jobs.claim_next(worker_pool="cpu")
    assert second is None


async def test_mark_completed(db):
    jobs = BacktestJobsDB(db)
    run_id = await _seed_run(db)
    job_id = await jobs.insert_job(name="t2", config={})
    await jobs.claim_next(worker_pool="cpu")
    await jobs.mark_completed(job_id=job_id, run_id=run_id)
    row = await jobs.get(job_id)
    assert row["status"] == "completed"
    assert row["run_id"] == run_id
    assert row["finished_at"] is not None


async def test_mark_failed(db):
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(name="t3", config={})
    await jobs.claim_next(worker_pool="cpu")
    await jobs.mark_failed(job_id=job_id, error="boom")
    row = await jobs.get(job_id)
    assert row["status"] == "failed"
    assert row["error"] == "boom"


async def test_list_pending_jobs(db):
    jobs = BacktestJobsDB(db)
    j1 = await jobs.insert_job(name="a", config={})
    j2 = await jobs.insert_job(name="b", config={})
    pending = await jobs.list_pending()
    ids = {r["id"] for r in pending}
    assert {j1, j2}.issubset(ids)
    assert all(r["status"] in ("queued", "running") for r in pending)


async def test_link_run_excludes_from_pending(db):
    jobs = BacktestJobsDB(db)
    run_id = await _seed_run(db)
    job_id = await jobs.insert_job(name="mid-flight", config={})
    await jobs.claim_next(worker_pool="cpu")  # status -> running, run_id still NULL

    # Before link: appears in pending.
    assert any(r["id"] == job_id for r in await jobs.list_pending())

    await jobs.link_run(job_id=job_id, run_id=run_id)

    # After link: excluded, even though status is still 'running'.
    pending = await jobs.list_pending()
    assert all(r["id"] != job_id for r in pending)
    row = await jobs.get(job_id)
    assert row["status"] == "running"
    assert row["run_id"] == run_id


async def test_insert_job_defaults_to_cpu(db):
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(name="t", config={})
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT worker_pool FROM backtest_jobs WHERE id = $1", job_id,
        )
    assert row["worker_pool"] == "cpu"


async def test_insert_job_derives_pool_from_model_type(db):
    jobs = BacktestJobsDB(db)
    cpu_id = await jobs.insert_job(
        name="cpu_job", config={"model_type": "gradient_boost"},
    )
    gpu_id = await jobs.insert_job(
        name="gpu_job", config={"model_type": "lstm"},
    )
    async with db.pool.acquire() as conn:
        cpu_row = await conn.fetchrow(
            "SELECT worker_pool FROM backtest_jobs WHERE id = $1", cpu_id,
        )
        gpu_row = await conn.fetchrow(
            "SELECT worker_pool FROM backtest_jobs WHERE id = $1", gpu_id,
        )
    assert cpu_row["worker_pool"] == "cpu"
    assert gpu_row["worker_pool"] == "gpu"


async def test_insert_job_explicit_worker_pool_overrides_derivation(db):
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(
        name="override", config={"model_type": "gradient_boost"},
        worker_pool="gpu",
    )
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT worker_pool FROM backtest_jobs WHERE id = $1", job_id,
        )
    assert row["worker_pool"] == "gpu"


async def test_claim_next_filters_by_pool(db):
    jobs = BacktestJobsDB(db)
    cpu_id = await jobs.insert_job(
        name="cpu_j", config={"model_type": "gradient_boost"},
    )
    gpu_id = await jobs.insert_job(
        name="gpu_j", config={"model_type": "lstm"},
    )

    claimed_cpu = await jobs.claim_next(worker_pool="cpu")
    assert claimed_cpu is not None and claimed_cpu["id"] == cpu_id

    claimed_gpu = await jobs.claim_next(worker_pool="gpu")
    assert claimed_gpu is not None and claimed_gpu["id"] == gpu_id

    assert await jobs.claim_next(worker_pool="cpu") is None
    assert await jobs.claim_next(worker_pool="gpu") is None


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
