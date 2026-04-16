# bullforge/tests/test_backtest_jobs_db.py
import json
from datetime import datetime, timezone

import pytest
from shared.config import Settings
from shared.db import Database
from services.backtest_service.jobs_db import BacktestJobsDB
from services.backtest_service.results_db import BacktestResultsDB

pytestmark = pytest.mark.asyncio

UTC = timezone.utc


@pytest.fixture
async def db():
    d = Database(Settings())
    await d.connect()
    # Isolate: clear queue so claim_next is deterministic across runs.
    async with d.pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE backtest_jobs RESTART IDENTITY CASCADE")
    yield d
    async with d.pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE backtest_jobs RESTART IDENTITY CASCADE")
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

    claimed = await jobs.claim_next()
    assert claimed is not None
    assert claimed["id"] == job_id
    assert claimed["status"] == "running"
    assert claimed["started_at"] is not None

    second = await jobs.claim_next()
    assert second is None


async def test_mark_completed(db):
    jobs = BacktestJobsDB(db)
    run_id = await _seed_run(db)
    job_id = await jobs.insert_job(name="t2", config={})
    await jobs.claim_next()
    await jobs.mark_completed(job_id=job_id, run_id=run_id)
    row = await jobs.get(job_id)
    assert row["status"] == "completed"
    assert row["run_id"] == run_id
    assert row["finished_at"] is not None


async def test_mark_failed(db):
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(name="t3", config={})
    await jobs.claim_next()
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
    await jobs.claim_next()  # status -> running, run_id still NULL

    # Before link: appears in pending.
    assert any(r["id"] == job_id for r in await jobs.list_pending())

    await jobs.link_run(job_id=job_id, run_id=run_id)

    # After link: excluded, even though status is still 'running'.
    pending = await jobs.list_pending()
    assert all(r["id"] != job_id for r in pending)
    row = await jobs.get(job_id)
    assert row["status"] == "running"
    assert row["run_id"] == run_id
