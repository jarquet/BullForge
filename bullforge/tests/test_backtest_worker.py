from unittest.mock import AsyncMock
import pytest

from services.backtest_service.worker import run_one


pytestmark = pytest.mark.asyncio


class FakeJobs:
    def __init__(self, job):
        self._job = job
        self.completed = None
        self.failed = None
        self.linked = None
    async def claim_next(self):
        j, self._job = self._job, None
        return j
    async def link_run(self, *, job_id, run_id):
        self.linked = (job_id, run_id)
    async def mark_completed(self, *, job_id, run_id):
        self.completed = (job_id, run_id)
    async def mark_failed(self, *, job_id, error):
        self.failed = (job_id, error)


async def test_run_one_success():
    jobs = FakeJobs({
        "id": 7,
        "name": "t",
        "config": {
            "symbols": ["SPY"], "timeframe": "1m", "horizons": [5],
            "range_start": "2025-01-01T00:00:00+00:00",
            "range_end": "2025-01-02T00:00:00+00:00",
            "train_days": 1, "test_days": 1, "embargo_min": 5,
            "mode": "rolling",
        },
    })
    engine = AsyncMock()

    async def fake_run(cfg, *, on_run_created=None):
        if on_run_created is not None:
            await on_run_created(42)
        return 42

    engine.run = AsyncMock(side_effect=fake_run)

    got = await run_one(jobs, engine_factory=lambda cfg: engine)
    assert got is True
    assert jobs.linked == (7, 42)
    assert jobs.completed == (7, 42)


async def test_run_one_idle_when_no_job():
    jobs = FakeJobs(None)
    engine = AsyncMock()
    got = await run_one(jobs, engine_factory=lambda cfg: engine)
    assert got is False
    assert jobs.completed is None
    engine.run.assert_not_called()


async def test_run_one_failure_marks_failed():
    jobs = FakeJobs({
        "id": 9, "name": "t",
        "config": {
            "symbols": ["SPY"], "timeframe": "1m", "horizons": [5],
            "range_start": "2025-01-01T00:00:00+00:00",
            "range_end": "2025-01-02T00:00:00+00:00",
            "train_days": 1, "test_days": 1, "embargo_min": 5,
            "mode": "rolling",
        },
    })
    engine = AsyncMock()
    engine.run = AsyncMock(side_effect=RuntimeError("boom"))
    await run_one(jobs, engine_factory=lambda cfg: engine)
    assert jobs.failed is not None
    assert "boom" in jobs.failed[1]
