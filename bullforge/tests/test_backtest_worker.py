from unittest.mock import AsyncMock
import pytest

from services.backtest_service.worker import _build_config, run_one


class FakeJobs:
    def __init__(self, job):
        self._job = job
        self.completed = None
        self.failed = None
        self.linked = None
    async def claim_next(self, *, worker_pool):
        j, self._job = self._job, None
        return j
    async def link_run(self, *, job_id, run_id):
        self.linked = (job_id, run_id)
    async def mark_completed(self, *, job_id, run_id):
        self.completed = (job_id, run_id)
    async def mark_failed(self, *, job_id, error):
        self.failed = (job_id, error)
    async def is_cancelled(self, job_id):
        return False


@pytest.mark.asyncio
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

    async def fake_run(cfg, *, on_run_created=None, check_cancelled=None):
        if on_run_created is not None:
            await on_run_created(42)
        return 42

    engine.run = AsyncMock(side_effect=fake_run)

    got = await run_one(
        jobs, engine_factory=lambda cfg: engine, worker_pool="cpu",
    )
    assert got is True
    assert jobs.linked == (7, 42)
    assert jobs.completed == (7, 42)


@pytest.mark.asyncio
async def test_run_one_idle_when_no_job():
    jobs = FakeJobs(None)
    engine = AsyncMock()
    got = await run_one(
        jobs, engine_factory=lambda cfg: engine, worker_pool="cpu",
    )
    assert got is False
    assert jobs.completed is None
    engine.run.assert_not_called()


@pytest.mark.asyncio
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
    await run_one(
        jobs, engine_factory=lambda cfg: engine, worker_pool="cpu",
    )
    assert jobs.failed is not None
    assert "boom" in jobs.failed[1]


def test_build_config_copies_threshold_overrides():
    cfg = _build_config({
        "name": "t",
        "symbols": ["SPY"], "timeframe": "1m", "horizons": [5],
        "range_start": "2025-01-01T00:00:00+00:00",
        "range_end": "2025-01-02T00:00:00+00:00",
        "train_days": 1, "test_days": 1, "embargo_min": 5,
        "mode": "rolling",
        "min_confidence": 0.75,
        "min_agreement": 3,
        "direction_band": 0.4,
        "min_signal_strength": 0.6,
        "max_position_pct": 0.05,
        "max_positions": 2,
        "max_exposure": 0.1,
    })
    assert cfg.min_confidence == 0.75
    assert cfg.min_agreement == 3
    assert cfg.direction_band == 0.4
    assert cfg.min_signal_strength == 0.6
    assert cfg.max_position_pct == 0.05
    assert cfg.max_positions == 2
    assert cfg.max_exposure == 0.1


def test_build_config_missing_thresholds_stay_none():
    cfg = _build_config({
        "name": "t",
        "symbols": ["SPY"], "timeframe": "1m", "horizons": [5],
        "range_start": "2025-01-01T00:00:00+00:00",
        "range_end": "2025-01-02T00:00:00+00:00",
        "train_days": 1, "test_days": 1, "embargo_min": 5,
        "mode": "rolling",
    })
    assert cfg.min_confidence is None
    assert cfg.min_agreement is None
    assert cfg.direction_band is None
    assert cfg.min_signal_strength is None
    assert cfg.max_position_pct is None
    assert cfg.max_positions is None
    assert cfg.max_exposure is None


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
