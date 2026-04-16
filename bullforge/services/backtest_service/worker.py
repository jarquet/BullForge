from __future__ import annotations

import asyncio
import logging
import signal
import traceback
from datetime import datetime
from typing import Callable

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from shared.runtime_config import RuntimeConfig

from services.backtest_service.engine import BacktestConfig, BacktestEngine
from services.backtest_service.jobs_db import BacktestJobsDB
from services.backtest_service.results_db import BacktestResultsDB
from services.model_service.model_store import ModelStore

logger = logging.getLogger(__name__)

IDLE_SLEEP_SECONDS = 2.0


def _build_config(cfg: dict) -> BacktestConfig:
    return BacktestConfig(
        name=cfg.get("name", "dashboard-run"),
        custom_name=cfg.get("custom_name"),
        symbols=list(cfg["symbols"]),
        timeframe=cfg["timeframe"],
        horizons=list(cfg["horizons"]),
        range_start=datetime.fromisoformat(cfg["range_start"]),
        range_end=datetime.fromisoformat(cfg["range_end"]),
        train_days=int(cfg["train_days"]),
        test_days=int(cfg["test_days"]),
        embargo_minutes=int(cfg["embargo_min"]),
        mode=cfg["mode"],
    )


async def run_one(
    jobs: BacktestJobsDB,
    *,
    engine_factory: Callable[[BacktestConfig], BacktestEngine],
) -> bool:
    """Claim and run exactly one job. Returns True iff a job was processed."""
    job = await jobs.claim_next()
    if job is None:
        return False

    job_id = job["id"]
    try:
        cfg = _build_config({
            "name": job["name"],
            "custom_name": job.get("custom_name"),
            **job["config"],
        })
        engine = engine_factory(cfg)

        async def _link(run_id: int) -> None:
            await jobs.link_run(job_id=job_id, run_id=run_id)

        run_id = await engine.run(cfg, on_run_created=_link)
        await jobs.mark_completed(job_id=job_id, run_id=int(run_id))
        logger.info("backtest job %s -> run %s completed", job_id, run_id)
    except Exception as exc:
        tb = traceback.format_exc()
        logger.exception("backtest job %s failed", job_id)
        await jobs.mark_failed(job_id=job_id, error=f"{exc}\n{tb}")
    return True


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = Settings()

    live_db = Database(settings)
    await live_db.connect()  # runs migrations, incl. backtest schema
    sandbox_db = Database(settings, sandbox=True)
    await sandbox_db.connect()  # search_path=backtest,public
    redis = RedisClient(settings)
    await redis.connect()
    rc = RuntimeConfig(redis)
    model_store = ModelStore(settings.model_artifacts_dir)
    results = BacktestResultsDB(live_db)
    jobs = BacktestJobsDB(live_db)

    def _engine_factory(_cfg: BacktestConfig) -> BacktestEngine:
        return BacktestEngine(
            db=sandbox_db,
            runtime_config=rc,
            model_store=model_store,
            results_db=results,
        )

    stop = asyncio.Event()

    def _graceful(*_):
        logger.info("worker shutdown requested")
        stop.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _graceful)
        except NotImplementedError:
            pass

    try:
        while not stop.is_set():
            did = await run_one(jobs, engine_factory=_engine_factory)
            if not did:
                try:
                    await asyncio.wait_for(
                        stop.wait(), timeout=IDLE_SLEEP_SECONDS,
                    )
                except asyncio.TimeoutError:
                    pass
    finally:
        await redis.disconnect()
        await sandbox_db.disconnect()
        await live_db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
