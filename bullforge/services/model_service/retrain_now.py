"""One-shot retrain entrypoint.

Runs a single RetrainScheduler.run_once() cycle (gradient_boost via CPU pool,
LSTM in-process on GPU) and exits. Intended for manual invocation after
clearing model_registry, e.g.:

    docker compose exec model_service python -m services.model_service.retrain_now
"""
from __future__ import annotations

import asyncio
import logging

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from shared.runtime_config import RuntimeConfig
from services.model_service.inference import InferenceEngine
from services.model_service.model_store import ModelStore
from services.model_service.scheduler import RetrainScheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("retrain_now")


async def main() -> None:
    settings = Settings()
    db = Database(settings)
    redis_client = RedisClient(settings)
    await db.connect()
    await redis_client.connect()

    model_store = ModelStore(settings.model_artifacts_dir)
    inference_engine = InferenceEngine(db=db, model_store=model_store)
    runtime_config = RuntimeConfig(redis_client)

    horizons_by_tf = {
        "1m": await runtime_config.get("label_horizons_1m"),
        "1d": await runtime_config.get("label_horizons_1d"),
    }

    equity_symbols = await runtime_config.get("trading_symbols_equity")
    crypto_symbols = await runtime_config.get("trading_symbols_crypto")
    all_symbols = list(equity_symbols or []) + list(crypto_symbols or [])

    scheduler = RetrainScheduler(
        db_dsn="",
        artifacts_dir=settings.model_artifacts_dir,
        runtime_config=runtime_config,
        inference_engine=inference_engine,
        redis_client=redis_client,
        symbols=all_symbols,
        timeframes=["1m", "1d"],
        horizons_by_tf=horizons_by_tf,
        min_samples=settings.model_min_samples,
        min_samples_1d=settings.model_min_samples_1d,
        db=db,
    )

    logger.info("Starting one-shot retrain cycle...")
    summary = await scheduler.run_once()
    logger.info(
        "Done. ok=%d errors=%d skipped=%d",
        summary["ok"], summary["errors"], summary["skipped"],
    )

    await redis_client.disconnect()
    await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
