from __future__ import annotations

import asyncio
import json
import logging
import signal
from datetime import datetime, timezone

from aiohttp import web

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from shared.runtime_config import RuntimeConfig
from services.model_service.model_store import ModelStore
from services.model_service.trainer import Trainer
from services.model_service.inference import InferenceEngine
from services.model_service.health import create_health_app
from services.model_service.scheduler import RetrainScheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("model_service")

HORIZONS = [5, 10, 30]


async def main() -> None:
    settings = Settings()
    db = Database(settings)
    redis_client = RedisClient(settings)

    logger.info("Connecting to TimescaleDB...")
    await db.connect()
    logger.info("Connecting to Redis...")
    await redis_client.connect()

    model_store = ModelStore(settings.model_artifacts_dir)
    trainer = Trainer(
        db=db,
        model_store=model_store,
        min_samples=settings.model_min_samples,
        min_samples_1d=settings.model_min_samples_1d,
    )
    inference_engine = InferenceEngine(db=db, model_store=model_store)
    runtime_config = RuntimeConfig(redis_client)

    # Wait for feature engine to produce enough data before training
    all_symbols = (
        settings.trading_symbols_equity + settings.trading_symbols_crypto
    )
    startup_as_of = datetime.now(timezone.utc)
    logger.info(
        "Waiting for feature engine to produce >= %d samples...",
        settings.model_min_samples,
    )
    max_wait = 600  # 10 minutes
    poll_interval = 15
    waited = 0
    while waited < max_wait:
        rows = await db.query_training_data(
            all_symbols[0], "1m", HORIZONS[0], as_of=startup_as_of, limit=settings.model_min_samples,
        )
        if len(rows) >= settings.model_min_samples:
            logger.info("Feature data ready (%d rows), proceeding with training", len(rows))
            break
        logger.info(
            "Feature data not ready yet (%d/%d rows), retrying in %ds...",
            len(rows), settings.model_min_samples, poll_interval,
        )
        await asyncio.sleep(poll_interval)
        waited += poll_interval
    else:
        logger.warning(
            "Timed out waiting for feature data after %ds, training with available data",
            max_wait,
        )

    # Train models on startup (skip if active model already exists)
    for symbol in all_symbols:
        for timeframe in ["1m", "1d"]:
            for horizon in HORIZONS:
                existing = await db.query_active_model(
                    symbol, timeframe, horizon,
                )
                if existing:
                    logger.info(
                        "Active model exists for %s %s %dm (id=%d), skipping training",
                        symbol, timeframe, horizon, existing.id,
                    )
                    continue
                await trainer.train(
                    symbol, timeframe, horizon, as_of=startup_as_of, model_type="gradient_boost",
                )

    # Load all trained models for inference
    await inference_engine.load_models()

    scheduler = RetrainScheduler(
        db_dsn="",  # Settings() env vars used inside worker processes
        artifacts_dir=settings.model_artifacts_dir,
        runtime_config=runtime_config,
        inference_engine=inference_engine,
        redis_client=redis_client,
        symbols=all_symbols,
        timeframes=["1m", "1d"],
        horizons=HORIZONS,
        min_samples=settings.model_min_samples,
        min_samples_1d=settings.model_min_samples_1d,
        db=db,
    )
    scheduler_task = asyncio.create_task(scheduler.run_forever())
    logger.info("Retrain scheduler started")

    # Subscribe to feature updates for reactive inference
    async def on_feature_update(message: str) -> None:
        data = json.loads(message)
        symbol = data["symbol"]
        timeframe = data["timeframe"]
        logger.debug("Feature update for %s %s, running inference", symbol, timeframe)

        predictions = await inference_engine.predict(symbol, timeframe)
        if predictions:
            await db.insert_predictions(predictions)
            await redis_client.publish(
                "model:predictions",
                json.dumps({
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "predictions": [
                        {
                            "horizon_minutes": p.horizon_minutes,
                            "direction": p.predicted_direction,
                            "confidence": p.confidence,
                            "model_id": p.model_id,
                        }
                        for p in predictions
                    ],
                }),
            )
            logger.info(
                "Published %d predictions for %s %s",
                len(predictions), symbol, timeframe,
            )

    await redis_client.subscribe("feature:updates", on_feature_update)
    logger.info("Subscribed to feature:updates for reactive inference")

    # Start health check server
    health_app = create_health_app(db, redis_client, inference_engine)
    runner = web.AppRunner(health_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8081)
    await site.start()
    logger.info("Health check server running on :8081")

    # Graceful shutdown
    shutdown_event = asyncio.Event()

    def handle_signal():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    logger.info("Model service running. Press Ctrl+C to stop.")

    try:
        await shutdown_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down...")
        scheduler.stop()
        scheduler_task.cancel()
        try:
            await scheduler_task
        except (asyncio.CancelledError, Exception):
            pass
        await runner.cleanup()
        await redis_client.disconnect()
        await db.disconnect()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
