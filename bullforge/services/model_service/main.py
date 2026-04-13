from __future__ import annotations

import asyncio
import json
import logging
import signal

from aiohttp import web

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from services.model_service.model_store import ModelStore
from services.model_service.trainer import Trainer
from services.model_service.inference import InferenceEngine
from services.model_service.health import create_health_app

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
    )
    inference_engine = InferenceEngine(db=db, model_store=model_store)

    # Train models on startup (skip if active model already exists)
    all_symbols = (
        settings.trading_symbols_equity + settings.trading_symbols_crypto
    )
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
                    symbol, timeframe, horizon, model_type="gradient_boost",
                )

    # Load all trained models for inference
    await inference_engine.load_models()

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
        await runner.cleanup()
        await redis_client.disconnect()
        await db.disconnect()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
