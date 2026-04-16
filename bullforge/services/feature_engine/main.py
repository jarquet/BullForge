from __future__ import annotations

import asyncio
import json
import logging
import signal

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from shared.runtime_config import RuntimeConfig
from services.feature_engine.registry import FeatureRegistry
from services.feature_engine.calculators.price import compute_price_features
from services.feature_engine.calculators.technical import compute_technical_features
from services.feature_engine.calculators.volume import compute_volume_features
from services.feature_engine.calculators.calendar import compute_calendar_features
from services.feature_engine.engine import FeatureEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("feature_engine")


def build_registry() -> FeatureRegistry:
    registry = FeatureRegistry()
    registry.register("price", compute_price_features)
    registry.register("technical", compute_technical_features)
    registry.register("volume", compute_volume_features)
    registry.register("calendar", compute_calendar_features)
    return registry


async def main() -> None:
    settings = Settings()
    db = Database(settings)
    redis_client = RedisClient(settings)

    logger.info("Connecting to TimescaleDB...")
    await db.connect()
    logger.info("Connecting to Redis...")
    await redis_client.connect()

    runtime_config = RuntimeConfig(redis_client)
    label_horizons = {
        "1m": await runtime_config.get("label_horizons_1m"),
        "1d": await runtime_config.get("label_horizons_1d"),
    }
    logger.info("Using label horizons from runtime config: %s", label_horizons)

    registry = build_registry()
    engine = FeatureEngine(
        settings=settings,
        db=db,
        registry=registry,
        label_horizons=label_horizons,
        label_threshold=0.001,
        min_bars=50,
        redis_client=redis_client,
    )

    # Run initial full computation
    logger.info("Running initial feature computation for all symbols...")
    await engine.compute_all()

    # Subscribe to market ticks for reactive computation
    async def on_tick(message: str) -> None:
        data = json.loads(message)
        symbol = data["symbol"]
        logger.debug("Tick received for %s, scheduling feature update", symbol)
        await engine.compute_features_for_symbol(symbol, "1m")

    await redis_client.subscribe("market:ticks", on_tick)
    logger.info("Subscribed to market:ticks for reactive feature updates")

    async def recompute_daily_features_loop(interval_seconds: int = 3600) -> None:
        all_symbols = (
            settings.trading_symbols_equity + settings.trading_symbols_crypto
        )
        while True:
            await asyncio.sleep(interval_seconds)
            logger.info("Hourly 1d feature recompute starting for %d symbols", len(all_symbols))
            for symbol in all_symbols:
                try:
                    await engine.compute_features_for_symbol(symbol, "1d")
                except Exception:
                    logger.exception("Failed 1d recompute for %s", symbol)

    daily_task = asyncio.create_task(recompute_daily_features_loop())
    logger.info("Hourly 1d feature recompute task started")

    # Set up graceful shutdown
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

    logger.info("Feature engine running. Press Ctrl+C to stop.")

    try:
        await shutdown_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down...")
        daily_task.cancel()
        try:
            await daily_task
        except (asyncio.CancelledError, Exception):
            pass
        await redis_client.disconnect()
        await db.disconnect()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
