from __future__ import annotations

import asyncio
import logging
import signal
import sys

from aiohttp import web

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from services.data_pipeline.alpaca_client import AlpacaClient
from services.data_pipeline.backfill import Backfiller
from services.data_pipeline.stream import TickStreamer
from services.data_pipeline.calendar_service import CalendarService
from services.data_pipeline.health import create_health_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("data_pipeline")


async def main() -> None:
    settings = Settings()
    db = Database(settings)
    redis_client = RedisClient(settings)
    alpaca_client = AlpacaClient(settings)

    logger.info("Connecting to TimescaleDB...")
    await db.connect()
    logger.info("Connecting to Redis...")
    await redis_client.connect()

    # Run backfill on startup
    backfiller = Backfiller(settings=settings, db=db, alpaca_client=alpaca_client)
    logger.info("Running initial backfill...")
    await backfiller.backfill_all(days_1m=14, days_daily=365)

    # Start live tick streaming
    streamer = TickStreamer(
        settings=settings, db=db,
        redis_client=redis_client, alpaca_client=alpaca_client,
    )

    # Start health check server
    health_app = create_health_app(db=db, redis_client=redis_client)
    runner = web.AppRunner(health_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    logger.info("Health check endpoint running on :8080/health")

    # Start Alpaca streams
    stock_stream = None
    crypto_stream = None

    if settings.trading_symbols_equity:
        stock_stream = alpaca_client.create_stock_stream()
        stock_stream.subscribe_bars(streamer.handle_bar, *settings.trading_symbols_equity)
        logger.info("Starting equity stream for: %s", settings.trading_symbols_equity)

    if settings.trading_symbols_crypto:
        crypto_stream = alpaca_client.create_crypto_stream()
        crypto_stream.subscribe_bars(streamer.handle_bar, *settings.trading_symbols_crypto)
        logger.info("Starting crypto stream for: %s", settings.trading_symbols_crypto)

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
            # Windows doesn't support add_signal_handler
            pass

    # Run streams in background threads (alpaca-py uses threading internally)
    stream_tasks = []
    if stock_stream:
        stream_tasks.append(
            asyncio.get_event_loop().run_in_executor(None, stock_stream.run)
        )
    if crypto_stream:
        stream_tasks.append(
            asyncio.get_event_loop().run_in_executor(None, crypto_stream.run)
        )

    logger.info("Data pipeline running. Press Ctrl+C to stop.")

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
