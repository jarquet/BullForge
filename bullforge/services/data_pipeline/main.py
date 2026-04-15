from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys

from aiohttp import web

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from shared.runtime_config import RuntimeConfig
from services.data_pipeline.alpaca_client import AlpacaClient
from services.data_pipeline.backfill import Backfiller
from services.data_pipeline.stream import TickStreamer
from services.data_pipeline.calendar_service import CalendarService
from services.data_pipeline.health import create_health_app

SYMBOLS_CHANGED_CHANNEL = "config:symbols_changed"


async def handle_symbol_change_event(
    message: str, *, stream, backfill,
    background_tasks: set[asyncio.Task],
) -> None:
    """Handle a config:symbols_changed event.

    On add: forward-fill (blocking), subscribe, then schedule reach_back
    for both timeframes as background tasks.
    On remove: unsubscribe only (historical data is retained).
    """
    data = json.loads(message)
    action, kind, symbol = data["action"], data["kind"], data["symbol"]
    if action == "add":
        await backfill.backfill_symbol(symbol, kind=kind)
        await stream.subscribe_symbol(symbol, kind=kind)
        for timeframe in ("1m", "1d"):
            task = asyncio.create_task(backfill.reach_back(symbol, timeframe))
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)
    elif action == "remove":
        await stream.unsubscribe_symbol(symbol, kind=kind)

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

    runtime_config = RuntimeConfig(redis_client)
    equity_symbols = await runtime_config.get("trading_symbols_equity")
    crypto_symbols = await runtime_config.get("trading_symbols_crypto")
    logger.info(
        "Runtime symbol config — equity=%s crypto=%s",
        equity_symbols, crypto_symbols,
    )

    # Forward gap-fill blocks startup so live ticks have no gap; the long
    # reach-back to the configured floor runs in the background once
    # streams are up.
    backfiller = Backfiller(
        settings=settings, db=db,
        alpaca_client=alpaca_client, redis_client=redis_client,
    )
    background_tasks: set[asyncio.Task] = set()
    logger.info("Running forward gap-fill...")
    await backfiller.backfill_all_forward()

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

    if equity_symbols:
        stock_stream = alpaca_client.create_stock_stream()
        stock_stream.subscribe_bars(streamer.handle_bar, *equity_symbols)
        logger.info("Starting equity stream for: %s", equity_symbols)

    if crypto_symbols:
        crypto_stream = alpaca_client.create_crypto_stream()
        crypto_stream.subscribe_bars(streamer.handle_bar, *crypto_symbols)
        logger.info("Starting crypto stream for: %s", crypto_symbols)

    streamer.attach_streams(stock_stream, crypto_stream)

    reach_back_task = asyncio.create_task(backfiller.backfill_all_reach_back())
    background_tasks.add(reach_back_task)
    reach_back_task.add_done_callback(background_tasks.discard)
    logger.info("Scheduled background reach-back to historical floor")

    async def _on_symbols_changed(msg: str) -> None:
        try:
            await handle_symbol_change_event(
                msg, stream=streamer, backfill=backfiller,
                background_tasks=background_tasks,
            )
        except Exception:
            logger.exception("Failed handling symbols_changed event: %s", msg)

    await redis_client.subscribe(SYMBOLS_CHANGED_CHANNEL, _on_symbols_changed)

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

    # Start consumer task on main loop to process bars from stream threads
    consumer_task = asyncio.create_task(streamer.consume())

    logger.info("Data pipeline running. Press Ctrl+C to stop.")

    try:
        await shutdown_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down...")
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
        for task in list(background_tasks):
            task.cancel()
        for task in list(background_tasks):
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Background task raised during shutdown")
        await runner.cleanup()
        await redis_client.disconnect()
        await db.disconnect()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
