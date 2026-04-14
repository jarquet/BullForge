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
from services.execution_service.alpaca_broker import AlpacaBroker
from services.execution_service.signal_aggregator import SignalAggregator
from services.execution_service.risk_manager import RiskManager
from services.execution_service.position_tracker import PositionTracker
from services.execution_service.order_executor import OrderExecutor
from services.execution_service.health import create_health_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("execution_service")


async def main() -> None:
    settings = Settings()
    db = Database(settings)
    redis_client = RedisClient(settings)

    logger.info("Connecting to TimescaleDB...")
    await db.connect()
    logger.info("Connecting to Redis...")
    await redis_client.connect()

    broker = AlpacaBroker(settings)
    aggregator = SignalAggregator(
        min_confidence=settings.execution_min_confidence,
        min_agreement=settings.execution_min_agreement,
    )
    runtime_config = RuntimeConfig(redis_client)
    risk_manager = RiskManager(
        max_position_value=settings.execution_max_position_value,
        max_total_positions=settings.execution_max_positions,
        max_portfolio_exposure=settings.execution_max_exposure,
        min_signal_strength=settings.execution_min_signal_strength,
        runtime_config=runtime_config,
    )
    position_tracker = PositionTracker(broker)
    executor = OrderExecutor(
        broker=broker,
        risk_manager=risk_manager,
        position_tracker=position_tracker,
        db=db,
        redis_client=redis_client,
    )

    async def on_prediction(message: str) -> None:
        try:
            data = json.loads(message)
            symbol = data["symbol"]
            timeframe = data["timeframe"]
            predictions = data["predictions"]

            signal_obj = aggregator.aggregate(
                symbol=symbol,
                timestamp=datetime.now(timezone.utc),
                predictions=predictions,
            )
            if signal_obj is None:
                logger.debug("No actionable signal for %s %s", symbol, timeframe)
                return

            bars = await db.query_bars(symbol, "1m", limit=1)
            if not bars:
                logger.warning("No price data for %s", symbol)
                return
            current_price = bars[0].close

            order = await executor.process_signal(signal_obj, current_price)
            if order:
                logger.info(
                    "Executed: %s %s %s qty=%.1f status=%s",
                    order.side, order.symbol, order.order_type,
                    order.quantity, order.status,
                )
        except Exception:
            logger.exception("Error processing prediction message")

    await redis_client.subscribe("model:predictions", on_prediction)
    logger.info("Subscribed to model:predictions")

    async def on_model_status(message: str) -> None:
        try:
            statuses = json.loads(message)
            disabled_ids = {
                int(model_id)
                for model_id, status in statuses.items()
                if status == "disabled"
            }
            aggregator.update_disabled_models(disabled_ids)
            logger.info(
                "Updated model status: %d disabled models", len(disabled_ids),
            )
        except Exception:
            logger.exception("Error processing model status update")

    await redis_client.subscribe("models:status", on_model_status)
    logger.info("Subscribed to models:status")

    health_app = create_health_app(db, redis_client, broker)
    runner = web.AppRunner(health_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8082)
    await site.start()
    logger.info("Health check server running on :8082")

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

    logger.info("Execution service running. Press Ctrl+C to stop.")

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
