from __future__ import annotations

import asyncio
import json
import logging
import signal
from datetime import datetime, timezone

from aiohttp import web

from shared.config import Settings
from shared.db import Database
from shared.models import ModelStatus
from shared.redis import RedisClient
from shared.runtime_config import RuntimeConfig
from services.execution_service.alpaca_broker import AlpacaBroker
from services.monitoring_service.broker_client import BrokerClient
from services.monitoring_service.health import create_health_app
from services.monitoring_service.metrics import MetricsComputer
from services.monitoring_service.fidelity import FidelityTracker
from services.monitoring_service.halt_manager import HaltManager
from services.monitoring_service.telegram_bot import TelegramBot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("monitoring_service")


async def monitoring_loop(
    db: Database,
    broker: AlpacaBroker,
    redis_client: RedisClient,
    metrics_computer: MetricsComputer,
    fidelity_tracker: FidelityTracker,
    halt_manager: HaltManager,
    settings: Settings,
    shutdown_event: asyncio.Event,
) -> None:
    equity_history: list[float] = []

    while not shutdown_event.is_set():
        try:
            # 1. Get current account state
            try:
                account = await broker.get_account()
                equity_history.append(account["equity"])
            except Exception as e:
                logger.warning("Failed to get account: %s", e)
                account = {}

            # 2. Compute trade metrics over rolling windows
            all_trades = await db.query_closed_trades(limit=1000)

            trading_metrics = metrics_computer.compute_trading_metrics(all_trades)
            await db.insert_metric_snapshot("trading", "", "all", trading_metrics)

            if len(all_trades) >= 20:
                last_20 = metrics_computer.compute_trading_metrics(all_trades[:20])
                await db.insert_metric_snapshot("trading", "", "last_20", last_20)

            if len(all_trades) >= 50:
                last_50 = metrics_computer.compute_trading_metrics(all_trades[:50])
                await db.insert_metric_snapshot("trading", "", "last_50", last_50)

            # Account metrics
            if account:
                account_metrics = metrics_computer.compute_account_metrics(
                    account, equity_history,
                )
                await db.insert_metric_snapshot("account", "", "all", account_metrics)

            # Per-symbol metrics
            symbols = set(t.symbol for t in all_trades)
            for sym in symbols:
                sym_trades = [t for t in all_trades if t.symbol == sym]
                sym_metrics = metrics_computer.compute_trading_metrics(sym_trades)
                await db.insert_metric_snapshot("symbol", sym, "all", sym_metrics)

            # Per-direction metrics
            for direction in ("long", "short"):
                dir_metrics = metrics_computer.compute_direction_metrics(
                    all_trades, direction,
                )
                await db.insert_metric_snapshot("direction", direction, "all", dir_metrics)

            # 3. Evaluate model fidelity
            fidelity_results = await fidelity_tracker.evaluate_all_models()
            now = datetime.now(timezone.utc)
            statuses = []
            for model_id, status, live_accuracy in fidelity_results:
                reason = f"Live accuracy: {live_accuracy:.2%}"
                await db.upsert_model_status(model_id, status, reason)
                statuses.append(ModelStatus(
                    model_id=model_id,
                    status=status,
                    reason=reason,
                    updated_at=now,
                ))

            # 4. Evaluate halt conditions
            current_drawdown = 0.0
            if account:
                acct_metrics = metrics_computer.compute_account_metrics(
                    account, equity_history,
                )
                current_drawdown = acct_metrics.get("current_drawdown", 0.0)

            alerts = await halt_manager.evaluate(statuses, current_drawdown)

            # 5. Publish status to Redis
            await halt_manager.publish_status(statuses)

            # 6. Publish alerts for Telegram bot (Phase 5B)
            for alert in alerts:
                await redis_client.publish(
                    "monitoring:alerts", json.dumps(alert),
                )

            logger.info(
                "Monitoring tick: %d trades, %d models, %d alerts, drawdown=%.2f%%",
                len(all_trades), len(statuses), len(alerts),
                current_drawdown * 100,
            )

        except Exception:
            logger.exception("Error in monitoring loop")

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=settings.monitoring_interval,
            )
            break
        except asyncio.TimeoutError:
            pass


async def main() -> None:
    settings = Settings()
    db = Database(settings)
    redis_client = RedisClient(settings)

    logger.info("Connecting to TimescaleDB...")
    await db.connect()
    logger.info("Connecting to Redis...")
    await redis_client.connect()

    broker = AlpacaBroker(settings)
    metrics_computer = MetricsComputer()
    fidelity_tracker = FidelityTracker(db, settings)
    halt_manager = HaltManager(db, redis_client, settings)
    runtime_config = RuntimeConfig(redis_client)
    broker_client = BrokerClient(redis_client=redis_client, timeout=5.0)

    # Start Telegram bot (optional — only if token is configured)
    telegram_bot = None
    if settings.telegram_bot_token and settings.telegram_chat_id:
        telegram_bot = TelegramBot(settings, db, halt_manager, redis_client)
        await telegram_bot.start()
        telegram_bot.attach_handlers(
            db=db,
            broker_client=broker_client,
            runtime_config=runtime_config,
            halt_manager=halt_manager,
        )
        logger.info("Telegram bot started")
    else:
        logger.info("Telegram bot disabled (no token configured)")

    # Start health check server
    health_app = create_health_app(db, redis_client)
    runner = web.AppRunner(health_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", settings.monitoring_port)
    await site.start()
    logger.info("Health check server running on :%d", settings.monitoring_port)

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

    logger.info(
        "Monitoring service running (interval=%ds). Press Ctrl+C to stop.",
        settings.monitoring_interval,
    )

    await monitoring_loop(
        db, broker, redis_client,
        metrics_computer, fidelity_tracker, halt_manager,
        settings, shutdown_event,
    )

    logger.info("Shutting down...")
    if telegram_bot:
        await telegram_bot.stop()
    await runner.cleanup()
    await redis_client.disconnect()
    await db.disconnect()
    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
