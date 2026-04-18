from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path

from aiohttp import web

from shared.config import Settings
from shared.db import Database
from shared.redis import RedisClient
from shared.runtime_config import RuntimeConfig
from services.backtest_service.jobs_db import BacktestJobsDB
from services.backtest_service.results_db import BacktestResultsDB
from services.dashboard_service.api import create_api_app
from services.dashboard_service.config_api import register_config_routes
from services.dashboard_service.freshness_api import register_freshness_routes
from services.dashboard_service.stubs_api import register_stub_routes
from services.dashboard_service.symbols_api import register_symbol_routes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("dashboard_service")

STATIC_DIR = Path(__file__).parent / "static"


async def main() -> None:
    settings = Settings()
    db = Database(settings)
    redis_client = RedisClient(settings)

    logger.info("Connecting to TimescaleDB...")
    await db.connect()
    logger.info("Connecting to Redis...")
    await redis_client.connect()

    runtime_config = RuntimeConfig(redis_client)

    # Build the aiohttp app with API routes
    app = create_api_app(db)
    app["redis_client"] = redis_client
    app["runtime_config"] = runtime_config
    app["results_db"] = BacktestResultsDB(db)
    app["jobs_db"] = BacktestJobsDB(db)
    register_config_routes(app)
    register_freshness_routes(app)
    register_symbol_routes(app)
    register_stub_routes(app)

    # Health check
    async def health_handler(request: web.Request) -> web.Response:
        try:
            async with db.pool.acquire() as conn:
                await conn.execute("SELECT 1")
            return web.json_response({"db": "ok"}, status=200)
        except Exception as e:
            return web.json_response({"db": f"error: {e}"}, status=503)

    app.router.add_get("/health", health_handler)

    # Serve static files
    app.router.add_static("/static", STATIC_DIR, show_index=False)

    # Serve index.html at root
    async def index_handler(request: web.Request) -> web.FileResponse:
        return web.FileResponse(STATIC_DIR / "index.html")

    app.router.add_get("/", index_handler)

    # Start the server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", settings.dashboard_port)
    await site.start()

    logger.info("Dashboard running on http://0.0.0.0:%d", settings.dashboard_port)

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

    await shutdown_event.wait()

    logger.info("Shutting down...")
    await runner.cleanup()
    await redis_client.disconnect()
    await db.disconnect()
    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
