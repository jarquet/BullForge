from __future__ import annotations

from aiohttp import web

from shared.db import Database
from shared.redis import RedisClient
from services.execution_service.alpaca_broker import AlpacaBroker


def create_health_app(
    db: Database,
    redis_client: RedisClient,
    broker: AlpacaBroker,
) -> web.Application:
    app = web.Application()
    app["db"] = db
    app["redis"] = redis_client
    app["broker"] = broker
    app.router.add_get("/health", health_handler)
    return app


async def health_handler(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    redis_client: RedisClient = request.app["redis"]
    broker: AlpacaBroker = request.app["broker"]

    checks = {}

    try:
        async with db.pool.acquire() as conn:
            await conn.execute("SELECT 1")
        checks["db"] = "ok"
    except Exception as e:
        checks["db"] = f"error: {e}"

    try:
        await redis_client.client.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {e}"

    try:
        await broker.get_account()
        checks["alpaca"] = "ok"
    except Exception as e:
        checks["alpaca"] = f"error: {e}"

    all_ok = all(v == "ok" for v in checks.values())
    status = 200 if all_ok else 503
    return web.json_response(checks, status=status)
