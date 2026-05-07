from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from aiohttp import web

from shared.db import Database
from shared.redis import RedisClient
from services.execution_service.alpaca_broker import AlpacaBroker

if TYPE_CHECKING:
    from services.execution_service.trade_updates_stream import TradeUpdatesStream


def create_health_app(
    db: Database,
    redis_client: RedisClient,
    broker: AlpacaBroker,
    stream: Optional["TradeUpdatesStream"] = None,
) -> web.Application:
    app = web.Application()
    app["db"] = db
    app["redis"] = redis_client
    app["broker"] = broker
    app["stream"] = stream
    app.router.add_get("/health", health_handler)
    return app


async def health_handler(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    redis_client: RedisClient = request.app["redis"]
    broker: AlpacaBroker = request.app["broker"]
    stream = request.app.get("stream")

    checks: dict = {}

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

    if stream is not None:
        checks["stream_queue_dropped"] = int(getattr(stream, "queue_full_dropped", 0))

    all_ok = all(
        v == "ok"
        for k, v in checks.items()
        if k != "stream_queue_dropped"
    )
    status = 200 if all_ok else 503
    return web.json_response(checks, status=status)
