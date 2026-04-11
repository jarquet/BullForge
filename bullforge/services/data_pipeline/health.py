from __future__ import annotations

import logging

from aiohttp import web

from shared.db import Database
from shared.redis import RedisClient

logger = logging.getLogger(__name__)


def create_health_app(db: Database, redis_client: RedisClient) -> web.Application:
    app = web.Application()
    app["db"] = db
    app["redis"] = redis_client
    app.router.add_get("/health", health_handler)
    return app


async def health_handler(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    redis_client: RedisClient = request.app["redis"]

    checks = {}
    all_healthy = True

    # Check database
    if db.pool is not None:
        try:
            async with db.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            checks["db"] = {
                "status": "healthy",
                "pool_size": db.pool.get_size(),
                "idle": db.pool.get_idle_size(),
            }
        except Exception as e:
            checks["db"] = {"status": "unhealthy", "error": str(e)}
            all_healthy = False
    else:
        checks["db"] = {"status": "unhealthy", "error": "pool is None"}
        all_healthy = False

    # Check Redis
    try:
        if redis_client.client:
            await redis_client.client.ping()
            checks["redis"] = {"status": "healthy"}
        else:
            checks["redis"] = {"status": "unhealthy", "error": "client is None"}
            all_healthy = False
    except Exception as e:
        checks["redis"] = {"status": "unhealthy", "error": str(e)}
        all_healthy = False

    status_code = 200 if all_healthy else 503
    return web.json_response(
        {"status": "healthy" if all_healthy else "unhealthy", **checks},
        status=status_code,
    )
