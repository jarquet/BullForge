from __future__ import annotations

import logging

from aiohttp import web

from shared.db import Database
from shared.redis import RedisClient

logger = logging.getLogger(__name__)


def create_health_app(
    db: Database,
    redis_client: RedisClient,
    inference_engine: object,
) -> web.Application:
    app = web.Application()
    app["db"] = db
    app["redis"] = redis_client
    app["inference"] = inference_engine
    app.router.add_get("/health", health_handler)
    return app


async def health_handler(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    redis_client: RedisClient = request.app["redis"]
    inference_engine = request.app["inference"]

    checks = {}
    all_healthy = True

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

    model_count = len(getattr(inference_engine, "_models", {}))
    checks["models"] = {"loaded": model_count}

    status_code = 200 if all_healthy else 503
    return web.json_response(
        {"status": "healthy" if all_healthy else "unhealthy", **checks},
        status=status_code,
    )
