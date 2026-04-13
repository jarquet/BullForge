from __future__ import annotations

from aiohttp import web

from shared.db import Database
from shared.redis import RedisClient


def create_health_app(db: Database, redis_client: RedisClient) -> web.Application:
    async def health_handler(request: web.Request) -> web.Response:
        checks = {}
        all_ok = True

        # Database check
        try:
            async with db.pool.acquire() as conn:
                await conn.execute("SELECT 1")
            checks["db"] = "ok"
        except Exception as e:
            checks["db"] = f"error: {e}"
            all_ok = False

        # Redis check
        try:
            await redis_client.client.ping()
            checks["redis"] = "ok"
        except Exception as e:
            checks["redis"] = f"error: {e}"
            all_ok = False

        status = 200 if all_ok else 503
        return web.json_response(checks, status=status)

    app = web.Application()
    app.router.add_get("/health", health_handler)
    return app
