from __future__ import annotations

import logging

from aiohttp import web

logger = logging.getLogger(__name__)

STUB_ENDPOINTS: dict[str, dict] = {}


def _make_handler(enabled_after_plan: str):
    async def handler(request: web.Request) -> web.Response:
        return web.json_response(
            {
                "error": "not_implemented",
                "enabled_after_plan": enabled_after_plan,
            },
            status=501,
        )
    return handler


def register_stub_routes(app: web.Application) -> None:
    for path, meta in STUB_ENDPOINTS.items():
        app.router.add_get(path, _make_handler(meta["enabled_after_plan"]))
