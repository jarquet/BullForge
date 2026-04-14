from __future__ import annotations

from aiohttp import web

from shared.runtime_config import RuntimeConfig


def register_config_routes(app: web.Application) -> None:
    app.router.add_get("/api/config", _get_config)
    app.router.add_put("/api/config/{key}", _put_config)


async def _get_config(request: web.Request) -> web.Response:
    rc: RuntimeConfig = request.app["runtime_config"]
    values = await rc.get_all()
    schema = rc.get_schema()
    return web.json_response({"values": values, "schema": schema})


async def _put_config(request: web.Request) -> web.Response:
    rc: RuntimeConfig = request.app["runtime_config"]
    key = request.match_info["key"]
    body = await request.json()
    value = body.get("value")
    try:
        await rc.set(key, value)
    except KeyError:
        return web.json_response(
            {"error": f"unknown key {key}"}, status=404,
        )
    except ValueError as e:
        return web.json_response({"error": str(e)}, status=400)
    return web.json_response({"ok": True, "key": key, "value": value})
