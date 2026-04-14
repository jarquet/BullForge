from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from aiohttp import web

from shared.symbol_utils import is_crypto


SYMBOLS_CHANGED_CHANNEL = "config:symbols_changed"


def register_symbol_routes(app: web.Application) -> None:
    app.router.add_get("/api/symbols", _get_symbols)
    app.router.add_post("/api/symbols", _add_symbol)
    app.router.add_delete("/api/symbols/{symbol:.+}", _remove_symbol)
    app.router.add_get("/api/symbols/{symbol:.+}/status", _symbol_status)


async def _get_symbols(request: web.Request) -> web.Response:
    rc = request.app["runtime_config"]
    return web.json_response({
        "equity": await rc.get("trading_symbols_equity"),
        "crypto": await rc.get("trading_symbols_crypto"),
    })


async def _add_symbol(request: web.Request) -> web.Response:
    body = await request.json()
    symbol = body["symbol"].strip().upper()
    kind = body.get("kind")
    if kind not in ("equity", "crypto"):
        return web.json_response(
            {"error": "kind must be equity or crypto"}, status=400,
        )
    if kind == "crypto" and "/" not in symbol:
        return web.json_response(
            {"error": "crypto symbols must contain a slash, e.g. BTC/USD"},
            status=400,
        )
    if kind == "equity" and is_crypto(symbol):
        return web.json_response(
            {"error": "equity symbols must not contain a slash"},
            status=400,
        )

    rc = request.app["runtime_config"]
    redis = request.app["redis_client"]
    key = f"trading_symbols_{kind}"
    current = await rc.get(key)
    if symbol in current:
        return web.json_response(
            {"ok": True, "added": False, "reason": "already present"},
        )
    current = current + [symbol]
    await rc.set(key, current)
    await redis.publish(
        SYMBOLS_CHANGED_CHANNEL,
        json.dumps({"action": "add", "kind": kind, "symbol": symbol}),
    )
    return web.json_response({"ok": True, "added": True, "symbol": symbol})


async def _remove_symbol(request: web.Request) -> web.Response:
    symbol = request.match_info["symbol"]
    kind = request.query.get("kind")
    if kind not in ("equity", "crypto"):
        return web.json_response(
            {"error": "kind query param required"}, status=400,
        )
    rc = request.app["runtime_config"]
    redis = request.app["redis_client"]
    key = f"trading_symbols_{kind}"
    current = await rc.get(key)
    if symbol not in current:
        return web.json_response({"ok": True, "removed": False})
    current = [s for s in current if s != symbol]
    await rc.set(key, current)
    await redis.publish(
        SYMBOLS_CHANGED_CHANNEL,
        json.dumps({"action": "remove", "kind": kind, "symbol": symbol}),
    )
    return web.json_response({"ok": True, "removed": True})


async def _symbol_status(request: web.Request) -> web.Response:
    symbol = request.match_info["symbol"]
    db = request.app["db"]
    stats = await db.query_symbol_bar_stats(symbol)
    bar_count = stats["bar_count"]
    last_bar = stats["last_bar"]
    if bar_count == 0 or last_bar is None:
        status = "pending"
    else:
        age = datetime.now(timezone.utc) - last_bar
        status = "complete" if age < timedelta(minutes=10) else "backfilling"
    return web.json_response({
        "symbol": symbol,
        "status": status,
        "bar_count": bar_count,
        "last_bar": last_bar.isoformat() if last_bar else None,
    })
