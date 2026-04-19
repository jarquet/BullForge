from __future__ import annotations

import logging
from datetime import datetime

from aiohttp import web

from shared.db import Database

logger = logging.getLogger(__name__)


def register_backtests_extra_routes(app: web.Application) -> None:
    # IMPORTANT: aiohttp matches routes in registration order. Register the
    # literal-path routes (compare, sensitivity) BEFORE the parameterized
    # /{id}/... routes so "/api/backtests/compare" isn't caught by
    # "/api/backtests/{id}" (which already exists in backtests_api.py but
    # doesn't shadow here — different paths — but keep this convention).
    app.router.add_get("/api/backtests/compare", _handle_compare)
    app.router.add_get("/api/backtests/sensitivity", _handle_sensitivity)
    app.router.add_get("/api/backtests/{id}/symbols", _handle_symbols)
    app.router.add_get("/api/backtests/{id}/distributions", _handle_distributions)
    app.router.add_get("/api/backtests/{id}/live-overlay", _handle_live_overlay)
    app.router.add_get("/api/backtests/{id}/funnel", _handle_funnel)
    app.router.add_post("/api/backtests/{id}/tags", _handle_set_tags)
    app.router.add_post("/api/backtests/{id}/notes", _handle_set_notes)
    app.router.add_post("/api/backtests/{id}/promote-config", _handle_promote_config)


def _parse_run_id(request: web.Request) -> int:
    raw = request.match_info.get("id", "")
    try:
        return int(raw)
    except ValueError:
        raise web.HTTPBadRequest(reason=f"invalid backtest run id: {raw!r}")


def _iso(dt):
    return dt.isoformat() if isinstance(dt, datetime) else dt


async def _handle_symbols(request: web.Request) -> web.Response:
    run_id = _parse_run_id(request)
    db: Database = request.app["db"]
    rows = await db.query_backtest_symbols(run_id=run_id)
    return web.json_response(rows)


async def _handle_distributions(request: web.Request) -> web.Response:
    run_id = _parse_run_id(request)
    db: Database = request.app["db"]
    data = await db.query_backtest_distributions(run_id=run_id)
    return web.json_response({
        "daily_pnl": [
            {"date": _iso(d["date"]), "pnl": d["pnl"], "trades": d["trades"]}
            for d in data["daily_pnl"]
        ],
        "trade_pnl_histogram": data["trade_pnl_histogram"],
        "hold_time_buckets": data["hold_time_buckets"],
        "side_breakdown": data["side_breakdown"],
    })


async def _handle_live_overlay(request: web.Request) -> web.Response:
    run_id = _parse_run_id(request)
    db: Database = request.app["db"]
    data = await db.query_backtest_live_overlay(run_id=run_id)
    return web.json_response({
        "has_overlap": data["has_overlap"],
        "symbols": data["symbols"],
        "range_start": _iso(data["range_start"]),
        "range_end": _iso(data["range_end"]),
        "backtest_series": [
            {"timestamp": _iso(s["timestamp"]), "equity": s["equity"]}
            for s in data["backtest_series"]
        ],
        "live_series": [
            {"timestamp": _iso(s["timestamp"]), "equity": s["equity"]}
            for s in data["live_series"]
        ],
    })


async def _handle_funnel(request: web.Request) -> web.Response:
    run_id = _parse_run_id(request)
    min_conf = float(request.query.get("min_confidence", "0.6"))
    db: Database = request.app["db"]
    data = await db.query_backtest_funnel(
        run_id=run_id, min_confidence=min_conf,
    )
    return web.json_response(data)


async def _handle_compare(request: web.Request) -> web.Response:
    raw_ids = request.query.get("ids")
    if not raw_ids:
        raise web.HTTPBadRequest(reason="missing 'ids' query parameter")
    try:
        ids = [int(x) for x in raw_ids.split(",") if x.strip()]
    except ValueError:
        raise web.HTTPBadRequest(reason="'ids' must be comma-separated integers")
    if not ids:
        raise web.HTTPBadRequest(reason="'ids' must include at least one run id")
    if len(ids) > 5:
        raise web.HTTPBadRequest(reason="compare supports at most 5 runs")
    db: Database = request.app["db"]
    data = await db.query_backtest_compare(run_ids=ids)
    return web.json_response({
        "runs": [
            {
                **r,
                "equity_series": [
                    {"timestamp": _iso(p["timestamp"]), "equity": p["equity"]}
                    for p in r["equity_series"]
                ],
            }
            for r in data["runs"]
        ],
    })


async def _handle_sensitivity(request: web.Request) -> web.Response:
    swept = request.query.get("swept_param")
    if not swept:
        raise web.HTTPBadRequest(reason="missing 'swept_param' query parameter")
    group_by_raw = request.query.get("group_by", "symbols,timeframe,horizons")
    group_by = [g.strip() for g in group_by_raw.split(",") if g.strip()]
    db: Database = request.app["db"]
    data = await db.query_backtest_sensitivity(
        group_by=group_by, swept_param=swept,
    )
    return web.json_response(data)


async def _handle_set_tags(request: web.Request) -> web.Response:
    run_id = _parse_run_id(request)
    try:
        body = await request.json()
    except Exception:
        raise web.HTTPBadRequest(reason="invalid JSON body")
    tags = body.get("tags")
    if not isinstance(tags, list) or not all(isinstance(t, str) for t in tags):
        raise web.HTTPBadRequest(reason="'tags' must be a list of strings")
    db: Database = request.app["db"]
    await db.set_backtest_tags(run_id, tags)
    saved = await db.get_backtest_tags(run_id)
    return web.json_response({"tags": saved})


async def _handle_set_notes(request: web.Request) -> web.Response:
    run_id = _parse_run_id(request)
    try:
        body = await request.json()
    except Exception:
        raise web.HTTPBadRequest(reason="invalid JSON body")
    notes = body.get("notes")
    if notes is not None and not isinstance(notes, str):
        raise web.HTTPBadRequest(reason="'notes' must be a string or null")
    db: Database = request.app["db"]
    await db.set_backtest_notes(run_id, notes or "")
    return web.json_response({"notes": await db.get_backtest_notes(run_id)})


_PROMOTABLE_KEY_PREFIXES = ("execution_",)


async def _handle_promote_config(request: web.Request) -> web.Response:
    """Promote a backtest run's config snapshot to live runtime_config.

    Body: {keys: ["min_confidence", ...], note: "..."}
    Walks the run's config_json for each key; compares to current
    runtime_config['execution_<key>']; applies changes; records audit rows.
    """
    run_id = _parse_run_id(request)
    try:
        body = await request.json()
    except Exception:
        raise web.HTTPBadRequest(reason="invalid JSON body")

    keys = body.get("keys")
    if not isinstance(keys, list) or not keys:
        raise web.HTTPBadRequest(reason="'keys' must be a non-empty list")
    note = body.get("note", "")

    db: Database = request.app["db"]
    rc = request.app.get("runtime_config")
    if rc is None:
        raise web.HTTPServiceUnavailable(reason="runtime_config not available")

    # Fetch the run's config_json.
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT config_json FROM backtest_runs WHERE id = $1", run_id,
        )
    if row is None:
        raise web.HTTPNotFound(reason=f"backtest run {run_id} not found")
    import json as _json
    cfg = row["config_json"] if isinstance(row["config_json"], dict) \
        else _json.loads(row["config_json"])

    diff: list[dict] = []
    for key in keys:
        if not isinstance(key, str) or key not in cfg:
            continue
        rt_key = None
        if f"execution_{key}" in cfg or any(key.startswith(p) for p in ("execution_",)):
            rt_key = key if key.startswith("execution_") else f"execution_{key}"
        else:
            rt_key = f"execution_{key}"
        new_value = cfg[key]
        try:
            old_value = await rc.get(rt_key)
        except Exception:
            old_value = None
        if old_value == new_value:
            continue
        await rc.set(rt_key, new_value)
        await db.insert_runtime_config_audit(
            key=rt_key,
            old_value=str(old_value) if old_value is not None else None,
            new_value=str(new_value),
            source_backtest_run_id=run_id,
            note=note or None,
        )
        diff.append({
            "key": rt_key, "old_value": old_value, "new_value": new_value,
        })
    return web.json_response({"diff": diff, "run_id": run_id})
