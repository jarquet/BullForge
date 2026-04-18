from __future__ import annotations

import logging
from datetime import datetime

from aiohttp import web

from shared.db import Database
from shared.time_window import parse_time_window

logger = logging.getLogger(__name__)


def register_trading_routes(app: web.Application) -> None:
    app.router.add_get("/api/trading/equity", _handle_equity)
    app.router.add_get("/api/trading/distributions", _handle_distributions)
    app.router.add_get("/api/trading/heatmap", _handle_heatmap)
    app.router.add_get("/api/trading/streaks", _handle_streaks)
    app.router.add_get("/api/trading/exposure", _handle_exposure)
    app.router.add_get("/api/trading/benchmark", _handle_benchmark)
    app.router.add_get("/api/trading/symbols/{sym}", _handle_symbol)


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


async def _starting_equity(db: Database) -> float:
    snap = await db.query_latest_metric_snapshot("account", "", "all")
    if snap and "equity" in snap.get("metrics", {}):
        return float(snap["metrics"]["equity"])
    return 100_000.0  # fallback default


async def _handle_equity(request: web.Request) -> web.Response:
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    starting = await _starting_equity(db)
    series = await db.query_equity_curve(
        starting_equity=starting, from_dt=from_dt, to_dt=to_dt,
    )
    return web.json_response([
        {
            "timestamp": _iso(s["timestamp"]),
            "realized_pnl": s["realized_pnl"],
            "realized_equity": s["realized_equity"],
            "trailing_peak": s["trailing_peak"],
            "drawdown": s["drawdown"],
        }
        for s in series
    ])


async def _handle_distributions(request: web.Request) -> web.Response:
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    data = await db.query_distributions(from_dt=from_dt, to_dt=to_dt)
    return web.json_response({
        "daily_pnl": [
            {"date": _iso(d["date"]), "pnl": d["pnl"], "trades": d["trades"]}
            for d in data["daily_pnl"]
        ],
        "trade_pnl_histogram": data["trade_pnl_histogram"],
        "hold_time_buckets": data["hold_time_buckets"],
        "side_breakdown": data["side_breakdown"],
    })


async def _handle_streaks(request: web.Request) -> web.Response:
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    data = await db.query_streaks(from_dt=from_dt, to_dt=to_dt)
    return web.json_response({
        "current_streak_kind": data["current_streak_kind"],
        "current_streak_length": data["current_streak_length"],
        "longest_win_streak": data["longest_win_streak"],
        "longest_loss_streak": data["longest_loss_streak"],
        "ribbon": [
            {"timestamp": _iso(r["timestamp"]), "win": r["win"]}
            for r in data["ribbon"]
        ],
    })


async def _handle_heatmap(request: web.Request) -> web.Response:
    from_dt, to_dt = parse_time_window(request)
    kind = request.query.get("kind", "time")
    db: Database = request.app["db"]
    if kind == "time":
        data = await db.query_heatmap_time_of_day(from_dt=from_dt, to_dt=to_dt)
    else:
        raise web.HTTPBadRequest(reason=f"unsupported kind: {kind!r}")
    return web.json_response(data)


async def _handle_exposure(request: web.Request) -> web.Response:
    from_dt, to_dt = parse_time_window(request)
    resolution_min = int(request.query.get("resolution_minutes", "5"))
    db: Database = request.app["db"]
    starting = await _starting_equity(db)
    timeline = await db.query_exposure_timeline(
        equity=starting, from_dt=from_dt, to_dt=to_dt,
        resolution_minutes=resolution_min,
    )
    return web.json_response([
        {
            "timestamp": _iso(p["timestamp"]),
            "total_exposure_pct": p["total_exposure_pct"],
            "by_symbol": p["by_symbol"],
        }
        for p in timeline
    ])


async def _handle_benchmark(request: web.Request) -> web.Response:
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    starting = await _starting_equity(db)
    data = await db.query_benchmark(
        starting_equity=starting, from_dt=from_dt, to_dt=to_dt,
    )
    return web.json_response({
        "strategy_series": [
            {"timestamp": _iso(s["timestamp"]), "equity": s["equity"]}
            for s in data["strategy_series"]
        ],
        "spy_series": [
            {"timestamp": _iso(s["timestamp"]), "equity": s["equity"]}
            for s in data["spy_series"]
        ],
        "stats": data["stats"],
    })


async def _handle_symbol(request: web.Request) -> web.Response:
    symbol = request.match_info.get("sym", "")
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    data = await db.query_symbol_drilldown(
        symbol=symbol, from_dt=from_dt, to_dt=to_dt,
    )
    return web.json_response({
        "symbol": data["symbol"],
        "trade_count": data["trade_count"],
        "equity_series": [
            {"timestamp": _iso(s["timestamp"]),
             "equity": s["equity"], "drawdown": s["drawdown"]}
            for s in data["equity_series"]
        ],
        "trades": [
            {**t,
             "entry_time": _iso(t["entry_time"]),
             "exit_time": _iso(t["exit_time"])}
            for t in data["trades"]
        ],
        "side_breakdown": data["side_breakdown"],
        "win_rate_over_time": [
            {**r, "date": _iso(r["date"])}
            for r in data["win_rate_over_time"]
        ],
    })
