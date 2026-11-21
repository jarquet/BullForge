from __future__ import annotations

import logging
from datetime import datetime, timezone

from aiohttp import web

from shared.db import Database
from shared.halt import is_full_halt_tier
from shared.time_window import parse_time_window

from services.dashboard_service.backtests_api import register_backtests_routes
from services.dashboard_service.backtests_extra_api import register_backtests_extra_routes
from services.dashboard_service.models_api import register_models_routes
from services.dashboard_service.trading_api import register_trading_routes

logger = logging.getLogger(__name__)


def _serialize_datetime(obj):
    """Convert datetime objects to ISO format strings for JSON."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def _serialize_dict(d: dict) -> dict:
    return {
        k: _serialize_datetime(v) if isinstance(v, datetime) else v
        for k, v in d.items()
    }


def create_api_app(db: Database) -> web.Application:
    app = web.Application()
    app["db"] = db

    app.router.add_get("/api/status", _handle_status)
    app.router.add_get("/api/metrics/trading", _handle_metrics_trading)
    app.router.add_get("/api/metrics/account", _handle_metrics_account)
    app.router.add_get("/api/metrics/symbols", _handle_metrics_symbols)
    app.router.add_get("/api/metrics/history", _handle_metrics_history)
    app.router.add_get("/api/models", _handle_models)
    app.router.add_get("/api/trades", _handle_trades)
    app.router.add_get("/api/orders", _handle_orders)
    app.router.add_get("/api/halts", _handle_halts)

    register_backtests_routes(app)
    register_backtests_extra_routes(app)
    register_models_routes(app)
    register_trading_routes(app)

    return app


async def _handle_status(request: web.Request) -> web.Response:
    db: Database = request.app["db"]

    account = await db.query_latest_metric_snapshot("account", "", "all")
    halts = await db.get_active_halts()
    models = await db.get_all_model_statuses()

    return web.json_response({
        "account": _serialize_dict(account["metrics"]) if account else None,
        "account_updated": (
            account["timestamp"].isoformat() if account else None
        ),
        "halts": [_serialize_dict(h) for h in halts],
        "is_halted": any(is_full_halt_tier(h["tier"]) for h in halts),
        "models": [
            {
                "model_id": m.model_id,
                "status": m.status,
                "reason": m.reason,
                "updated_at": m.updated_at.isoformat(),
            }
            for m in models
        ],
    })


async def _handle_metrics_trading(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    window = request.query.get("window", "all")

    snapshot = await db.query_latest_metric_snapshot("trading", "", window)

    if snapshot is None:
        return web.json_response({"metrics": None, "timestamp": None})

    return web.json_response({
        "metrics": snapshot["metrics"],
        "timestamp": snapshot["timestamp"].isoformat(),
    })


async def _handle_metrics_account(request: web.Request) -> web.Response:
    db: Database = request.app["db"]

    snapshot = await db.query_latest_metric_snapshot("account", "", "all")

    if snapshot is None:
        return web.json_response({"metrics": None, "timestamp": None})

    return web.json_response({
        "metrics": snapshot["metrics"],
        "timestamp": snapshot["timestamp"].isoformat(),
    })


async def _handle_metrics_symbols(request: web.Request) -> web.Response:
    db: Database = request.app["db"]

    # The monitoring service stores per-symbol metrics with
    # category="symbol", scope=<symbol_name>, window="all".
    # Discover symbols from recent closed trades, then query each.
    closed_trades = await db.query_closed_trades(limit=500)
    symbols = sorted(set(t.symbol for t in closed_trades))

    result = {}
    for sym in symbols:
        snap = await db.query_latest_metric_snapshot("symbol", sym, "all")
        if snap:
            result[sym] = {
                "metrics": snap["metrics"],
                "timestamp": snap["timestamp"].isoformat(),
            }

    return web.json_response(result)


async def _handle_metrics_history(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    category = request.query.get("category", "trading")
    scope = request.query.get("scope", "")
    window = request.query.get("window", "all")
    limit = min(int(request.query.get("limit", "60")), 500)

    from_dt, to_dt = parse_time_window(request)

    snapshots = await db.query_metric_snapshots(
        category, scope, window, limit,
        from_dt=from_dt, to_dt=to_dt,
    )

    return web.json_response([
        {
            "timestamp": s["timestamp"].isoformat(),
            "metrics": s["metrics"],
        }
        for s in snapshots
    ])


async def _handle_models(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    details = await db.query_model_details()
    model_ids = [d["model_id"] for d in details]
    extra = await db.query_models_with_leaderboard_stats(model_ids)

    payload = []
    for d in details:
        mid = d["model_id"]
        stats = extra.get(mid, {})
        backtested = float((d.get("metrics") or {}).get("accuracy", 0.0))
        live = stats.get("live_accuracy_7d", 0.0)
        label = (
            f"{d['model_type']} \u00b7 {d['symbol']} \u00b7 "
            f"{d['timeframe']} \u00b7 {d['horizon_minutes']}m"
        )
        payload.append({
            "model_id": mid,
            "label": label,
            "symbol": d["symbol"],
            "timeframe": d["timeframe"],
            "horizon_minutes": d["horizon_minutes"],
            "model_type": d["model_type"],
            "status": d["status"],
            "reason": d["reason"],
            "metrics": d["metrics"],
            "trained_at": (
                d["trained_at"].isoformat() if d["trained_at"] else None
            ),
            "status_updated_at": (
                d["status_updated_at"].isoformat()
                if d.get("status_updated_at") else None
            ),
            "backtested_accuracy": backtested,
            "live_accuracy_7d": live,
            "fidelity_delta": backtested - live,
            "signals_7d": stats.get("signals_7d", 0),
            "simulated_pnl_7d_pct": stats.get("simulated_pnl_7d_pct", 0.0),
            "accuracy_sparkline_30d": stats.get("accuracy_sparkline_30d", []),
        })
    return web.json_response(payload)


async def _handle_trades(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    limit = min(int(request.query.get("limit", "50")), 500)

    trades = await db.query_closed_trades(limit=limit)

    return web.json_response([
        {
            "id": t.id,
            "symbol": t.symbol,
            "side": t.side,
            "entry_time": t.entry_time.isoformat(),
            "exit_time": t.exit_time.isoformat() if t.exit_time else None,
            "entry_price": t.entry_price,
            "exit_price": t.exit_price,
            "quantity": t.quantity,
            "pnl": t.pnl,
            "hold_duration_seconds": t.hold_duration_seconds,
        }
        for t in trades
    ])


async def _handle_orders(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    limit = min(int(request.query.get("limit", "50")), 500)

    orders = await db.query_orders(limit=limit)

    return web.json_response([
        {
            "id": o.id,
            "symbol": o.symbol,
            "side": o.side,
            "quantity": o.quantity,
            "order_type": o.order_type,
            "submitted_at": o.submitted_at.isoformat(),
            "signal_strength": o.signal_strength,
            "status": o.status,
            "filled_at": o.filled_at.isoformat() if o.filled_at else None,
            "filled_price": o.filled_price,
            "filled_quantity": o.filled_quantity,
        }
        for o in orders
    ])


async def _handle_halts(request: web.Request) -> web.Response:
    db: Database = request.app["db"]

    active = await db.get_active_halts()
    history = await db.query_halt_history(limit=50)

    return web.json_response({
        "active": [_serialize_dict(h) for h in active],
        "history": [_serialize_dict(h) for h in history],
    })
