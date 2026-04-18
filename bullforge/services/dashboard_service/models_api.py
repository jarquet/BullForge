from __future__ import annotations

import logging
from datetime import datetime

from aiohttp import web

from shared.db import Database
from shared.time_window import parse_time_window

logger = logging.getLogger(__name__)


def register_models_routes(app: web.Application) -> None:
    app.router.add_get("/api/models/{id}/history", _handle_history)
    app.router.add_get("/api/models/{id}/calibration", _handle_calibration)
    app.router.add_get("/api/models/{id}/simulated_pnl", _handle_simulated_pnl)
    app.router.add_get("/api/models/{id}/feature_importance", _handle_feature_importance)
    app.router.add_get("/api/models/{id}/regime_breakdown", _handle_regime_breakdown)
    app.router.add_get("/api/models/{id}/signal_history", _handle_signal_history)


def _parse_model_id(request: web.Request) -> int:
    raw = request.match_info.get("id", "")
    try:
        return int(raw)
    except ValueError:
        raise web.HTTPBadRequest(reason=f"invalid model id: {raw!r}")


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


async def _handle_history(request: web.Request) -> web.Response:
    model_id = _parse_model_id(request)
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    rows = await db.query_accuracy_history(
        model_id, from_dt=from_dt, to_dt=to_dt,
    )
    return web.json_response([
        {
            "date": _iso(r["date"]),
            "total": r["total"],
            "correct": r["correct"],
            "accuracy": r["accuracy"],
        }
        for r in rows
    ])


async def _handle_calibration(request: web.Request) -> web.Response:
    model_id = _parse_model_id(request)
    from_dt, to_dt = parse_time_window(request)
    num_bins = int(request.query.get("bins", "10"))
    db: Database = request.app["db"]
    bins = await db.query_calibration_bins(
        model_id=model_id, num_bins=num_bins,
        from_dt=from_dt, to_dt=to_dt,
    )
    # Calibration error = weighted mean |avg_conf - hit_rate| across bins
    # with count > 0.
    total_n = sum(b["count"] for b in bins)
    if total_n > 0:
        err = sum(
            b["count"] * abs(b["avg_confidence"] - b["hit_rate"])
            for b in bins if b["count"] > 0
        ) / total_n
    else:
        err = 0.0
    return web.json_response({
        "bins": bins,
        "calibration_error": err,
        "sample_size": total_n,
    })


async def _handle_simulated_pnl(request: web.Request) -> web.Response:
    model_id = _parse_model_id(request)
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    result = await db.query_simulated_pnl(
        model_id=model_id, from_dt=from_dt, to_dt=to_dt,
    )
    return web.json_response({
        "signal_count": result["signal_count"],
        "cumulative_pnl_pct": result["cumulative_pnl_pct"],
        "avg_pnl_pct": result["avg_pnl_pct"],
        "win_rate": result["win_rate"],
        "daily_series": [
            {
                "date": _iso(d["date"]),
                "pnl_pct": d["pnl_pct"],
                "count": d["count"],
            }
            for d in result["daily_series"]
        ],
    })


async def _handle_feature_importance(request: web.Request) -> web.Response:
    model_id = _parse_model_id(request)
    db: Database = request.app["db"]
    from shared.models import ModelMetadata
    meta = None
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT artifact_path, feature_columns, model_type,
                   symbol, timeframe, horizon_minutes, trained_at
            FROM model_registry WHERE id = $1 AND backtest_run_id IS NULL
            """,
            model_id,
        )
    if row is None:
        raise web.HTTPNotFound(reason=f"model {model_id} not found")

    model_type = row["model_type"]
    if model_type in ("lstm",):
        return web.json_response({
            "available": False,
            "reason": f"feature importance not available for {model_type}",
            "features": [],
        })

    # Load artifact once per request. For scale, swap in an LRU cache later.
    from services.model_service.model_store import ModelStore
    import asyncio as _asyncio
    import json as _json
    store = ModelStore(artifacts_dir=str(row["artifact_path"]).rsplit("/", 1)[0])
    try:
        pipeline = store.load(row["artifact_path"])
    except FileNotFoundError:
        raise web.HTTPNotFound(reason="artifact missing on disk")

    # Sklearn pipelines: the classifier is the last step. Keep a handle to
    # both the pipeline (for permutation importance over scaler + model) and
    # the bare classifier (for the native feature_importances_ fast path).
    classifier = pipeline.steps[-1][1] if hasattr(pipeline, "steps") else pipeline

    features = _json.loads(row["feature_columns"]) if isinstance(
        row["feature_columns"], str,
    ) else row["feature_columns"]

    if hasattr(classifier, "feature_importances_"):
        # Fast path for tree ensembles that publish impurity-based importances
        # (GradientBoostingClassifier, LightGBM, RandomForest, etc.).
        importances = list(classifier.feature_importances_)
    else:
        # Fallback for HistGradientBoostingClassifier and others that don't
        # expose the attribute: permutation importance over the held-out slice
        # of the training dataset (same 80/20 split the trainer used).
        from services.model_service.dataset import DatasetBuilder, time_based_split
        from sklearn.inspection import permutation_importance

        builder = DatasetBuilder(db)
        built = await builder.build(
            row["symbol"], row["timeframe"], int(row["horizon_minutes"]),
            as_of=row["trained_at"],
        )
        if built is None:
            return web.json_response({
                "available": False,
                "reason": (
                    f"{classifier.__class__.__name__} has no feature_importances_ "
                    "and training dataset is no longer available for permutation importance"
                ),
                "features": [],
            })
        X, y = built
        _, X_test, _, y_test = time_based_split(X, y, test_ratio=0.2)
        if len(X_test) == 0:
            return web.json_response({
                "available": False,
                "reason": "permutation importance needs non-empty test split",
                "features": [],
            })
        loop = _asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: permutation_importance(
                pipeline, X_test, y_test,
                n_repeats=5, random_state=42, n_jobs=-1,
            ),
        )
        importances = list(result.importances_mean)

    pairs = list(zip(features, importances))
    pairs.sort(key=lambda p: p[1], reverse=True)
    return web.json_response({
        "available": True,
        "features": [
            {"name": name, "importance": float(imp)}
            for name, imp in pairs[:20]
        ],
    })


async def _handle_regime_breakdown(request: web.Request) -> web.Response:
    model_id = _parse_model_id(request)
    from_dt, to_dt = parse_time_window(request)
    db: Database = request.app["db"]
    rows = await db.query_regime_breakdown(
        model_id=model_id, from_dt=from_dt, to_dt=to_dt,
    )
    return web.json_response(rows)


async def _handle_signal_history(request: web.Request) -> web.Response:
    model_id = _parse_model_id(request)
    limit = min(int(request.query.get("limit", "100")), 500)
    db: Database = request.app["db"]
    rows = await db.query_signal_history(model_id=model_id, limit=limit)
    return web.json_response([
        {
            "timestamp": _iso(r["timestamp"]),
            "symbol": r["symbol"],
            "timeframe": r["timeframe"],
            "horizon_minutes": r["horizon_minutes"],
            "predicted_direction": r["predicted_direction"],
            "confidence": r["confidence"],
            "entry_price": r["entry_price"],
            "exit_price": r["exit_price"],
            "is_correct": r["is_correct"],
            "pnl_pct": r["pnl_pct"],
        }
        for r in rows
    ])
