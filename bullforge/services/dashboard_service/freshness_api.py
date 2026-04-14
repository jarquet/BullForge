from __future__ import annotations

from datetime import datetime, timezone

from aiohttp import web

from shared.symbol_utils import is_crypto


def _age_seconds(ts: datetime) -> float:
    return (datetime.now(timezone.utc) - ts).total_seconds()


def _bar_color(symbol: str, age_sec: float) -> str:
    if is_crypto(symbol):
        if age_sec < 120:
            return "green"
        if age_sec < 300:
            return "yellow"
        return "red"
    if age_sec < 300:
        return "green"
    if age_sec < 900:
        return "yellow"
    return "red"


def _model_age_color(age_hours: float) -> str:
    if age_hours < 24:
        return "green"
    if age_hours < 48:
        return "yellow"
    return "red"


def register_freshness_routes(app: web.Application) -> None:
    app.router.add_get("/api/freshness", _get_freshness)


async def _get_freshness(request: web.Request) -> web.Response:
    db = request.app["db"]
    bar_rows = await db.query_latest_bar_timestamps()
    feat_rows = await db.query_latest_feature_timestamps()
    models = await db.query_model_details()

    bars = []
    for r in bar_rows:
        age = _age_seconds(r["last_bar"])
        bars.append({
            "symbol": r["symbol"],
            "last_bar": r["last_bar"].isoformat(),
            "age_seconds": age,
            "color": _bar_color(r["symbol"], age),
        })
    features = []
    for r in feat_rows:
        age = _age_seconds(r["last_feature"])
        features.append({
            "symbol": r["symbol"],
            "last_feature": r["last_feature"].isoformat(),
            "age_seconds": age,
            "color": _bar_color(r["symbol"], age),
        })
    model_rows = []
    for m in models:
        age_h = _age_seconds(m["trained_at"]) / 3600
        metrics = m.get("metrics") or {}
        model_rows.append({
            "model_id": m["model_id"],
            "label": (
                f"{m['model_type']} \u00b7 {m['symbol']} \u00b7 "
                f"{m['timeframe']} \u00b7 {m['horizon_minutes']}m"
            ),
            "trained_at": m["trained_at"].isoformat(),
            "age_hours": age_h,
            "age_color": _model_age_color(age_h),
            "status": m["status"],
            "accuracy": metrics.get("accuracy"),
            "f1_macro": metrics.get("f1_macro"),
        })

    return web.json_response({
        "bars": bars,
        "features": features,
        "models": model_rows,
    })
