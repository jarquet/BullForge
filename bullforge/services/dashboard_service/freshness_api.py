from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from aiohttp import web

from shared.market_hours import is_market_open, last_market_close
from shared.symbol_utils import is_crypto


def _age_seconds(ts: datetime) -> float:
    return (datetime.now(timezone.utc) - ts).total_seconds()


def _bar_color(
    symbol: str,
    last_bar: datetime,
    now: datetime,
    market_open_now: bool,
    last_close: Optional[datetime],
) -> str:
    age = (now - last_bar).total_seconds()
    if is_crypto(symbol):
        if age < 120:
            return "green"
        if age < 300:
            return "yellow"
        return "red"
    if market_open_now:
        if age < 300:
            return "green"
        if age < 900:
            return "yellow"
        return "red"
    if last_close is None:
        if age < 900:
            return "green"
        if age < 3600:
            return "yellow"
        return "red"
    gap = max(0.0, (last_close - last_bar).total_seconds())
    if gap < 900:
        return "green"
    if gap < 3600:
        return "yellow"
    return "red"


def _model_age_color(age_hours: float) -> str:
    if age_hours < 24:
        return "green"
    if age_hours < 48:
        return "yellow"
    return "red"


async def _configured_symbols(runtime_config) -> set[str]:
    equity = await runtime_config.get("trading_symbols_equity") or []
    crypto = await runtime_config.get("trading_symbols_crypto") or []
    return set(equity) | set(crypto)


def register_freshness_routes(app: web.Application) -> None:
    app.router.add_get("/api/freshness", _get_freshness)


async def _get_freshness(request: web.Request) -> web.Response:
    db = request.app["db"]
    runtime_config = request.app["runtime_config"]
    bar_rows = await db.query_latest_bar_timestamps()
    feat_rows = await db.query_latest_feature_timestamps()
    models = await db.query_model_details()

    now = datetime.now(timezone.utc)
    market_open_now = is_market_open(now)
    last_close = last_market_close(now)
    configured = await _configured_symbols(runtime_config)

    def _row(symbol: str, ts: datetime) -> dict:
        age = (now - ts).total_seconds()
        disabled = symbol not in configured
        color = "disabled" if disabled else _bar_color(
            symbol, ts, now, market_open_now, last_close
        )
        return {
            "symbol": symbol,
            "age_seconds": age,
            "color": color,
            "disabled": disabled,
        }

    bars = []
    for r in bar_rows:
        row = _row(r["symbol"], r["last_bar"])
        row["last_bar"] = r["last_bar"].isoformat()
        bars.append(row)
    features = []
    for r in feat_rows:
        row = _row(r["symbol"], r["last_feature"])
        row["last_feature"] = r["last_feature"].isoformat()
        features.append(row)
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
