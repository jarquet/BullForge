import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from services.dashboard_service.freshness_api import register_freshness_routes


def _build_app(db):
    app = web.Application()
    app["db"] = db
    register_freshness_routes(app)
    return app


@pytest.mark.asyncio
async def test_freshness_endpoint():
    now = datetime.now(timezone.utc)
    db = AsyncMock()
    db.query_latest_bar_timestamps = AsyncMock(return_value=[
        {"symbol": "SPY", "last_bar": now - timedelta(minutes=2)},
        {"symbol": "BTC/USD", "last_bar": now - timedelta(seconds=30)},
    ])
    db.query_latest_feature_timestamps = AsyncMock(return_value=[
        {"symbol": "SPY", "last_feature": now - timedelta(minutes=3)},
    ])
    db.query_model_details = AsyncMock(return_value=[
        {
            "model_id": 1, "symbol": "SPY",
            "model_type": "gradient_boost",
            "timeframe": "1m", "horizon_minutes": 5,
            "trained_at": now - timedelta(hours=12),
            "status": "active",
            "metrics": {"accuracy": 0.6, "f1_macro": 0.58},
            "reason": "",
        },
    ])

    async with TestServer(_build_app(db)) as server:
        async with TestClient(server) as client:
            resp = await client.get("/api/freshness")
            assert resp.status == 200
            body = await resp.json()
            assert "bars" in body and "features" in body and "models" in body
            spy_bar = next(b for b in body["bars"] if b["symbol"] == "SPY")
            assert spy_bar["color"] == "green"  # <5m for equity
            btc_bar = next(b for b in body["bars"] if b["symbol"] == "BTC/USD")
            assert btc_bar["color"] == "green"  # <2m for crypto

            m = body["models"][0]
            assert m["age_color"] == "green"
            assert m["label"] == (
                "gradient_boost \u00b7 SPY \u00b7 1m \u00b7 5m"
            )


@pytest.mark.asyncio
async def test_freshness_stale_bars_colors():
    now = datetime.now(timezone.utc)
    db = AsyncMock()
    db.query_latest_bar_timestamps = AsyncMock(return_value=[
        {"symbol": "SPY", "last_bar": now - timedelta(minutes=10)},
        {"symbol": "QQQ", "last_bar": now - timedelta(minutes=20)},
        {"symbol": "BTC/USD", "last_bar": now - timedelta(minutes=6)},
    ])
    db.query_latest_feature_timestamps = AsyncMock(return_value=[])
    db.query_model_details = AsyncMock(return_value=[])

    async with TestServer(_build_app(db)) as server:
        async with TestClient(server) as client:
            body = await (await client.get("/api/freshness")).json()
            colors = {b["symbol"]: b["color"] for b in body["bars"]}
            assert colors["SPY"] == "yellow"  # 10m equity → 5-15m yellow
            assert colors["QQQ"] == "red"     # 20m equity → >15m red
            assert colors["BTC/USD"] == "red"  # 6m crypto → >5m red
