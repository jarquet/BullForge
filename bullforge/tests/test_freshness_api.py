import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from services.dashboard_service import freshness_api as fa_mod
from services.dashboard_service.freshness_api import register_freshness_routes


def _build_app(db, runtime_config):
    app = web.Application()
    app["db"] = db
    app["runtime_config"] = runtime_config
    register_freshness_routes(app)
    return app


def _fake_runtime_config(equity=None, crypto=None):
    rc = AsyncMock()

    async def _get(key):
        if key == "trading_symbols_equity":
            return list(equity or [])
        if key == "trading_symbols_crypto":
            return list(crypto or [])
        return None

    rc.get = AsyncMock(side_effect=_get)
    return rc


@pytest.mark.asyncio
async def test_freshness_endpoint_market_open(monkeypatch):
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(fa_mod, "is_market_open", lambda *a, **k: True)
    monkeypatch.setattr(fa_mod, "last_market_close", lambda *a, **k: None)

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
    rc = _fake_runtime_config(equity=["SPY"], crypto=["BTC/USD"])

    async with TestServer(_build_app(db, rc)) as server:
        async with TestClient(server) as client:
            resp = await client.get("/api/freshness")
            assert resp.status == 200
            body = await resp.json()
            assert "bars" in body and "features" in body and "models" in body
            spy = next(b for b in body["bars"] if b["symbol"] == "SPY")
            btc = next(b for b in body["bars"] if b["symbol"] == "BTC/USD")
            assert spy["color"] == "green"
            assert spy["disabled"] is False
            assert btc["color"] == "green"

            m = body["models"][0]
            assert m["age_color"] == "green"
            assert m["label"] == (
                "gradient_boost \u00b7 SPY \u00b7 1m \u00b7 5m"
            )


@pytest.mark.asyncio
async def test_freshness_endpoint_market_open_stale(monkeypatch):
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(fa_mod, "is_market_open", lambda *a, **k: True)
    monkeypatch.setattr(fa_mod, "last_market_close", lambda *a, **k: None)

    db = AsyncMock()
    db.query_latest_bar_timestamps = AsyncMock(return_value=[
        {"symbol": "SPY", "last_bar": now - timedelta(minutes=10)},
        {"symbol": "QQQ", "last_bar": now - timedelta(minutes=20)},
        {"symbol": "BTC/USD", "last_bar": now - timedelta(minutes=6)},
    ])
    db.query_latest_feature_timestamps = AsyncMock(return_value=[])
    db.query_model_details = AsyncMock(return_value=[])
    rc = _fake_runtime_config(equity=["SPY", "QQQ"], crypto=["BTC/USD"])

    async with TestServer(_build_app(db, rc)) as server:
        async with TestClient(server) as client:
            body = await (await client.get("/api/freshness")).json()
            colors = {b["symbol"]: b["color"] for b in body["bars"]}
            assert colors["SPY"] == "yellow"
            assert colors["QQQ"] == "red"
            assert colors["BTC/USD"] == "red"


@pytest.mark.asyncio
async def test_freshness_endpoint_market_closed_uses_gap_to_last_close(monkeypatch):
    now = datetime.now(timezone.utc)
    last_close = now - timedelta(hours=2)
    monkeypatch.setattr(fa_mod, "is_market_open", lambda *a, **k: False)
    monkeypatch.setattr(fa_mod, "last_market_close", lambda *a, **k: last_close)

    db = AsyncMock()
    db.query_latest_bar_timestamps = AsyncMock(return_value=[
        {"symbol": "SPY", "last_bar": last_close - timedelta(minutes=1)},
        {"symbol": "QQQ", "last_bar": now - timedelta(days=5)},
    ])
    db.query_latest_feature_timestamps = AsyncMock(return_value=[])
    db.query_model_details = AsyncMock(return_value=[])
    rc = _fake_runtime_config(equity=["SPY", "QQQ"], crypto=[])

    async with TestServer(_build_app(db, rc)) as server:
        async with TestClient(server) as client:
            body = await (await client.get("/api/freshness")).json()
            colors = {b["symbol"]: b["color"] for b in body["bars"]}
            assert colors["SPY"] == "green"
            assert colors["QQQ"] == "red"


@pytest.mark.asyncio
async def test_freshness_endpoint_ghost_symbol_is_disabled(monkeypatch):
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(fa_mod, "is_market_open", lambda *a, **k: True)
    monkeypatch.setattr(fa_mod, "last_market_close", lambda *a, **k: None)

    db = AsyncMock()
    db.query_latest_bar_timestamps = AsyncMock(return_value=[
        {"symbol": "SPY", "last_bar": now - timedelta(minutes=2)},
        {"symbol": "AAPL", "last_bar": now - timedelta(days=5)},
    ])
    db.query_latest_feature_timestamps = AsyncMock(return_value=[
        {"symbol": "AAPL", "last_feature": now - timedelta(days=5)},
    ])
    db.query_model_details = AsyncMock(return_value=[])
    rc = _fake_runtime_config(equity=["SPY"], crypto=[])

    async with TestServer(_build_app(db, rc)) as server:
        async with TestClient(server) as client:
            body = await (await client.get("/api/freshness")).json()
            rows = {b["symbol"]: b for b in body["bars"]}
            feats = {f["symbol"]: f for f in body["features"]}
            assert rows["AAPL"]["color"] == "disabled"
            assert rows["AAPL"]["disabled"] is True
            assert rows["SPY"]["disabled"] is False
            assert feats["AAPL"]["color"] == "disabled"
            assert feats["AAPL"]["disabled"] is True
