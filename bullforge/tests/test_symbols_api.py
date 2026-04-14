import pytest
from unittest.mock import AsyncMock
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from services.dashboard_service.symbols_api import register_symbol_routes


@pytest.fixture
def app():
    app = web.Application()
    state = {
        "trading_symbols_equity": ["SPY", "QQQ"],
        "trading_symbols_crypto": ["BTC/USD"],
    }
    rc = AsyncMock()

    async def rc_get(key):
        return list(state[key])

    async def rc_set(key, value):
        state[key] = list(value)

    rc.get = AsyncMock(side_effect=rc_get)
    rc.set = AsyncMock(side_effect=rc_set)
    app["runtime_config"] = rc

    redis = AsyncMock()
    redis.publish = AsyncMock()
    app["redis_client"] = redis

    db = AsyncMock()
    db.query_symbol_bar_stats = AsyncMock(return_value={
        "symbol": "AAPL", "bar_count": 0, "last_bar": None,
    })
    app["db"] = db

    register_symbol_routes(app)
    return app


@pytest.mark.asyncio
async def test_get_symbols_lists_both_groups(app):
    async with TestServer(app) as server, TestClient(server) as client:
        r = await client.get("/api/symbols")
        body = await r.json()
        assert body == {"equity": ["SPY", "QQQ"], "crypto": ["BTC/USD"]}


@pytest.mark.asyncio
async def test_add_equity_publishes_event_and_updates_rc(app):
    async with TestServer(app) as server, TestClient(server) as client:
        r = await client.post(
            "/api/symbols", json={"symbol": "AAPL", "kind": "equity"},
        )
        assert r.status == 200
        app["runtime_config"].set.assert_awaited()
        (key, val), _ = app["runtime_config"].set.call_args
        assert key == "trading_symbols_equity"
        assert "AAPL" in val
        app["redis_client"].publish.assert_awaited()


@pytest.mark.asyncio
async def test_add_crypto_requires_slash(app):
    async with TestServer(app) as server, TestClient(server) as client:
        r = await client.post(
            "/api/symbols", json={"symbol": "DOGE", "kind": "crypto"},
        )
        assert r.status == 400


@pytest.mark.asyncio
async def test_add_equity_rejects_slash(app):
    async with TestServer(app) as server, TestClient(server) as client:
        r = await client.post(
            "/api/symbols", json={"symbol": "BTC/USD", "kind": "equity"},
        )
        assert r.status == 400


@pytest.mark.asyncio
async def test_add_duplicate_noop(app):
    async with TestServer(app) as server, TestClient(server) as client:
        r = await client.post(
            "/api/symbols", json={"symbol": "SPY", "kind": "equity"},
        )
        body = await r.json()
        assert body["added"] is False
        app["redis_client"].publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_symbol(app):
    async with TestServer(app) as server, TestClient(server) as client:
        r = await client.delete("/api/symbols/SPY?kind=equity")
        assert r.status == 200
        (key, val), _ = app["runtime_config"].set.call_args
        assert key == "trading_symbols_equity"
        assert "SPY" not in val
        app["redis_client"].publish.assert_awaited()


@pytest.mark.asyncio
async def test_delete_requires_kind(app):
    async with TestServer(app) as server, TestClient(server) as client:
        r = await client.delete("/api/symbols/SPY")
        assert r.status == 400


@pytest.mark.asyncio
async def test_symbol_status_pending(app):
    async with TestServer(app) as server, TestClient(server) as client:
        r = await client.get("/api/symbols/AAPL/status")
        body = await r.json()
        assert body["status"] == "pending"
