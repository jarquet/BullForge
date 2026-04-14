import pytest
from unittest.mock import AsyncMock
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from services.dashboard_service.config_api import register_config_routes
from shared.runtime_config import CONFIG_SCHEMA


@pytest.fixture
def app():
    app = web.Application()
    rc = AsyncMock()
    rc.get_all = AsyncMock(return_value={
        k: v["default"] for k, v in CONFIG_SCHEMA.items()
    })
    rc.get_schema = lambda: CONFIG_SCHEMA
    rc.set = AsyncMock()
    app["runtime_config"] = rc
    register_config_routes(app)
    return app


@pytest.mark.asyncio
async def test_get_config_returns_values_and_schema(app):
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await client.get("/api/config")
            assert resp.status == 200
            body = await resp.json()
            assert "values" in body
            assert "schema" in body
            assert body["values"]["retrain_schedule_hour"] == 1
            assert "label" in body["schema"]["retrain_schedule_hour"]


@pytest.mark.asyncio
async def test_put_config_updates_single_key(app):
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await client.put(
                "/api/config/retrain_schedule_hour",
                json={"value": 3},
            )
            assert resp.status == 200
            app["runtime_config"].set.assert_awaited_once_with(
                "retrain_schedule_hour", 3,
            )


@pytest.mark.asyncio
async def test_put_config_rejects_out_of_range(app):
    app["runtime_config"].set = AsyncMock(
        side_effect=ValueError("must be between 0 and 23")
    )
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await client.put(
                "/api/config/retrain_schedule_hour",
                json={"value": 99},
            )
            assert resp.status == 400
            body = await resp.json()
            assert "must be between" in body["error"]


@pytest.mark.asyncio
async def test_put_config_rejects_unknown_key(app):
    app["runtime_config"].set = AsyncMock(
        side_effect=KeyError("unknown config key")
    )
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await client.put(
                "/api/config/bogus",
                json={"value": 1},
            )
            assert resp.status == 404
