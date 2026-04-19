import pytest
from aiohttp import web

from services.dashboard_service.stubs_api import register_stub_routes, STUB_ENDPOINTS


@pytest.fixture
def stub_app():
    app = web.Application()
    register_stub_routes(app)
    return app


@pytest.mark.asyncio
async def test_every_stub_returns_501(aiohttp_client, stub_app):
    client = await aiohttp_client(stub_app)
    for path, meta in STUB_ENDPOINTS.items():
        concrete = (
            path.replace("{id}", "1")
                .replace("{sym}", "AAPL")
                .replace("{run_id}", "1")
        )
        resp = await client.get(concrete)
        assert resp.status == 501, f"{concrete} returned {resp.status}"
        body = await resp.json()
        assert body["error"] == "not_implemented"
        assert body["enabled_after_plan"] == meta["enabled_after_plan"]


@pytest.mark.asyncio
async def test_stubs_include_every_spec_endpoint():
    expected_prefixes: set = set()
    assert expected_prefixes <= set(STUB_ENDPOINTS)
