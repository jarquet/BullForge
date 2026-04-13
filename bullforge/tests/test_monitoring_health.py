import pytest
from unittest.mock import AsyncMock, MagicMock
from services.monitoring_service.health import create_health_app


@pytest.fixture
def mock_db():
    db = MagicMock()
    pool = MagicMock()
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=None)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx
    db.pool = pool
    return db


@pytest.fixture
def mock_redis():
    redis_client = MagicMock()
    redis_client.client = AsyncMock()
    redis_client.client.ping = AsyncMock(return_value=True)
    return redis_client


@pytest.mark.asyncio
async def test_health_endpoint_ok(aiohttp_client, mock_db, mock_redis):
    app = create_health_app(mock_db, mock_redis)
    client = await aiohttp_client(app)

    resp = await client.get("/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["db"] == "ok"
    assert data["redis"] == "ok"


@pytest.mark.asyncio
async def test_health_endpoint_db_failure(aiohttp_client, mock_db, mock_redis):
    conn = AsyncMock()
    conn.execute = AsyncMock(side_effect=Exception("connection refused"))
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    mock_db.pool.acquire.return_value = ctx

    app = create_health_app(mock_db, mock_redis)
    client = await aiohttp_client(app)

    resp = await client.get("/health")
    assert resp.status == 503
    data = await resp.json()
    assert "error" in data["db"]
    assert data["redis"] == "ok"
