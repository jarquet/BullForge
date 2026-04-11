import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, TestClient
from services.data_pipeline.health import create_health_app


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.pool = MagicMock()
    db.pool.get_size.return_value = 5
    db.pool.get_idle_size.return_value = 3
    return db


@pytest.fixture
def mock_redis():
    redis = AsyncMock()
    redis.client = AsyncMock()
    redis.client.ping = AsyncMock(return_value=True)
    return redis


@pytest_asyncio.fixture
async def health_client(aiohttp_client, mock_db, mock_redis):
    app = create_health_app(db=mock_db, redis_client=mock_redis)
    return await aiohttp_client(app)


@pytest.mark.asyncio
async def test_health_check_healthy(health_client):
    resp = await health_client.get("/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "healthy"
    assert "db" in data
    assert "redis" in data


@pytest.mark.asyncio
async def test_health_check_db_down(aiohttp_client):
    mock_db = AsyncMock()
    mock_db.pool = None
    mock_redis = AsyncMock()
    mock_redis.client = AsyncMock()
    mock_redis.client.ping = AsyncMock(return_value=True)

    app = create_health_app(db=mock_db, redis_client=mock_redis)
    client = await aiohttp_client(app)

    resp = await client.get("/health")
    assert resp.status == 503
    data = await resp.json()
    assert data["status"] == "unhealthy"
