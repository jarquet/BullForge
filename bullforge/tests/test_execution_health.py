import pytest
from unittest.mock import AsyncMock, MagicMock
from services.execution_service.health import create_health_app


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


@pytest.fixture
def mock_broker():
    broker = AsyncMock()
    broker.get_account = AsyncMock(return_value={
        "equity": 100000.0,
        "buying_power": 200000.0,
        "cash": 100000.0,
        "portfolio_value": 100000.0,
    })
    return broker


@pytest.mark.asyncio
async def test_health_endpoint_ok(aiohttp_client, mock_db, mock_redis, mock_broker):
    app = create_health_app(mock_db, mock_redis, mock_broker)
    client = await aiohttp_client(app)

    resp = await client.get("/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["db"] == "ok"
    assert data["redis"] == "ok"
    assert data["alpaca"] == "ok"


@pytest.mark.asyncio
async def test_health_endpoint_degraded(aiohttp_client, mock_db, mock_redis, mock_broker):
    mock_broker.get_account = AsyncMock(side_effect=Exception("connection refused"))

    app = create_health_app(mock_db, mock_redis, mock_broker)
    client = await aiohttp_client(app)

    resp = await client.get("/health")
    assert resp.status == 503
    data = await resp.json()
    assert data["db"] == "ok"
    assert "error" in data["alpaca"]


@pytest.mark.asyncio
async def test_health_includes_stream_queue_dropped_when_stream_provided(
    aiohttp_client, mock_db, mock_redis, mock_broker
):
    stream = MagicMock()
    stream.queue_full_dropped = 7
    app = create_health_app(mock_db, mock_redis, mock_broker, stream=stream)
    client = await aiohttp_client(app)
    resp = await client.get("/health")
    body = await resp.json()
    assert body.get("stream_queue_dropped") == 7


@pytest.mark.asyncio
async def test_health_omits_stream_queue_dropped_when_no_stream(
    aiohttp_client, mock_db, mock_redis, mock_broker
):
    app = create_health_app(mock_db, mock_redis, mock_broker)
    client = await aiohttp_client(app)
    resp = await client.get("/health")
    body = await resp.json()
    assert "stream_queue_dropped" not in body
