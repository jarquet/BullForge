import pytest
import pytest_asyncio
from shared.config import Settings
from shared.redis import RedisClient


@pytest_asyncio.fixture
async def redis_client():
    settings = Settings()
    client = RedisClient(settings)
    await client.connect()
    yield client
    await client.disconnect()


@pytest.mark.asyncio
async def test_connect_and_disconnect():
    settings = Settings()
    client = RedisClient(settings)
    await client.connect()
    assert client.client is not None
    await client.disconnect()


@pytest.mark.asyncio
async def test_publish_and_subscribe(redis_client):
    received = []

    async def handler(message: str):
        received.append(message)

    await redis_client.subscribe("test:channel", handler)
    await redis_client.publish("test:channel", '{"test": "data"}')

    # Give the subscriber a moment to receive
    import asyncio
    await asyncio.sleep(0.1)

    await redis_client.unsubscribe("test:channel")
    assert len(received) == 1
    assert received[0] == '{"test": "data"}'
