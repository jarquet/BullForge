import asyncio
import json
import uuid
import pytest
from unittest.mock import AsyncMock, patch

from services.monitoring_service.broker_client import BrokerClient


@pytest.mark.asyncio
async def test_get_account_round_trip():
    redis = AsyncMock()
    subscribed = {}

    async def fake_subscribe(channel, handler):
        subscribed[channel] = handler

    redis.subscribe = fake_subscribe
    redis.unsubscribe = AsyncMock()

    async def fake_publish(channel, payload):
        data = json.loads(payload)
        cid = data["correlation_id"]
        # Simulate relay responding on the correlation channel
        await subscribed[f"response:broker:{cid}"](
            json.dumps({"ok": True, "result": {"equity": 123.0}})
        )
    redis.publish = fake_publish

    client = BrokerClient(redis_client=redis, timeout=1.0)
    result = await client.get_account()
    assert result == {"equity": 123.0}


@pytest.mark.asyncio
async def test_timeout_raises_broker_unavailable():
    redis = AsyncMock()
    redis.subscribe = AsyncMock()
    redis.unsubscribe = AsyncMock()
    redis.publish = AsyncMock()  # never triggers a response
    client = BrokerClient(redis_client=redis, timeout=0.1)
    with pytest.raises(TimeoutError):
        await client.get_account()
