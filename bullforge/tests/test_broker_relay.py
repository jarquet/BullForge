import json
import pytest
from unittest.mock import AsyncMock
from services.execution_service.broker_relay import BrokerRelay


@pytest.mark.asyncio
async def test_handle_get_positions():
    redis = AsyncMock()
    broker = AsyncMock()
    broker.get_positions = AsyncMock(return_value=[
        {"symbol": "SPY", "qty": 10, "avg_entry_price": 420.0,
         "current_price": 421.0, "market_value": 4210.0, "unrealized_pl": 10.0},
    ])
    relay = BrokerRelay(redis_client=redis, broker=broker)

    msg = json.dumps({"method": "get_positions", "correlation_id": "abc-123"})
    await relay.handle_request(msg)

    redis.publish.assert_awaited_once()
    channel, payload = redis.publish.call_args[0]
    assert channel == "response:broker:abc-123"
    data = json.loads(payload)
    assert data["ok"] is True
    assert data["result"][0]["symbol"] == "SPY"


@pytest.mark.asyncio
async def test_handle_get_account():
    redis = AsyncMock()
    broker = AsyncMock()
    broker.get_account = AsyncMock(return_value={
        "equity": 100000.0, "buying_power": 200000.0,
        "cash": 20000.0, "portfolio_value": 100000.0,
    })
    relay = BrokerRelay(redis_client=redis, broker=broker)
    msg = json.dumps({"method": "get_account", "correlation_id": "xyz"})
    await relay.handle_request(msg)

    _, payload = redis.publish.call_args[0]
    data = json.loads(payload)
    assert data["result"]["equity"] == 100000.0


@pytest.mark.asyncio
async def test_unknown_method_returns_error():
    redis = AsyncMock()
    broker = AsyncMock()
    relay = BrokerRelay(redis_client=redis, broker=broker)
    msg = json.dumps({"method": "delete_everything", "correlation_id": "c1"})
    await relay.handle_request(msg)
    data = json.loads(redis.publish.call_args[0][1])
    assert data["ok"] is False
    assert "unknown method" in data["error"].lower()


@pytest.mark.asyncio
async def test_broker_exception_surfaces_as_error():
    redis = AsyncMock()
    broker = AsyncMock()
    broker.get_account = AsyncMock(side_effect=Exception("alpaca 503"))
    relay = BrokerRelay(redis_client=redis, broker=broker)
    msg = json.dumps({"method": "get_account", "correlation_id": "c2"})
    await relay.handle_request(msg)
    data = json.loads(redis.publish.call_args[0][1])
    assert data["ok"] is False
    assert "alpaca 503" in data["error"]
