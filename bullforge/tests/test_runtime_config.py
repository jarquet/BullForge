import pytest
from unittest.mock import AsyncMock
from shared.runtime_config import RuntimeConfig, CONFIG_SCHEMA


@pytest.fixture
def fake_redis():
    r = AsyncMock()
    r.client = AsyncMock()
    r.client.hget = AsyncMock(return_value=None)
    r.client.hset = AsyncMock(return_value=1)
    r.client.hgetall = AsyncMock(return_value={})
    return r


@pytest.mark.asyncio
async def test_get_returns_default_when_missing(fake_redis):
    cfg = RuntimeConfig(fake_redis)
    val = await cfg.get("retrain_schedule_hour")
    assert val == 1
    assert isinstance(val, int)


@pytest.mark.asyncio
async def test_get_parses_type_from_schema(fake_redis):
    fake_redis.client.hget = AsyncMock(return_value="0.005")
    cfg = RuntimeConfig(fake_redis)
    val = await cfg.get("lstm_learning_rate")
    assert val == 0.005
    assert isinstance(val, float)


@pytest.mark.asyncio
async def test_set_validates_range(fake_redis):
    cfg = RuntimeConfig(fake_redis)
    with pytest.raises(ValueError, match="must be between"):
        await cfg.set("retrain_schedule_hour", 24)


@pytest.mark.asyncio
async def test_set_validates_unknown_key(fake_redis):
    cfg = RuntimeConfig(fake_redis)
    with pytest.raises(KeyError, match="unknown config key"):
        await cfg.set("not_a_real_key", 5)


@pytest.mark.asyncio
async def test_set_writes_redis_hash(fake_redis):
    cfg = RuntimeConfig(fake_redis)
    await cfg.set("retrain_schedule_hour", 3)
    fake_redis.client.hset.assert_called_once_with(
        "bullforge:config", "retrain_schedule_hour", "3"
    )


@pytest.mark.asyncio
async def test_get_all_merges_defaults_and_stored(fake_redis):
    fake_redis.client.hgetall = AsyncMock(
        return_value={"retrain_schedule_hour": "5"}
    )
    cfg = RuntimeConfig(fake_redis)
    result = await cfg.get_all()
    assert result["retrain_schedule_hour"] == 5
    assert result["retrain_cpu_workers"] == 15
    assert result["lstm_epochs"] == 50


@pytest.mark.asyncio
async def test_symbols_stored_as_json(fake_redis):
    fake_redis.client.hget = AsyncMock(return_value='["SPY", "QQQ"]')
    cfg = RuntimeConfig(fake_redis)
    val = await cfg.get("trading_symbols_equity")
    assert val == ["SPY", "QQQ"]


@pytest.mark.asyncio
async def test_set_symbols_serializes_json(fake_redis):
    cfg = RuntimeConfig(fake_redis)
    await cfg.set("trading_symbols_equity", ["SPY", "AAPL", "MSFT"])
    args, _ = fake_redis.client.hset.call_args
    assert args[1] == "trading_symbols_equity"
    assert args[2] == '["SPY", "AAPL", "MSFT"]'


def test_schema_includes_symbols_and_last_retrain():
    assert CONFIG_SCHEMA["trading_symbols_equity"]["type"] == "json"
    assert CONFIG_SCHEMA["trading_symbols_crypto"]["type"] == "json"
    assert CONFIG_SCHEMA["last_retrain_time"]["type"] == "str"


def test_schema_has_required_keys():
    required = {
        "retrain_schedule_hour", "retrain_cpu_workers",
        "lstm_epochs", "lstm_hidden_size", "lstm_sequence_length",
        "lstm_learning_rate", "lstm_batch_size",
    }
    assert required.issubset(CONFIG_SCHEMA.keys())
    for key, meta in CONFIG_SCHEMA.items():
        assert "default" in meta
        assert "type" in meta
        assert meta["type"] in ("int", "float", "str", "json")
        assert "label" in meta
        assert "description" in meta
        assert "reference" in meta
