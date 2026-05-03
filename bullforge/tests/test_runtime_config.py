import pytest
from unittest.mock import AsyncMock
from shared.runtime_config import RuntimeConfig, CONFIG_SCHEMA, _parse, _serialize


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
        assert meta["type"] in ("int", "float", "str", "json", "bool")
        assert "label" in meta
        assert "description" in meta
        assert "reference" in meta


def test_schema_has_trading_window_buffers():
    assert "equity_window_open_buffer_minutes" in CONFIG_SCHEMA
    assert "equity_window_close_buffer_minutes" in CONFIG_SCHEMA
    assert CONFIG_SCHEMA["equity_window_open_buffer_minutes"]["default"] == 60
    assert CONFIG_SCHEMA["equity_window_close_buffer_minutes"]["default"] == 60


EXECUTION_KEYS = (
    "execution_min_confidence",
    "execution_min_agreement",
    "execution_direction_band",
    "execution_min_signal_strength",
    "execution_max_position_pct",
    "execution_max_positions",
    "execution_max_exposure",
)


def test_execution_keys_present_in_schema():
    for key in EXECUTION_KEYS:
        assert key in CONFIG_SCHEMA, f"{key} missing from CONFIG_SCHEMA"
        meta = CONFIG_SCHEMA[key]
        assert "default" in meta and "type" in meta
        assert meta.get("label"), f"{key} needs a label"
        assert meta.get("description"), f"{key} needs a description"


def test_execution_key_defaults_and_bounds():
    assert CONFIG_SCHEMA["execution_min_confidence"]["default"] == 0.6
    assert CONFIG_SCHEMA["execution_min_agreement"]["default"] == 2
    assert CONFIG_SCHEMA["execution_min_agreement"]["max"] == 10000
    assert CONFIG_SCHEMA["execution_direction_band"]["default"] == 0.3
    assert CONFIG_SCHEMA["execution_min_signal_strength"]["default"] == 0.5
    assert CONFIG_SCHEMA["execution_max_position_pct"]["default"] == 0.02
    assert CONFIG_SCHEMA["execution_max_positions"]["default"] == 5
    assert CONFIG_SCHEMA["execution_max_exposure"]["default"] == 0.2


def test_meta_config_keys_present():
    expected = {
        "meta_inference_enabled",
        "meta_estimator_types",
        "meta_pair_grid",
        "meta_train_window_days",
        "meta_max_hold_minutes",
        "meta_min_train_rows",
        "meta_label_backfill_interval_minutes",
        "meta_regime_max_staleness_hours",
        "meta_rolling_stats_ttl_seconds",
    }
    assert expected.issubset(CONFIG_SCHEMA.keys())


def test_meta_pair_grid_default_shape():
    grid = CONFIG_SCHEMA["meta_pair_grid"]["default"]
    assert isinstance(grid, list)
    assert len(grid) == 3
    for pair in grid:
        assert len(pair) == 2
        target_mult, stop_mult = pair
        assert target_mult > stop_mult > 0


def test_meta_inference_enabled_defaults_off():
    assert CONFIG_SCHEMA["meta_inference_enabled"]["default"] is False


@pytest.mark.parametrize(
    "value,serialized",
    [(True, "true"), (False, "false")],
)
def test_bool_type_round_trips(value, serialized):
    assert _serialize(value, "bool") == serialized
    assert _parse(serialized, "bool") is value
    assert _parse(_serialize(value, "bool"), "bool") is value


@pytest.mark.parametrize(
    "bad",
    ["yes", "no", "1", "0", "True", "False", "TRUE", "FALSE", ""],
)
def test_bool_type_rejects_invalid_strings(bad):
    with pytest.raises(ValueError):
        _parse(bad, "bool")


def test_bool_type_allowed_in_schema_validator():
    # The allowed-types tuple in test_schema_has_required_keys must include
    # "bool"; this test pins that intent independently.
    allowed = ("int", "float", "str", "json", "bool")
    assert "bool" in allowed
    for key, meta in CONFIG_SCHEMA.items():
        assert meta["type"] in allowed, f"{key} has unexpected type {meta['type']}"


def test_meta_inference_enabled_uses_bool_type():
    assert CONFIG_SCHEMA["meta_inference_enabled"]["type"] == "bool"


def test_meta_inference_enabled_existing_redis_values_round_trip():
    # Operators set this via `redis-cli HSET bullforge:config
    # meta_inference_enabled true` (per TODO.md). The new bool _parse
    # path must accept those raw strings without operator action.
    assert _parse("true", "bool") is True
    assert _parse("false", "bool") is False
