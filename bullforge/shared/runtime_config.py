from __future__ import annotations

import json
import logging
from typing import Any

from shared.redis import RedisClient

logger = logging.getLogger(__name__)

CONFIG_HASH_KEY = "bullforge:config"

CONFIG_SCHEMA: dict[str, dict[str, Any]] = {
    "retrain_schedule_hour": {
        "default": 1, "type": "int", "min": 0, "max": 23,
        "label": "Retrain Hour (UTC)",
        "description": "Hour of day (0-23 UTC) when nightly model retraining begins. Default 1 AM UTC = 9 PM EDT.",
        "reference": "https://en.wikipedia.org/wiki/Training,_validation,_and_test_data_sets",
    },
    "retrain_cpu_workers": {
        "default": 15, "type": "int", "min": 1, "max": 20,
        "label": "CPU Workers for Training",
        "description": "Number of parallel processes for gradient boosting model training. Each worker trains one model at a time. Higher values train more models simultaneously but increase memory usage.",
        "reference": "https://en.wikipedia.org/wiki/Parallel_computing",
    },
    "lstm_epochs": {
        "default": 50, "type": "int", "min": 1, "max": 500,
        "label": "LSTM Training Epochs",
        "description": "Number of complete passes through the training dataset for LSTM neural network models. More epochs can improve accuracy but risk overfitting.",
        "reference": "https://en.wikipedia.org/wiki/Epoch_(machine_learning)",
    },
    "lstm_hidden_size": {
        "default": 64, "type": "int", "min": 16, "max": 512,
        "label": "LSTM Hidden Layer Size",
        "description": "Number of hidden units in each LSTM layer. Larger values increase model capacity but require more memory.",
        "reference": "https://en.wikipedia.org/wiki/Long_short-term_memory",
    },
    "lstm_sequence_length": {
        "default": 30, "type": "int", "min": 5, "max": 200,
        "label": "LSTM Lookback Window",
        "description": "Number of consecutive time steps fed into the LSTM as input context.",
        "reference": "https://en.wikipedia.org/wiki/Sliding_window_protocol",
    },
    "lstm_learning_rate": {
        "default": 0.001, "type": "float", "min": 0.00001, "max": 0.1,
        "label": "LSTM Learning Rate",
        "description": "Step size for the Adam optimizer during LSTM training. Smaller values train more slowly but converge more reliably.",
        "reference": "https://en.wikipedia.org/wiki/Learning_rate",
    },
    "lstm_batch_size": {
        "default": 64, "type": "int", "min": 8, "max": 512,
        "label": "LSTM Batch Size",
        "description": "Number of training samples processed together in each gradient update.",
        "reference": "https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Mini-batch_gradient_descent",
    },
    "trading_symbols_equity": {
        "default": ["SPY", "QQQ", "AAPL", "MSFT", "NVDA"],
        "type": "json",
        "label": "Equity Symbols",
        "description": "Equity/ETF tickers to trade. Adding a symbol triggers historical backfill and live stream subscription. Removing stops streaming (historical data retained).",
        "reference": "https://en.wikipedia.org/wiki/Ticker_symbol",
    },
    "trading_symbols_crypto": {
        "default": ["BTC/USD", "ETH/USD"],
        "type": "json",
        "label": "Crypto Pairs",
        "description": "Crypto trading pairs (base/quote format). Trades 24/7.",
        "reference": "https://en.wikipedia.org/wiki/Cryptocurrency",
    },
    "last_retrain_time": {
        "default": "", "type": "str",
        "label": "Last Retrain Time (UTC ISO)",
        "description": "ISO-8601 UTC timestamp of the most recent completed retrain cycle. Read-only from user perspective; written by the scheduler.",
        "reference": "https://en.wikipedia.org/wiki/ISO_8601",
    },
}


def _parse(value: str, type_: str) -> Any:
    if type_ == "int":
        return int(value)
    if type_ == "float":
        return float(value)
    if type_ == "json":
        return json.loads(value)
    return value


def _serialize(value: Any, type_: str) -> str:
    if type_ == "json":
        return json.dumps(value)
    return str(value)


class RuntimeConfig:
    def __init__(self, redis_client: RedisClient):
        self._redis = redis_client

    async def get(self, key: str) -> Any:
        if key not in CONFIG_SCHEMA:
            raise KeyError(f"unknown config key: {key}")
        meta = CONFIG_SCHEMA[key]
        raw = await self._redis.client.hget(CONFIG_HASH_KEY, key)
        if raw is None:
            return meta["default"]
        return _parse(raw, meta["type"])

    async def set(self, key: str, value: Any) -> None:
        if key not in CONFIG_SCHEMA:
            raise KeyError(f"unknown config key: {key}")
        meta = CONFIG_SCHEMA[key]
        if "min" in meta and value < meta["min"]:
            raise ValueError(
                f"{key}={value} must be between {meta['min']} and {meta['max']}"
            )
        if "max" in meta and value > meta["max"]:
            raise ValueError(
                f"{key}={value} must be between {meta['min']} and {meta['max']}"
            )
        await self._redis.client.hset(
            CONFIG_HASH_KEY, key, _serialize(value, meta["type"])
        )

    async def get_all(self) -> dict[str, Any]:
        stored = await self._redis.client.hgetall(CONFIG_HASH_KEY)
        result: dict[str, Any] = {}
        for key, meta in CONFIG_SCHEMA.items():
            if key in stored:
                result[key] = _parse(stored[key], meta["type"])
            else:
                result[key] = meta["default"]
        return result

    def get_schema(self) -> dict[str, dict[str, Any]]:
        return CONFIG_SCHEMA
