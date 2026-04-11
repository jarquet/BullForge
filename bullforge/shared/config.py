from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import time
from typing import List


def _parse_csv(env_key: str, default: str) -> List[str]:
    raw = os.getenv(env_key, default)
    return [s.strip() for s in raw.split(",") if s.strip()]


def _parse_time(env_key: str, default: str) -> time:
    raw = os.getenv(env_key, default)
    parts = raw.split(":")
    return time(hour=int(parts[0]), minute=int(parts[1]))


@dataclass(frozen=True)
class Settings:
    # Alpaca
    alpaca_api_key: str = field(
        default_factory=lambda: os.getenv("ALPACA_API_KEY", "")
    )
    alpaca_secret_key: str = field(
        default_factory=lambda: os.getenv("ALPACA_SECRET_KEY", "")
    )
    alpaca_base_url: str = field(
        default_factory=lambda: os.getenv(
            "ALPACA_BASE_URL", "https://paper-api.alpaca.markets"
        )
    )
    alpaca_data_url: str = field(
        default_factory=lambda: os.getenv(
            "ALPACA_DATA_URL", "https://data.alpaca.markets"
        )
    )
    alpaca_stream_url: str = field(
        default_factory=lambda: os.getenv(
            "ALPACA_STREAM_URL", "wss://stream.data.alpaca.markets"
        )
    )

    # Database
    timescaledb_host: str = field(
        default_factory=lambda: os.getenv("TIMESCALEDB_HOST", "timescaledb")
    )
    timescaledb_port: int = field(
        default_factory=lambda: int(os.getenv("TIMESCALEDB_PORT", "5432"))
    )
    timescaledb_user: str = field(
        default_factory=lambda: os.getenv("TIMESCALEDB_USER", "bullforge")
    )
    timescaledb_password: str = field(
        default_factory=lambda: os.getenv("TIMESCALEDB_PASSWORD", "bullforge_dev")
    )
    timescaledb_db: str = field(
        default_factory=lambda: os.getenv("TIMESCALEDB_DB", "bullforge")
    )

    # Redis
    redis_host: str = field(
        default_factory=lambda: os.getenv("REDIS_HOST", "redis")
    )
    redis_port: int = field(
        default_factory=lambda: int(os.getenv("REDIS_PORT", "6379"))
    )

    # Trading symbols
    trading_symbols_equity: List[str] = field(
        default_factory=lambda: _parse_csv(
            "TRADING_SYMBOLS_EQUITY", "SPY,QQQ,AAPL,MSFT,NVDA"
        )
    )
    trading_symbols_crypto: List[str] = field(
        default_factory=lambda: _parse_csv(
            "TRADING_SYMBOLS_CRYPTO", "BTC/USD,ETH/USD"
        )
    )

    # Trading window (UTC times)
    trading_window_start: time = field(
        default_factory=lambda: _parse_time("TRADING_WINDOW_START", "11:30")
    )
    trading_window_end: time = field(
        default_factory=lambda: _parse_time("TRADING_WINDOW_END", "14:00")
    )
