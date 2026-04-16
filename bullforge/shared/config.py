from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, time
from typing import List


def _parse_csv(env_key: str, default: str) -> List[str]:
    raw = os.getenv(env_key, default)
    return [s.strip() for s in raw.split(",") if s.strip()]


def _parse_time(env_key: str, default: str) -> time:
    raw = os.getenv(env_key, default)
    parts = raw.split(":")
    return time(hour=int(parts[0]), minute=int(parts[1]))


def _parse_date(env_key: str, default: str) -> date:
    raw = os.getenv(env_key, default)
    return date.fromisoformat(raw)


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

    # Backfill floors (earliest historical bar dates from Alpaca)
    backfill_floor_equity: date = field(
        default_factory=lambda: _parse_date(
            "BACKFILL_FLOOR_EQUITY", "2016-01-01"
        )
    )
    backfill_floor_crypto: date = field(
        default_factory=lambda: _parse_date(
            "BACKFILL_FLOOR_CRYPTO", "2021-01-01"
        )
    )

    # Trading window (UTC times)
    trading_window_start: time = field(
        default_factory=lambda: _parse_time("TRADING_WINDOW_START", "11:30")
    )
    trading_window_end: time = field(
        default_factory=lambda: _parse_time("TRADING_WINDOW_END", "14:00")
    )

    # Model service
    model_artifacts_dir: str = field(
        default_factory=lambda: os.getenv("MODEL_ARTIFACTS_DIR", "/app/models/artifacts")
    )
    model_min_samples: int = field(
        default_factory=lambda: int(os.getenv("MODEL_MIN_SAMPLES", "500"))
    )
    model_min_samples_1d: int = field(
        default_factory=lambda: int(os.getenv("MODEL_MIN_SAMPLES_1D", "84"))
    )

    # Execution service
    execution_reconciler_poll_interval_s: int = field(
        default_factory=lambda: int(
            os.getenv("EXECUTION_RECONCILER_POLL_INTERVAL_S", "60")
        )
    )
    execution_reconciler_orphan_every: int = field(
        default_factory=lambda: int(
            os.getenv("EXECUTION_RECONCILER_ORPHAN_EVERY", "5")
        )
    )
    execution_reconciler_orphan_window_hours: int = field(
        default_factory=lambda: int(
            os.getenv("EXECUTION_RECONCILER_ORPHAN_WINDOW_HOURS", "48")
        )
    )
    execution_reconciler_stream_enabled: bool = field(
        default_factory=lambda: os.getenv(
            "EXECUTION_RECONCILER_STREAM_ENABLED", "true"
        ).lower() == "true"
    )

    # Monitoring service
    monitoring_port: int = field(
        default_factory=lambda: int(os.getenv("MONITORING_PORT", "8083"))
    )
    monitoring_interval: int = field(
        default_factory=lambda: int(os.getenv("MONITORING_INTERVAL", "60"))
    )
    fidelity_warning_margin: float = field(
        default_factory=lambda: float(os.getenv("FIDELITY_WARNING_MARGIN", "0.05"))
    )
    fidelity_critical_margin: float = field(
        default_factory=lambda: float(os.getenv("FIDELITY_CRITICAL_MARGIN", "0.10"))
    )
    fidelity_min_trades: int = field(
        default_factory=lambda: int(os.getenv("FIDELITY_MIN_TRADES", "20"))
    )
    drawdown_halt_threshold: float = field(
        default_factory=lambda: float(os.getenv("DRAWDOWN_HALT_THRESHOLD", "0.10"))
    )

    # Dashboard service
    dashboard_port: int = field(
        default_factory=lambda: int(os.getenv("DASHBOARD_PORT", "8084"))
    )

    # Telegram bot
    telegram_bot_token: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", "")
    )
    telegram_chat_id: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", "")
    )
