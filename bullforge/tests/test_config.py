import os
import pytest
from shared.config import Settings


def test_settings_loads_defaults(monkeypatch):
    monkeypatch.delenv("TIMESCALEDB_HOST", raising=False)
    monkeypatch.delenv("REDIS_HOST", raising=False)
    settings = Settings()
    assert settings.timescaledb_host == "timescaledb"
    assert settings.timescaledb_port == 5432
    assert settings.redis_host == "redis"
    assert settings.redis_port == 6379


def test_settings_loads_from_env(monkeypatch):
    monkeypatch.setenv("TIMESCALEDB_HOST", "localhost")
    monkeypatch.setenv("TIMESCALEDB_PORT", "5433")
    settings = Settings()
    assert settings.timescaledb_host == "localhost"
    assert settings.timescaledb_port == 5433


def test_settings_parses_symbol_lists(monkeypatch):
    monkeypatch.setenv("TRADING_SYMBOLS_EQUITY", "SPY,QQQ")
    monkeypatch.setenv("TRADING_SYMBOLS_CRYPTO", "BTC/USD,ETH/USD")
    settings = Settings()
    assert settings.trading_symbols_equity == ["SPY", "QQQ"]
    assert settings.trading_symbols_crypto == ["BTC/USD", "ETH/USD"]


def test_settings_parses_trading_window(monkeypatch):
    monkeypatch.setenv("TRADING_WINDOW_START", "11:30")
    monkeypatch.setenv("TRADING_WINDOW_END", "14:00")
    settings = Settings()
    assert settings.trading_window_start.hour == 11
    assert settings.trading_window_start.minute == 30
    assert settings.trading_window_end.hour == 14
    assert settings.trading_window_end.minute == 0


def test_model_service_defaults(settings):
    assert settings.model_artifacts_dir == "/app/models/artifacts"
    assert settings.model_min_samples == 500


def test_model_service_env_override(monkeypatch):
    monkeypatch.setenv("MODEL_ARTIFACTS_DIR", "/tmp/models")
    monkeypatch.setenv("MODEL_MIN_SAMPLES", "1000")
    from shared.config import Settings
    s = Settings()
    assert s.model_artifacts_dir == "/tmp/models"
    assert s.model_min_samples == 1000
