import os
import pytest
from shared.config import Settings


def test_settings_loads_defaults():
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
