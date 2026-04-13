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


def test_execution_defaults(settings):
    assert settings.execution_max_position_value == 10000.0
    assert settings.execution_max_positions == 5
    assert settings.execution_max_exposure == 0.5
    assert settings.execution_min_signal_strength == 0.5
    assert settings.execution_min_confidence == 0.6
    assert settings.execution_min_agreement == 2


def test_execution_env_override(monkeypatch):
    monkeypatch.setenv("EXECUTION_MAX_POSITION_VALUE", "20000.0")
    monkeypatch.setenv("EXECUTION_MAX_POSITIONS", "10")
    monkeypatch.setenv("EXECUTION_MAX_EXPOSURE", "0.8")
    monkeypatch.setenv("EXECUTION_MIN_SIGNAL_STRENGTH", "0.7")
    monkeypatch.setenv("EXECUTION_MIN_CONFIDENCE", "0.75")
    monkeypatch.setenv("EXECUTION_MIN_AGREEMENT", "3")
    from shared.config import Settings
    s = Settings()
    assert s.execution_max_position_value == 20000.0
    assert s.execution_max_positions == 10
    assert s.execution_max_exposure == 0.8
    assert s.execution_min_signal_strength == 0.7
    assert s.execution_min_confidence == 0.75
    assert s.execution_min_agreement == 3


def test_monitoring_settings_defaults(settings):
    assert settings.monitoring_port == 8083
    assert settings.monitoring_interval == 60
    assert settings.fidelity_warning_margin == 0.05
    assert settings.fidelity_critical_margin == 0.10
    assert settings.fidelity_min_trades == 20
    assert settings.drawdown_halt_threshold == 0.10
