import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from shared.models import Bar
from services.data_pipeline.alpaca_client import AlpacaClient
from shared.config import Settings


@pytest.fixture
def mock_settings(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test_secret")
    return Settings()


def test_alpaca_client_creation(mock_settings):
    client = AlpacaClient(mock_settings)
    assert client is not None


def test_parse_bar_to_model(mock_settings):
    """AlpacaClient should convert Alpaca bar objects to our Bar model."""
    client = AlpacaClient(mock_settings)

    mock_alpaca_bar = MagicMock()
    mock_alpaca_bar.timestamp = datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc)
    mock_alpaca_bar.open = 420.10
    mock_alpaca_bar.high = 420.50
    mock_alpaca_bar.low = 419.80
    mock_alpaca_bar.close = 420.30
    mock_alpaca_bar.volume = 15000

    bar = client.parse_bar("SPY", mock_alpaca_bar, timeframe="1m")
    assert isinstance(bar, Bar)
    assert bar.symbol == "SPY"
    assert bar.timestamp.tzinfo == timezone.utc
    assert bar.close == 420.30
    assert bar.timeframe == "1m"


def test_parse_bar_converts_naive_timestamp_to_utc(mock_settings):
    """If Alpaca returns a naive timestamp, convert it to UTC."""
    client = AlpacaClient(mock_settings)

    mock_alpaca_bar = MagicMock()
    mock_alpaca_bar.timestamp = datetime(2026, 4, 10, 15, 30, 0)  # naive
    mock_alpaca_bar.open = 420.10
    mock_alpaca_bar.high = 420.50
    mock_alpaca_bar.low = 419.80
    mock_alpaca_bar.close = 420.30
    mock_alpaca_bar.volume = 15000

    bar = client.parse_bar("SPY", mock_alpaca_bar, timeframe="1m")
    assert bar.timestamp.tzinfo == timezone.utc


def test_parse_bar_preserves_fractional_volume(mock_settings):
    """Crypto bars carry fractional base-asset volume (e.g. 0.7 BTC).
    parse_bar must not truncate to int — that drops sub-1.0 BTC bars
    to volume=0 and feeds a falsely-zero volume into the fill cap and
    volume features."""
    client = AlpacaClient(mock_settings)

    mock_alpaca_bar = MagicMock()
    mock_alpaca_bar.timestamp = datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc)
    mock_alpaca_bar.open = 70000.0
    mock_alpaca_bar.high = 70100.0
    mock_alpaca_bar.low = 69950.0
    mock_alpaca_bar.close = 70050.0
    mock_alpaca_bar.volume = 0.7

    bar = client.parse_bar("BTC/USD", mock_alpaca_bar, timeframe="1m")
    assert bar.volume == 0.7
    assert isinstance(bar.volume, float)
