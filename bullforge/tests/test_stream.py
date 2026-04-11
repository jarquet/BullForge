import pytest
import pytest_asyncio
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, call
from shared.config import Settings
from shared.models import Bar
from services.data_pipeline.stream import TickStreamer


@pytest.fixture
def mock_settings(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test_secret")
    monkeypatch.setenv("TRADING_SYMBOLS_EQUITY", "SPY,QQQ")
    monkeypatch.setenv("TRADING_SYMBOLS_CRYPTO", "BTC/USD")
    return Settings()


def _make_alpaca_bar(symbol: str):
    bar = MagicMock()
    bar.symbol = symbol
    bar.timestamp = datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc)
    bar.open = 420.10
    bar.high = 420.50
    bar.low = 419.80
    bar.close = 420.30
    bar.volume = 15000
    return bar


@pytest.mark.asyncio
async def test_handle_bar_enqueues_and_process_bar_stores_and_publishes(mock_settings):
    mock_db = AsyncMock()
    mock_redis = AsyncMock()
    mock_alpaca = MagicMock()
    test_bar = Bar(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        open=420.10, high=420.50, low=419.80, close=420.30,
        volume=15000, timeframe="1m",
    )
    mock_alpaca.parse_bar.return_value = test_bar
    streamer = TickStreamer(
        settings=mock_settings, db=mock_db,
        redis_client=mock_redis, alpaca_client=mock_alpaca,
    )
    # handle_bar enqueues
    alpaca_bar = _make_alpaca_bar("SPY")
    await streamer.handle_bar(alpaca_bar)
    assert not streamer._queue.empty()

    # _process_bar does the actual DB/Redis work
    bar = streamer._queue.get()
    await streamer._process_bar(bar)

    mock_db.insert_bar.assert_called_once()
    mock_redis.publish.assert_called_once()
    publish_args = mock_redis.publish.call_args
    assert publish_args[0][0] == "market:ticks"
    assert '"SPY"' in publish_args[0][1]


@pytest.mark.asyncio
async def test_process_bar_publishes_valid_json(mock_settings):
    import json
    mock_db = AsyncMock()
    mock_redis = AsyncMock()
    mock_alpaca = MagicMock()
    streamer = TickStreamer(
        settings=mock_settings, db=mock_db,
        redis_client=mock_redis, alpaca_client=mock_alpaca,
    )
    bar = Bar(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        open=420.10, high=420.50, low=419.80, close=420.30,
        volume=15000, timeframe="1m",
    )
    await streamer._process_bar(bar)
    published_json = mock_redis.publish.call_args[0][1]
    data = json.loads(published_json)
    assert data["symbol"] == "SPY"
    assert data["close"] == 420.30
    assert "timestamp" in data
