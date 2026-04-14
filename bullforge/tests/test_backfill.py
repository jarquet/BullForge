import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from shared.config import Settings
from shared.models import Bar
from services.data_pipeline.backfill import Backfiller


@pytest.fixture
def mock_settings(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test_secret")
    monkeypatch.setenv("TRADING_SYMBOLS_EQUITY", "SPY")
    monkeypatch.setenv("TRADING_SYMBOLS_CRYPTO", "BTC/USD")
    return Settings()


def _make_bars(symbol: str, count: int, timeframe: str) -> list[Bar]:
    base = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
    return [
        Bar(
            symbol=symbol,
            timestamp=base + timedelta(minutes=i),
            open=100.0 + i,
            high=101.0 + i,
            low=99.0 + i,
            close=100.5 + i,
            volume=1000 * (i + 1),
            timeframe=timeframe,
        )
        for i in range(count)
    ]


@pytest.mark.asyncio
async def test_backfill_fetches_and_stores_bars(mock_settings):
    mock_db = AsyncMock()
    mock_alpaca = MagicMock()
    mock_alpaca.get_historical_bars.return_value = _make_bars("SPY", 5, "1m")

    backfiller = Backfiller(
        settings=mock_settings, db=mock_db, alpaca_client=mock_alpaca
    )
    await backfiller.backfill_timeframe("SPY", timeframe="1m", days=1)

    mock_alpaca.get_historical_bars.assert_called_once()
    mock_db.insert_bars.assert_called_once()
    inserted_bars = mock_db.insert_bars.call_args[0][0]
    assert len(inserted_bars) == 5
    assert all(b.symbol == "SPY" for b in inserted_bars)


@pytest.mark.asyncio
async def test_backfill_all_symbols(mock_settings):
    mock_db = AsyncMock()
    mock_alpaca = MagicMock()
    mock_alpaca.get_historical_bars.side_effect = [
        _make_bars("SPY", 3, "1m"),
        _make_bars("SPY", 3, "1d"),
        _make_bars("BTC/USD", 3, "1m"),
        _make_bars("BTC/USD", 3, "1d"),
    ]

    backfiller = Backfiller(
        settings=mock_settings, db=mock_db, alpaca_client=mock_alpaca
    )
    await backfiller.backfill_all(days_1m=1, days_daily=365)

    assert mock_alpaca.get_historical_bars.call_count == 4
    assert mock_db.insert_bars.call_count == 4


@pytest.mark.asyncio
async def test_backfill_handles_empty_response(mock_settings):
    mock_db = AsyncMock()
    mock_alpaca = MagicMock()
    mock_alpaca.get_historical_bars.return_value = []

    backfiller = Backfiller(
        settings=mock_settings, db=mock_db, alpaca_client=mock_alpaca
    )
    await backfiller.backfill_timeframe("SPY", timeframe="1m", days=1)

    mock_db.insert_bars.assert_not_called()
