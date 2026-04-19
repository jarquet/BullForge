import pytest
from unittest.mock import AsyncMock, MagicMock

from services.feature_engine.backfill_features import discover_symbols


def _mock_db_with_bar_symbols(
    minute_symbols: list[str], daily_symbols: list[str],
):
    pool = MagicMock()
    conn = AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx
    conn.fetch = AsyncMock(side_effect=[
        [{"symbol": s} for s in minute_symbols],
        [{"symbol": s} for s in daily_symbols],
    ])
    db = MagicMock()
    db.pool = pool
    return db


@pytest.mark.asyncio
async def test_discover_symbols_returns_both_timeframes():
    db = _mock_db_with_bar_symbols(
        minute_symbols=["AAPL", "BTC/USD", "SPY"],
        daily_symbols=["AAPL", "SPY"],  # BTC/USD has only 1m bars
    )
    result = await discover_symbols(db)
    assert result == {
        "1m": ["AAPL", "BTC/USD", "SPY"],
        "1d": ["AAPL", "SPY"],
    }


@pytest.mark.asyncio
async def test_discover_symbols_handles_empty_tables():
    db = _mock_db_with_bar_symbols(minute_symbols=[], daily_symbols=[])
    result = await discover_symbols(db)
    assert result == {"1m": [], "1d": []}
