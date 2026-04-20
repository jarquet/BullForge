from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.backtest_service.results_db import BacktestResultsDB


UTC = timezone.utc


def _mock_db():
    conn = AsyncMock()
    conn.executemany = AsyncMock()
    pool_ctx = MagicMock()
    pool_ctx.__aenter__ = AsyncMock(return_value=conn)
    pool_ctx.__aexit__ = AsyncMock(return_value=False)
    db = MagicMock()
    db.pool.acquire = MagicMock(return_value=pool_ctx)
    return db, conn


@pytest.mark.asyncio
async def test_insert_equity_points_many_batches_in_one_executemany():
    db, conn = _mock_db()
    rdb = BacktestResultsDB(db)
    points = [
        {
            "timestamp": datetime(2026, 4, 20, 14, i, tzinfo=UTC),
            "equity": 100_000.0 + i,
            "cash": 95_000.0 + i,
        }
        for i in range(5)
    ]
    await rdb.insert_equity_points_many(run_id=7, fold_id=11, points=points)

    conn.executemany.assert_awaited_once()
    sql, rows = conn.executemany.await_args.args
    assert "INSERT INTO backtest_equity" in sql
    assert "ON CONFLICT" in sql
    assert len(rows) == 5
    assert rows[0] == (7, 11, points[0]["timestamp"], 100_000.0, 95_000.0)


@pytest.mark.asyncio
async def test_insert_equity_points_many_noop_on_empty():
    db, conn = _mock_db()
    rdb = BacktestResultsDB(db)
    await rdb.insert_equity_points_many(run_id=7, fold_id=11, points=[])
    conn.executemany.assert_not_called()


@pytest.mark.asyncio
async def test_insert_trades_many_batches_in_one_executemany():
    db, conn = _mock_db()
    rdb = BacktestResultsDB(db)
    trades = [
        {
            "symbol": "SPY", "side": "long",
            "entry_time": datetime(2026, 4, 20, 14, 0, tzinfo=UTC),
            "entry_price": 420.0, "quantity": 10.0,
            "exit_time": datetime(2026, 4, 20, 15, 0, tzinfo=UTC),
            "exit_price": 421.0, "pnl": 10.0, "signal_strength": 0.7,
        },
        {
            "symbol": "BTC/USD", "side": "short",
            "entry_time": datetime(2026, 4, 20, 14, 1, tzinfo=UTC),
            "entry_price": 60000.0, "quantity": 0.001,
            "exit_time": None, "exit_price": None,
            "pnl": None, "signal_strength": 0.5,
        },
    ]
    await rdb.insert_trades_many(run_id=3, fold_id=4, trades=trades)

    conn.executemany.assert_awaited_once()
    sql, rows = conn.executemany.await_args.args
    assert "INSERT INTO backtest_trades" in sql
    assert len(rows) == 2
    assert rows[0][0] == 3
    assert rows[0][1] == 4
    assert rows[0][2] == "SPY"
    assert rows[1][2] == "BTC/USD"
    assert rows[1][7] is None  # exit_time
    assert rows[1][9] is None  # pnl


@pytest.mark.asyncio
async def test_insert_trades_many_noop_on_empty():
    db, conn = _mock_db()
    rdb = BacktestResultsDB(db)
    await rdb.insert_trades_many(run_id=3, fold_id=4, trades=[])
    conn.executemany.assert_not_called()
