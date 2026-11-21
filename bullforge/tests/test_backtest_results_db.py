import json
import os
from datetime import datetime, timezone

import pytest

from shared.config import Settings
from shared.db import Database
from services.backtest_service.results_db import BacktestResultsDB


pytestmark = [pytest.mark.asyncio, pytest.mark.services]


UTC = timezone.utc


@pytest.fixture
async def db():
    settings = Settings()
    d = Database(settings)
    await d.connect()
    yield d
    await d.disconnect()


async def test_insert_run_and_fold(db):
    rdb = BacktestResultsDB(db)
    run_id = await rdb.insert_run(
        name="unit-test-run",
        symbols=["SPY"],
        timeframe="1m",
        horizons=[5, 10, 30],
        range_start=datetime(2025, 1, 1, tzinfo=UTC),
        range_end=datetime(2025, 1, 31, tzinfo=UTC),
        train_days=10, test_days=5, embargo_minutes=30, mode="rolling",
        config={"slippage_bps": 5.0},
    )
    assert run_id > 0

    fold_id = await rdb.insert_fold(
        run_id=run_id, fold_index=0,
        train_start=datetime(2025, 1, 1, tzinfo=UTC),
        train_end=datetime(2025, 1, 11, tzinfo=UTC),
        test_start=datetime(2025, 1, 11, 0, 30, tzinfo=UTC),
        test_end=datetime(2025, 1, 16, 0, 30, tzinfo=UTC),
    )
    assert fold_id > 0

    await rdb.insert_trade(
        run_id=run_id, fold_id=fold_id,
        symbol="SPY", side="long",
        entry_time=datetime(2025, 1, 11, 14, 0, tzinfo=UTC),
        entry_price=420.0, quantity=10,
        exit_time=datetime(2025, 1, 11, 15, 0, tzinfo=UTC),
        exit_price=421.0, pnl=10.0, signal_strength=0.7,
    )
    await rdb.insert_equity_point(
        run_id=run_id, fold_id=fold_id,
        timestamp=datetime(2025, 1, 11, 14, 0, tzinfo=UTC),
        equity=100_000.0, cash=95_800.0,
    )
    await rdb.update_fold_metrics(
        fold_id=fold_id, metrics={"sharpe": 1.5, "max_dd": -0.05},
    )
    await rdb.finish_run(run_id=run_id, status="completed")
