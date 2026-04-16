from datetime import datetime, timezone
import pytest
from shared.config import Settings
from shared.db import Database
from services.backtest_service.results_db import BacktestResultsDB

pytestmark = pytest.mark.asyncio
UTC = timezone.utc


@pytest.fixture
async def seeded():
    d = Database(Settings()); await d.connect()
    r = BacktestResultsDB(d)
    run_id = await r.insert_run(
        name="query-test", symbols=["SPY"], timeframe="1m", horizons=[5],
        range_start=datetime(2025, 1, 1, tzinfo=UTC),
        range_end=datetime(2025, 1, 5, tzinfo=UTC),
        train_days=1, test_days=1, embargo_minutes=5, mode="rolling",
        config={},
    )
    fold_id = await r.insert_fold(
        run_id=run_id, fold_index=0,
        train_start=datetime(2025,1,1,tzinfo=UTC),
        train_end=datetime(2025,1,2,tzinfo=UTC),
        test_start=datetime(2025,1,2,tzinfo=UTC),
        test_end=datetime(2025,1,3,tzinfo=UTC),
    )
    await r.insert_trade(
        run_id=run_id, fold_id=fold_id, symbol="SPY", side="long",
        entry_time=datetime(2025,1,2,14,0,tzinfo=UTC), entry_price=100.0,
        quantity=1.0,
        exit_time=datetime(2025,1,2,15,0,tzinfo=UTC), exit_price=102.0,
        pnl=2.0, signal_strength=0.5,
    )
    await r.insert_trade(
        run_id=run_id, fold_id=fold_id, symbol="SPY", side="long",
        entry_time=datetime(2025,1,2,16,0,tzinfo=UTC), entry_price=102.0,
        quantity=1.0,
        exit_time=datetime(2025,1,2,17,0,tzinfo=UTC), exit_price=101.0,
        pnl=-1.0, signal_strength=0.3,
    )
    for (t, eq) in [
        (datetime(2025,1,2,14,0,tzinfo=UTC), 100000.0),
        (datetime(2025,1,2,15,0,tzinfo=UTC), 100002.0),
        (datetime(2025,1,2,17,0,tzinfo=UTC), 100001.0),
    ]:
        await r.insert_equity_point(
            run_id=run_id, fold_id=fold_id, timestamp=t, equity=eq, cash=eq,
        )
    await r.finish_run(run_id=run_id, status="completed")
    yield d, r, run_id, fold_id
    await d.disconnect()


async def test_list_runs_returns_summary(seeded):
    _, r, run_id, _ = seeded
    rows = await r.list_runs(status="completed", q="query", limit=10, offset=0,
                             sort="finished_at", order="desc")
    match = [row for row in rows if row["id"] == run_id]
    assert match
    row = match[0]
    assert row["metrics"]["n_trades"] == 2
    assert row["metrics"]["total_pnl"] == pytest.approx(1.0)
    assert row["metrics"]["hit_rate"] == pytest.approx(0.5)
    assert row["metrics"]["max_dd"] <= 0


async def test_get_run_returns_full_metrics(seeded):
    _, r, run_id, _ = seeded
    row = await r.get_run(run_id)
    assert row is not None
    m = row["metrics"]
    for key in ("total_pnl","sharpe","sortino","max_dd","hit_rate",
                "profit_factor","n_trades","n_folds"):
        assert key in m


async def test_get_run_returns_none_for_missing(seeded):
    _, r, *_ = seeded
    assert await r.get_run(-1) is None


async def test_list_folds(seeded):
    _, r, run_id, fold_id = seeded
    folds = await r.list_folds(run_id)
    assert len(folds) == 1
    f = folds[0]
    assert f["id"] == fold_id
    assert f["metrics"]["n_trades"] == 2
    assert f["metrics"]["total_pnl"] == pytest.approx(1.0)


async def test_list_trades_paginated(seeded):
    _, r, run_id, _ = seeded
    trades = await r.list_trades(run_id, limit=1, offset=0,
                                 sort="entry_time", order="asc")
    assert len(trades) == 1
    assert trades[0]["entry_price"] == 100.0


async def test_get_equity_curve(seeded):
    _, r, run_id, fold_id = seeded
    full = await r.get_equity_curve(run_id, fold_id=None)
    assert len(full) == 3
    assert full[0]["equity"] == 100000.0
    per = await r.get_equity_curve(run_id, fold_id=fold_id)
    assert len(per) == 3
