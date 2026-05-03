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


async def test_update_fold_metrics_persists_rejected_count(seeded):
    """Round-trip: update_fold_metrics(rejected_orders_count=N) persists to
    backtest_folds.rejected_orders_count, and list_folds surfaces it on the
    top-level field (NOT duplicated inside the metrics sub-dict)."""
    d, r, run_id, fold_id = seeded
    await r.update_fold_metrics(
        fold_id=fold_id, metrics={"sharpe": 1.0},
        rejected_orders_count=42,
    )
    async with d.pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT rejected_orders_count FROM backtest_folds WHERE id = $1",
            fold_id,
        )
    assert n == 42
    folds = await r.list_folds(run_id)
    assert folds[0]["rejected_orders_count"] == 42
    # Single source of truth: the metrics sub-dict must NOT carry a copy.
    assert "rejected_orders_count" not in folds[0]["metrics"]


async def test_update_fold_metrics_without_kwarg_does_not_clobber_count(seeded):
    """Calling update_fold_metrics WITHOUT the rejected_orders_count kwarg
    must not overwrite an existing column value to zero. Locks the bug
    where the previous default-0 signature silently zeroed the column on
    every metrics-only refresh."""
    d, r, run_id, fold_id = seeded
    # First, set the column to a known non-zero value via the kwarg path.
    await r.update_fold_metrics(
        fold_id=fold_id, metrics={"sharpe": 1.0},
        rejected_orders_count=7,
    )
    # Now perform a metrics-only update without the kwarg.
    await r.update_fold_metrics(
        fold_id=fold_id, metrics={"sharpe": 2.0},
    )
    async with d.pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT rejected_orders_count FROM backtest_folds WHERE id = $1",
            fold_id,
        )
    assert n == 7


async def test_insert_fold_defaults_rejected_count_to_zero(seeded):
    """Newly-inserted folds (before update_fold_metrics has run) report 0,
    not NULL — protects the column's NOT NULL DEFAULT contract."""
    d, r, run_id, _ = seeded
    fold_id_2 = await r.insert_fold(
        run_id=run_id, fold_index=1,
        train_start=datetime(2025, 1, 3, tzinfo=UTC),
        train_end=datetime(2025, 1, 4, tzinfo=UTC),
        test_start=datetime(2025, 1, 4, tzinfo=UTC),
        test_end=datetime(2025, 1, 5, tzinfo=UTC),
    )
    async with d.pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT rejected_orders_count FROM backtest_folds WHERE id = $1",
            fold_id_2,
        )
    assert n == 0
