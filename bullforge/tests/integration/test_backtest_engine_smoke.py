# bullforge/tests/integration/test_backtest_engine_smoke.py
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from shared.config import Settings
from shared.db import Database
from shared.runtime_config import RuntimeConfig
from shared.redis import RedisClient

from services.backtest_service.engine import BacktestConfig, BacktestEngine
from services.backtest_service.results_db import BacktestResultsDB
from services.model_service.model_store import ModelStore


pytestmark = pytest.mark.asyncio(loop_scope="session")
UTC = timezone.utc


@pytest_asyncio.fixture(loop_scope="session")
async def stack():
    settings = Settings()
    live_db = Database(settings)
    await live_db.connect()
    try:
        sandbox_db = Database(settings, sandbox=True)
        await sandbox_db.connect()
        try:
            redis = RedisClient(settings)
            await redis.connect()
            try:
                yield live_db, sandbox_db, redis, settings
            finally:
                await redis.disconnect()
        finally:
            await sandbox_db.disconnect()
    finally:
        await live_db.disconnect()


async def test_single_fold_smoke(stack):
    """Requires bars + features + labels for SPY 1m loaded for the test window.

    To prepare data once before this test runs, execute:
        python -m services.data_pipeline.backfill --symbol SPY --days 90
        python -m services.feature_engine.main --batch --symbol SPY
    """
    live_db, sandbox_db, redis, settings = stack

    async with live_db.pool.acquire() as conn:
        bar_count = await conn.fetchval(
            "SELECT COUNT(*) FROM bars_1m WHERE symbol='SPY' AND timestamp >= NOW() - INTERVAL '25 days'"
        )
    if bar_count < 5000:
        pytest.skip(f"SPY 1m data not loaded for window (have {bar_count} bars, need >=5000)")

    rc = RuntimeConfig(redis)
    model_store = ModelStore(settings.model_artifacts_dir)
    results = BacktestResultsDB(live_db)
    engine = BacktestEngine(
        db=sandbox_db, runtime_config=rc, model_store=model_store, results_db=results,
    )

    end = datetime.now(UTC).replace(microsecond=0, second=0)
    cfg = BacktestConfig(
        name="smoke-single-fold",
        symbols=["SPY"], timeframe="1m", horizons=[5, 10],
        range_start=end - timedelta(days=20),
        range_end=end - timedelta(days=1),
        train_days=10, test_days=2, embargo_minutes=30, mode="rolling",
        starting_cash=100_000.0, slippage_bps=5.0, max_volume_pct=0.10,
    )
    run_id = await engine.run(cfg)
    assert run_id > 0

    try:
        # Verify run completed and at least one fold + equity points exist.
        async with live_db.pool.acquire() as conn:
            run = await conn.fetchrow(
                "SELECT status FROM backtest_runs WHERE id=$1", run_id,
            )
            folds = await conn.fetch(
                "SELECT id FROM backtest_folds WHERE run_id=$1", run_id,
            )
            eq_count = await conn.fetchval(
                "SELECT COUNT(*) FROM backtest_equity WHERE run_id=$1", run_id,
            )
        assert run["status"] == "completed"
        assert len(folds) >= 1
        assert eq_count > 0
    finally:
        async with live_db.pool.acquire() as conn:
            await conn.execute("DELETE FROM backtest_equity WHERE run_id=$1", run_id)
            await conn.execute("DELETE FROM backtest_trades WHERE run_id=$1", run_id)
            await conn.execute("DELETE FROM backtest_folds WHERE run_id=$1", run_id)
            await conn.execute("DELETE FROM backtest_runs WHERE id=$1", run_id)
