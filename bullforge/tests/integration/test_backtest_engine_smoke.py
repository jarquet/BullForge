# bullforge/tests/integration/test_backtest_engine_smoke.py
from datetime import datetime, timedelta, timezone

import pytest

from shared.config import Settings
from shared.db import Database
from shared.runtime_config import RuntimeConfig
from shared.redis import RedisClient

from services.backtest_service.engine import BacktestConfig, BacktestEngine
from services.backtest_service.results_db import BacktestResultsDB
from services.model_service.model_store import ModelStore


pytestmark = pytest.mark.asyncio
UTC = timezone.utc


@pytest.fixture
async def stack():
    settings = Settings()
    live_db = Database(settings)
    await live_db.connect()
    sandbox_db = Database(settings, sandbox=True)
    await sandbox_db.connect()
    redis = RedisClient(settings)
    await redis.connect()
    yield live_db, sandbox_db, redis, settings
    await redis.disconnect()
    await sandbox_db.disconnect()
    await live_db.disconnect()


async def test_single_fold_smoke(stack):
    """Requires bars + features + labels for SPY 1m loaded for the test window.

    To prepare data once before this test runs, execute:
        python -m services.data_pipeline.backfill --symbol SPY --days 90
        python -m services.feature_engine.main --batch --symbol SPY
    """
    live_db, sandbox_db, redis, settings = stack
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
