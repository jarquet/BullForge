from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from services.model_service.scheduler import RetrainScheduler


@pytest.mark.asyncio
async def test_run_once_invokes_meta_phase_per_symbol_direction():
    rc = AsyncMock()
    rc.get.side_effect = lambda key: {
        "retrain_schedule_hour": 1,
        "label_horizons_1m": [5],
        "label_horizons_1d": [1],
        "trading_symbols_equity": ["AAPL"],
        "trading_symbols_crypto": [],
    }.get(key)
    rc.get_all.return_value = {
        "retrain_cpu_workers": 1,
        "label_horizons_1m": [5], "label_horizons_1d": [1],
        "trading_symbols_equity": ["AAPL"], "trading_symbols_crypto": [],
        "meta_estimator_types": ["lightgbm"],
        "meta_pair_grid": [[2, 1], [3, 1.5], [4, 2]],
        "meta_train_window_days": 60,
        "meta_max_hold_minutes": 30,
        "meta_min_train_rows": 100,
    }
    redis = AsyncMock()
    inference = AsyncMock()
    db = AsyncMock()
    db.query_bars.return_value = []  # SPY pre-load returns nothing for the test

    sched = RetrainScheduler(
        db_dsn="postgres://x", artifacts_dir="/tmp/x", runtime_config=rc,
        inference_engine=inference, redis_client=redis,
        symbols=["AAPL"], timeframes=["1m"], horizons_by_tf={"1m": [5]},
        min_samples=10, db=db,
    )

    with patch.object(sched, "_train_meta_phase",
                       new=AsyncMock(return_value=[])) as mock_meta, \
         patch("services.model_service.scheduler.train_one_job"), \
         patch.object(sched, "_train_lstm_in_process",
                       new=AsyncMock(return_value={"status": "ok"})):
        await sched.run_once()

    mock_meta.assert_awaited_once()
    # Should pass the per-cycle config snapshot (positionally or as kwarg)
    call = mock_meta.await_args
    assert ("config" in call.kwargs) or (len(call.args) >= 1)
