from datetime import datetime, timedelta, timezone
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


@pytest.mark.asyncio
async def test_train_meta_phase_preloads_spy_with_35_day_buffer():
    """RegimeBuilder.vol_regime needs a 30-day rolling median, so the SPY
    pre-load must extend at least train_window + 30 days (we use +35) before
    `as_of`. Phase 1.5 cleanup of the older +7 buffer."""
    rc = AsyncMock()
    redis = AsyncMock()
    inference = AsyncMock()
    db = AsyncMock()
    db.query_bars.return_value = []
    db.count_predictions_in_window.return_value = 1  # bypass pre-flight skip

    sched = RetrainScheduler(
        db_dsn="postgres://x", artifacts_dir="/tmp/x", runtime_config=rc,
        inference_engine=inference, redis_client=redis,
        symbols=["AAPL"], timeframes=["1m"], horizons_by_tf={"1m": [5]},
        min_samples=10, db=db,
    )

    as_of = datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc)
    train_window = 60
    config = {
        "meta_estimator_types": ["lightgbm"],
        "meta_pair_grid": [[2, 1]],
        "meta_train_window_days": train_window,
        "meta_max_hold_minutes": 30,
        "meta_min_train_rows": 100,
        "meta_regime_max_staleness_hours": 48,
    }

    with patch("services.model_service.scheduler.MetaTrainer") as MockTrainer:
        instance = MagicMock()
        instance.train_entry = AsyncMock(return_value=None)
        instance.train_exec = AsyncMock(return_value=None)
        MockTrainer.return_value = instance
        await sched._train_meta_phase(config=config, as_of=as_of)

    db.query_bars.assert_awaited_once()
    kwargs = db.query_bars.await_args.kwargs
    args = db.query_bars.await_args.args
    since = kwargs.get("since") or (args[2] if len(args) >= 3 else None)
    assert since is not None
    since_dt = datetime.fromisoformat(since)
    delta = as_of - since_dt
    assert delta >= timedelta(days=train_window + 30), (
        f"SPY pre-load buffer too small: {delta} < {train_window + 30} days"
    )


@pytest.mark.asyncio
async def test_train_meta_phase_skips_when_predictions_table_empty():
    """When count_predictions_in_window returns 0, the meta phase must
    short-circuit before pre-loading SPY or constructing a MetaTrainer.
    Saves N×M trainer calls that would all silently hit Insufficient data,
    and surfaces the cause to operators at WARNING level."""
    rc = AsyncMock()
    redis = AsyncMock()
    inference = AsyncMock()
    db = AsyncMock()
    db.count_predictions_in_window.return_value = 0  # trigger skip

    sched = RetrainScheduler(
        db_dsn="postgres://x", artifacts_dir="/tmp/x", runtime_config=rc,
        inference_engine=inference, redis_client=redis,
        symbols=["AAPL", "MSFT", "GOOGL"], timeframes=["1m"],
        horizons_by_tf={"1m": [5]},
        min_samples=10, db=db,
    )

    as_of = datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc)
    config = {
        "meta_estimator_types": ["lightgbm", "mlp"],
        "meta_pair_grid": [[2, 1]],
        "meta_train_window_days": 60,
        "meta_max_hold_minutes": 30,
        "meta_min_train_rows": 100,
    }

    with patch("services.model_service.scheduler.MetaTrainer") as MockTrainer:
        out = await sched._train_meta_phase(config=config, as_of=as_of)

    assert out == []
    db.count_predictions_in_window.assert_awaited_once()
    db.query_bars.assert_not_awaited()  # skipped before SPY pre-load
    MockTrainer.assert_not_called()  # skipped before trainer construction


@pytest.mark.asyncio
async def test_train_meta_phase_reports_skipped_when_trainer_returns_none():
    """When both train_entry and train_exec return None (insufficient data),
    the scheduler must record status='skipped_insufficient_data', not 'ok'.
    Otherwise the run_once summary line counts every empty meta job as a
    success and the operator can't distinguish real ok vs. silent skip."""
    rc = AsyncMock()
    redis = AsyncMock()
    inference = AsyncMock()
    db = AsyncMock()
    db.query_bars.return_value = []
    db.count_predictions_in_window.return_value = 100  # bypass pre-flight

    sched = RetrainScheduler(
        db_dsn="postgres://x", artifacts_dir="/tmp/x", runtime_config=rc,
        inference_engine=inference, redis_client=redis,
        symbols=["AAPL"], timeframes=["1m"], horizons_by_tf={"1m": [5]},
        min_samples=10, db=db,
    )

    as_of = datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc)
    config = {
        "meta_estimator_types": ["lightgbm"],
        "meta_pair_grid": [[2, 1]],
        "meta_train_window_days": 60,
        "meta_max_hold_minutes": 30,
        "meta_min_train_rows": 100,
    }

    with patch("services.model_service.scheduler.MetaTrainer") as MockTrainer:
        instance = MagicMock()
        instance.train_entry = AsyncMock(return_value=None)
        instance.train_exec = AsyncMock(return_value=None)
        MockTrainer.return_value = instance
        out = await sched._train_meta_phase(config=config, as_of=as_of)

    # AAPL × {long, short} × {lightgbm} = 2 entries, all skipped
    assert len(out) == 2
    for row in out:
        assert row["status"] == "skipped_insufficient_data", row
        assert row["entry_id"] is None
        assert row["exec_id"] is None
