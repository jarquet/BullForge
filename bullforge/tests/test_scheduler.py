import asyncio
from datetime import datetime, timezone
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from services.model_service.scheduler import RetrainScheduler, compute_seconds_until


def test_compute_seconds_until_same_day():
    now = datetime(2026, 4, 13, 10, 0, 0, tzinfo=timezone.utc)
    secs = compute_seconds_until(hour=14, now=now)
    assert secs == 4 * 3600


def test_compute_seconds_until_next_day():
    now = datetime(2026, 4, 13, 15, 0, 0, tzinfo=timezone.utc)
    secs = compute_seconds_until(hour=14, now=now)
    assert secs == 23 * 3600


@pytest.mark.asyncio
async def test_run_once_dispatches_cpu_then_gpu_phase():
    runtime_config = AsyncMock()
    runtime_config.get = AsyncMock(side_effect=lambda k: {
        "retrain_cpu_workers": 2,
        "lstm_epochs": 1, "lstm_hidden_size": 8, "lstm_sequence_length": 5,
        "lstm_learning_rate": 0.01, "lstm_batch_size": 16,
    }.get(k))
    runtime_config.get_all = AsyncMock(return_value={
        "retrain_cpu_workers": 2,
        "lstm_epochs": 1, "lstm_hidden_size": 8, "lstm_sequence_length": 5,
        "lstm_learning_rate": 0.01, "lstm_batch_size": 16,
    })
    runtime_config.set = AsyncMock()

    inference_engine = AsyncMock()
    redis_client = AsyncMock()

    sched = RetrainScheduler(
        db_dsn="postgres://fake",
        artifacts_dir="/tmp/artifacts",
        runtime_config=runtime_config,
        inference_engine=inference_engine,
        redis_client=redis_client,
        symbols=["SPY"],
        timeframes=["1m"],
        horizons_by_tf={"1m": [5]},
        min_samples=10,
    )

    with patch("services.model_service.scheduler.ProcessPoolExecutor") as pool_cls:
        pool_instance = MagicMock()
        pool_cls.return_value.__enter__.return_value = pool_instance
        future = MagicMock()
        future.result.return_value = {
            "symbol": "SPY", "timeframe": "1m", "horizon": 5,
            "model_type": "gradient_boost", "status": "ok", "model_id": 42,
        }
        pool_instance.submit.return_value = future

        lstm_mock = AsyncMock(
            return_value={"symbol": "SPY", "timeframe": "1m", "horizon": 5,
                          "model_type": "lstm", "status": "ok", "model_id": 43}
        )
        with patch.object(sched, "_train_lstm_in_process", lstm_mock):
            summary = await sched.run_once()

    assert pool_instance.submit.call_count == 1
    assert lstm_mock.await_count == 1
    inference_engine.load_models.assert_awaited_once()
    # Meta models must also be reloaded after the meta phase, otherwise
    # freshly-trained meta artifacts sit on disk + in model_registry but
    # the running InferenceEngine's _meta_models dict remains stale until
    # restart, and predict_meta returns [] for every event.
    inference_engine.load_meta_models.assert_awaited_once()
    redis_client.publish.assert_awaited()
    # last_retrain_time stored as ISO string
    set_calls = runtime_config.set.call_args_list
    assert any(c[0][0] == "last_retrain_time" for c in set_calls)


@pytest.mark.asyncio
async def test_run_once_handles_worker_error():
    runtime_config = AsyncMock()
    runtime_config.get = AsyncMock(return_value=1)
    runtime_config.get_all = AsyncMock(return_value={
        "retrain_cpu_workers": 1, "lstm_epochs": 1, "lstm_hidden_size": 8,
        "lstm_sequence_length": 5, "lstm_learning_rate": 0.01, "lstm_batch_size": 16,
    })
    runtime_config.set = AsyncMock()
    inference_engine = AsyncMock()
    redis_client = AsyncMock()

    sched = RetrainScheduler(
        db_dsn="postgres://fake", artifacts_dir="/tmp", runtime_config=runtime_config,
        inference_engine=inference_engine, redis_client=redis_client,
        symbols=["SPY"], timeframes=["1m"], horizons_by_tf={"1m": [5]}, min_samples=10,
    )

    with patch("services.model_service.scheduler.ProcessPoolExecutor") as pool_cls:
        pool_instance = MagicMock()
        pool_cls.return_value.__enter__.return_value = pool_instance
        future = MagicMock()
        future.result.return_value = {
            "symbol": "SPY", "timeframe": "1m", "horizon": 5,
            "model_type": "gradient_boost", "status": "error", "error": "boom",
        }
        pool_instance.submit.return_value = future

        with patch.object(sched, "_train_lstm_in_process", new=AsyncMock(
            return_value={"status": "ok", "symbol": "SPY", "timeframe": "1m",
                          "horizon": 5, "model_type": "lstm"}
        )):
            summary = await sched.run_once()

    assert any(r["status"] == "error" for r in summary["results"])
    inference_engine.load_models.assert_awaited_once()
    inference_engine.load_meta_models.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_once_uses_spawn_context_for_process_pool():
    """Regression for 2026-05-04 incident: forking from model_service
    (which has imported torch / lightgbm / sklearn / UCX in the parent)
    is the textbook OpenMP-segfault setup. Spawn workers start from a
    clean interpreter. Pin the constructor argument so a future refactor
    can't quietly drop it."""
    import multiprocessing

    runtime_config = AsyncMock()
    runtime_config.get = AsyncMock(return_value=1)
    runtime_config.get_all = AsyncMock(return_value={
        "retrain_cpu_workers": 1, "lstm_epochs": 1, "lstm_hidden_size": 8,
        "lstm_sequence_length": 5, "lstm_learning_rate": 0.01,
        "lstm_batch_size": 16,
    })
    runtime_config.set = AsyncMock()
    inference_engine = AsyncMock()
    redis_client = AsyncMock()

    sched = RetrainScheduler(
        db_dsn="postgres://fake", artifacts_dir="/tmp",
        runtime_config=runtime_config,
        inference_engine=inference_engine, redis_client=redis_client,
        symbols=["SPY"], timeframes=["1m"], horizons_by_tf={"1m": [5]},
        min_samples=10,
    )

    with patch("services.model_service.scheduler.ProcessPoolExecutor") as pool_cls:
        pool_instance = MagicMock()
        pool_cls.return_value.__enter__.return_value = pool_instance
        future = MagicMock()
        future.result.return_value = {
            "symbol": "SPY", "timeframe": "1m", "horizon": 5,
            "model_type": "gradient_boost", "status": "ok", "model_id": 1,
        }
        pool_instance.submit.return_value = future

        with patch.object(sched, "_train_lstm_in_process", new=AsyncMock(
            return_value={"status": "ok", "symbol": "SPY", "timeframe": "1m",
                          "horizon": 5, "model_type": "lstm"},
        )):
            await sched.run_once()

    assert pool_cls.call_count == 1, (
        "ProcessPoolExecutor must be invoked exactly once per cycle"
    )
    kwargs = pool_cls.call_args.kwargs
    assert "mp_context" in kwargs, (
        "ProcessPoolExecutor must be constructed with mp_context= "
        "to avoid the default fork start method on Linux. See "
        "docs/superpowers/specs/2026-05-04-model-service-retrain-resilience-design.md"
    )
    assert kwargs["mp_context"].get_start_method() == "spawn", (
        f"Expected spawn start method, got "
        f"{kwargs['mp_context'].get_start_method()!r}"
    )
