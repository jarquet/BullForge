"""Tests that BacktestEngine honors the check_cancelled callback at the
two poll points: start of each fold, and every 100 bars in the replay loop."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.backtest_service.engine import BacktestConfig, BacktestEngine
from services.backtest_service.exceptions import BacktestCancelled
from services.backtest_service.splitter import Fold


def _cfg() -> BacktestConfig:
    return BacktestConfig(
        name="cancel-test",
        symbols=["AAA"],
        timeframe="1m",
        horizons=[5],
        range_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        range_end=datetime(2026, 1, 10, tzinfo=timezone.utc),
        train_days=3, test_days=1, embargo_minutes=0, mode="rolling",
    )


def _fold() -> Fold:
    return Fold(
        index=0,
        train_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        train_end=datetime(2026, 1, 4, tzinfo=timezone.utc),
        test_start=datetime(2026, 1, 4, tzinfo=timezone.utc),
        test_end=datetime(2026, 1, 5, tzinfo=timezone.utc),
    )


def _make_engine() -> BacktestEngine:
    results = MagicMock()
    results.insert_run = AsyncMock(return_value=1)
    results.insert_fold = AsyncMock(return_value=1)
    results.finish_run = AsyncMock()
    return BacktestEngine(
        db=MagicMock(),
        runtime_config=MagicMock(),
        model_store=MagicMock(),
        results_db=results,
    )


@pytest.mark.asyncio
async def test_run_fold_raises_on_fold_start_cancellation():
    engine = _make_engine()
    check = AsyncMock(return_value=True)
    with pytest.raises(BacktestCancelled):
        await engine._run_fold(
            run_id=1, cfg=_cfg(), fold=_fold(), check_cancelled=check,
        )
    check.assert_awaited()


@pytest.mark.asyncio
async def test_run_marks_run_cancelled_on_cancellation():
    engine = _make_engine()
    check = AsyncMock(return_value=True)
    # Patch _run_fold to raise immediately on first call.
    engine._run_fold = AsyncMock(side_effect=BacktestCancelled())
    with pytest.raises(BacktestCancelled):
        await engine.run(_cfg(), check_cancelled=check)
    engine._results.finish_run.assert_awaited_once_with(
        run_id=1, status="cancelled",
    )


@pytest.mark.asyncio
async def test_run_marks_run_failed_on_generic_exception():
    """Regression: the cancel handling doesn't swallow other exceptions."""
    engine = _make_engine()
    engine._run_fold = AsyncMock(side_effect=RuntimeError("boom"))
    with pytest.raises(RuntimeError):
        await engine.run(_cfg())
    engine._results.finish_run.assert_awaited_once_with(
        run_id=1, status="failed",
    )


@pytest.mark.asyncio
async def test_run_no_checker_does_not_call_it():
    """When check_cancelled is None, the engine must not crash on the poll."""
    engine = _make_engine()
    engine._run_fold = AsyncMock(return_value=None)
    run_id = await engine.run(_cfg())
    assert run_id == 1
    engine._results.finish_run.assert_awaited_once_with(
        run_id=1, status="completed",
    )
