from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.monitoring_service.accuracy_snapshot import persist_daily_accuracy


@pytest.mark.asyncio
async def test_persist_daily_accuracy_writes_one_snapshot_per_active_model():
    db = AsyncMock()
    db.query_all_active_models.return_value = [
        MagicMock(id=1), MagicMock(id=2), MagicMock(id=3),
    ]
    db.query_prediction_outcomes.return_value = [
        {"is_correct": True}, {"is_correct": False}, {"is_correct": True},
    ]

    count = await persist_daily_accuracy(db, window_days=30)

    assert count == 3
    assert db.insert_metric_snapshot.await_count == 3
    for call in db.insert_metric_snapshot.call_args_list:
        args, _ = call
        # Args: (category, scope, window, metrics)
        assert args[0] == "model_fidelity"
        assert args[2] == "all"
        assert "live_accuracy" in args[3]
        assert "sample_size" in args[3]


@pytest.mark.asyncio
async def test_persist_daily_accuracy_skips_model_with_no_outcomes():
    db = AsyncMock()
    db.query_all_active_models.return_value = [
        MagicMock(id=1), MagicMock(id=2),
    ]
    # Model 1 has outcomes, model 2 does not.
    async def outcomes(model_id, limit):
        if model_id == 1:
            return [{"is_correct": True}, {"is_correct": True}]
        return []
    db.query_prediction_outcomes.side_effect = outcomes

    count = await persist_daily_accuracy(db, window_days=30)

    assert count == 1  # Only model 1 snapshotted.
    assert db.insert_metric_snapshot.await_count == 1
    args = db.insert_metric_snapshot.call_args.args
    assert args[1] == "1"  # model_id as string scope
