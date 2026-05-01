from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
import json
import pytest

from services.execution_service.meta_shadow_writer import MetaShadowWriter


@pytest.mark.asyncio
async def test_writes_one_shadow_row_per_signal():
    db = AsyncMock()
    writer = MetaShadowWriter(db=db)
    msg = json.dumps({
        "symbol": "AAPL", "timeframe": "1m",
        "timestamp": "2026-04-30T14:00:00+00:00",
        "feature_vector": {"x": 1.0},
        "signals": [
            {"direction": 1, "estimator_type": "lightgbm",
             "entry_probability": 0.6, "kelly_fraction": 0.05,
             "pair_index": 1, "target_pct": 0.01, "stop_pct": 0.005,
             "model_id": 11},
            {"direction": -1, "estimator_type": "lightgbm",
             "entry_probability": 0.3, "kelly_fraction": 0.0,
             "pair_index": 0, "target_pct": 0.005, "stop_pct": 0.005,
             "model_id": 12},
        ],
    })
    await writer.handle_message(msg)
    assert db.insert_meta_signal_shadow.await_count == 2
    # One meta_features row per unique direction (long + short = 2)
    assert db.insert_meta_feature.await_count == 2


@pytest.mark.asyncio
async def test_handles_empty_signals_gracefully():
    db = AsyncMock()
    writer = MetaShadowWriter(db=db)
    await writer.handle_message(json.dumps({
        "symbol": "AAPL", "timeframe": "1m",
        "timestamp": "2026-04-30T14:00:00+00:00",
        "feature_vector": {}, "signals": [],
    }))
    db.insert_meta_signal_shadow.assert_not_called()
    db.insert_meta_feature.assert_not_called()


@pytest.mark.asyncio
async def test_malformed_message_logs_and_continues():
    db = AsyncMock()
    writer = MetaShadowWriter(db=db)
    await writer.handle_message("not json")
    db.insert_meta_signal_shadow.assert_not_called()


@pytest.mark.asyncio
async def test_dedups_meta_features_within_payload():
    """Two signals with the same direction -> only one meta_features row."""
    db = AsyncMock()
    writer = MetaShadowWriter(db=db)
    msg = json.dumps({
        "symbol": "AAPL", "timeframe": "1m",
        "timestamp": "2026-04-30T14:00:00+00:00",
        "feature_vector": {"x": 1.0},
        "signals": [
            {"direction": 1, "estimator_type": "lightgbm",
             "entry_probability": 0.6, "kelly_fraction": 0.05,
             "pair_index": 0, "target_pct": 0.01, "stop_pct": 0.005,
             "model_id": 11},
            {"direction": 1, "estimator_type": "mlp",
             "entry_probability": 0.7, "kelly_fraction": 0.07,
             "pair_index": 1, "target_pct": 0.012, "stop_pct": 0.006,
             "model_id": 21},
        ],
    })
    await writer.handle_message(msg)
    # 2 shadow rows (one per estimator), but only 1 meta_features row
    # (both signals are direction=1)
    assert db.insert_meta_signal_shadow.await_count == 2
    assert db.insert_meta_feature.await_count == 1
