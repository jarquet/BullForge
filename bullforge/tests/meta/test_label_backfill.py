from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock
import pytest

from shared.models import MetaFeatureRow
from services.model_service.meta.label_backfill import MetaLabelBackfiller


def _ts(min_offset=0):
    return datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc) + timedelta(minutes=min_offset)


@pytest.mark.asyncio
async def test_one_pass_labels_eligible_rows():
    # Entry is at bar 20 (bars_recs[20]) so ATR-14 is defined.
    entry_ts = _ts(20)
    unlabeled = MetaFeatureRow(
        symbol="AAPL", timestamp=entry_ts, direction=1,
        feature_vector={"x": 1.0}, source="live",
    )
    db = AsyncMock()
    db.query_unlabeled_meta_features.return_value = [unlabeled]
    bars_recs = [
        {"symbol": "AAPL", "timestamp": _ts(i), "open": 100.0, "high": 100.5,
         "low": 99.5, "close": 100.0 + 0.05 * i, "volume": 1000.0,
         "timeframe": "1m"} for i in range(60)
    ]
    db.query_bars_between.return_value = bars_recs

    bf = MetaLabelBackfiller(
        db=db, max_hold_minutes=30, pair_grid=[[2, 1], [3, 1.5]],
        safety_buffer_minutes=5,
    )
    n = await bf.run_once(now=_ts(60))
    assert n == 1
    db.update_meta_feature_labels.assert_called_once()
    args = db.update_meta_feature_labels.call_args.kwargs
    assert args["symbol"] == "AAPL"
    assert args["direction"] == 1
    assert args["entry_label"] in (0, 1)
    assert args["optimal_pair_idx"] in (0, 1)


@pytest.mark.asyncio
async def test_no_op_when_no_unlabeled_rows():
    db = AsyncMock()
    db.query_unlabeled_meta_features.return_value = []
    bf = MetaLabelBackfiller(
        db=db, max_hold_minutes=30, pair_grid=[[2, 1]], safety_buffer_minutes=5,
    )
    n = await bf.run_once(now=_ts(60))
    assert n == 0
    db.update_meta_feature_labels.assert_not_called()


@pytest.mark.asyncio
async def test_skips_when_too_few_post_entry_bars():
    unlabeled = MetaFeatureRow(
        symbol="AAPL", timestamp=_ts(0), direction=1,
        feature_vector={"x": 1.0}, source="live",
    )
    db = AsyncMock()
    db.query_unlabeled_meta_features.return_value = [unlabeled]
    db.query_bars_between.return_value = [{"symbol": "AAPL", "timestamp": _ts(0),
                                    "open": 100.0, "high": 100.0, "low": 100.0,
                                    "close": 100.0, "volume": 0.0,
                                    "timeframe": "1m"}]
    bf = MetaLabelBackfiller(
        db=db, max_hold_minutes=30, pair_grid=[[2, 1]], safety_buffer_minutes=5,
    )
    n = await bf.run_once(now=_ts(60))
    assert n == 0
    db.update_meta_feature_labels.assert_not_called()
