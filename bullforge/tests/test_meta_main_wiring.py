from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
import json
import pytest

from services.model_service.main import on_feature_update_factory


def _ts():
    return datetime(2026, 4, 30, 14, 0, tzinfo=timezone.utc)


def _make_inference(predict_meta_signals):
    inference = AsyncMock()
    inference.predict.return_value = [
        MagicMock(model_id=1, predicted_direction=1, confidence=0.7,
                  symbol="AAPL", timestamp=_ts(),
                  horizon_minutes=5)
    ]
    inference.predict_meta.return_value = predict_meta_signals
    return inference


def _make_db():
    db = AsyncMock()
    db.query_feature_rows.return_value = [MagicMock(
        timestamp=_ts(),
        features={
            "atr_14": 0.5, "sma_20": 100.0, "realized_vol": 0.02,
            "dist_sma_50": 0.0, "dist_sma_200": 0.0, "dist_sma_200d": 0.0,
        },
    )]
    return db


def _make_rb():
    rb = MagicMock()
    rb.regime_at = MagicMock(return_value={
        "spy_trend_1m": 0.0, "spy_trend_1h": 0.0, "spy_trend_1d": 0.0,
        "spy_momentum": 0.0, "vol_regime": 1.0,
    })
    return rb


@pytest.mark.asyncio
async def test_handler_publishes_meta_signals_when_enabled():
    rc = AsyncMock()
    rc.get.side_effect = lambda k: {
        "meta_inference_enabled": True,
        "meta_pair_grid": [[2, 1], [3, 1.5], [4, 2]],
        "meta_rolling_stats_ttl_seconds": 600,
    }.get(k)

    redis = AsyncMock()
    inference = _make_inference([{
        "direction": 1, "estimator_type": "lightgbm",
        "entry_probability": 0.6, "kelly_fraction": 0.05,
        "pair_index": 1, "target_pct": 0.01, "stop_pct": 0.005,
        "model_id": 11,
    }])
    db = _make_db()

    handler = on_feature_update_factory(
        inference=inference, redis=redis, runtime_config=rc, db=db,
        regime_builder=_make_rb(),
    )
    await handler(json.dumps({"symbol": "AAPL", "timeframe": "1m"}))

    channels = [c.args[0] for c in redis.publish.call_args_list]
    assert "model:predictions" in channels
    assert "model:meta_signals" in channels


@pytest.mark.asyncio
async def test_handler_skips_meta_publish_when_disabled():
    rc = AsyncMock()
    rc.get.side_effect = lambda k: {"meta_inference_enabled": False}.get(k)
    redis = AsyncMock()
    inference = _make_inference([])
    db = _make_db()
    handler = on_feature_update_factory(
        inference=inference, redis=redis, runtime_config=rc, db=db,
        regime_builder=_make_rb(),
    )
    await handler(json.dumps({"symbol": "AAPL", "timeframe": "1m"}))
    channels = [c.args[0] for c in redis.publish.call_args_list]
    assert "model:meta_signals" not in channels


@pytest.mark.asyncio
async def test_handler_skips_meta_publish_when_no_signals_returned():
    rc = AsyncMock()
    rc.get.side_effect = lambda k: {
        "meta_inference_enabled": True,
        "meta_pair_grid": [[2, 1]],
        "meta_rolling_stats_ttl_seconds": 600,
    }.get(k)
    redis = AsyncMock()
    inference = _make_inference([])  # predict_meta returns no signals
    db = _make_db()
    handler = on_feature_update_factory(
        inference=inference, redis=redis, runtime_config=rc, db=db,
        regime_builder=_make_rb(),
    )
    await handler(json.dumps({"symbol": "AAPL", "timeframe": "1m"}))
    channels = [c.args[0] for c in redis.publish.call_args_list]
    assert "model:meta_signals" not in channels
