from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
import numpy as np
import pandas as pd
import pytest

from services.model_service.inference import InferenceEngine
from shared.models import ModelMetadata


def _md(mid, model_type, estimator_type=None, dir_label=None):
    suffix = f"{model_type}"
    if dir_label:
        suffix = f"{model_type}_{dir_label}_{estimator_type}"
    elif estimator_type:
        suffix = f"{model_type}_{estimator_type}"
    return ModelMetadata(
        id=mid, symbol="AAPL", timeframe="1m",
        horizon_minutes=0 if model_type.startswith("meta") else 5,
        model_type=model_type,
        artifact_path=f"/m/{suffix}_{mid}.joblib",
        metrics={}, feature_columns=["x"], trained_at=datetime.now(timezone.utc),
        train_samples=100, test_samples=20, is_active=True,
        estimator_type=estimator_type,
    )


@pytest.mark.asyncio
async def test_predict_meta_no_meta_models_returns_empty():
    db = AsyncMock(); db.query_all_active_models.return_value = []
    store = MagicMock()
    eng = InferenceEngine(db=db, model_store=store)
    await eng.load_meta_models()
    out = await eng.predict_meta(
        symbol="AAPL", timeframe="1m",
        base_predictions=[], regime={}, symbol_state={},
        timestamp=datetime.now(timezone.utc),
    )
    assert out == []


@pytest.mark.asyncio
async def test_predict_meta_returns_signal_per_loaded_estimator():
    db = AsyncMock()
    db.query_all_active_models.return_value = [
        _md(11, "meta_entry", "lightgbm", dir_label="long"),
        _md(12, "meta_exec", "lightgbm", dir_label="long"),
    ]
    db.query_prediction_outcomes.return_value = []

    fake_entry = MagicMock()
    fake_entry.predict_proba = MagicMock(return_value=np.array([[0.4, 0.6]]))
    fake_exec = MagicMock()
    fake_exec.predict = MagicMock(
        return_value=(np.array([0.07]), np.array([1])))

    def _load(path):
        return fake_entry if "11" in path else fake_exec
    store = MagicMock(); store.load.side_effect = _load

    eng = InferenceEngine(db=db, model_store=store)
    await eng.load_meta_models()

    out = await eng.predict_meta(
        symbol="AAPL", timeframe="1m",
        base_predictions=[{"model_id": 1, "direction": 1, "confidence": 0.7}],
        regime={"spy_trend_1m": 0.0, "spy_trend_1h": 0.0, "spy_trend_1d": 0.0,
                "spy_momentum": 0.0, "vol_regime": 1.0},
        symbol_state={"realized_vol": 0.02, "atr_price_ratio": 0.005,
                       "dist_sma_50": 0.0, "dist_sma_200bar": 0.0,
                       "dist_sma_200day": 0.0},
        timestamp=datetime.now(timezone.utc),
    )

    assert len(out) == 1
    sig = out[0]
    assert sig["estimator_type"] == "lightgbm"
    assert sig["direction"] == 1
    assert 0.0 <= sig["entry_probability"] <= 1.0
    assert 0.0 <= sig["kelly_fraction"] <= 1.0
    assert sig["pair_index"] == 1
    assert sig["target_pct"] > 0


@pytest.mark.asyncio
async def test_predict_meta_skips_when_atr_zero():
    db = AsyncMock()
    db.query_all_active_models.return_value = [
        _md(11, "meta_entry", "lightgbm", dir_label="long"),
        _md(12, "meta_exec", "lightgbm", dir_label="long"),
    ]
    db.query_prediction_outcomes.return_value = []

    store = MagicMock(); store.load.return_value = MagicMock()

    eng = InferenceEngine(db=db, model_store=store)
    await eng.load_meta_models()
    out = await eng.predict_meta(
        symbol="AAPL", timeframe="1m",
        base_predictions=[{"model_id": 1, "direction": 1, "confidence": 0.7}],
        regime={"spy_trend_1m": 0.0, "spy_trend_1h": 0.0, "spy_trend_1d": 0.0,
                "spy_momentum": 0.0, "vol_regime": 1.0},
        symbol_state={"realized_vol": 0.02, "atr_price_ratio": 0.0,  # zero ATR
                       "dist_sma_50": 0.0, "dist_sma_200bar": 0.0,
                       "dist_sma_200day": 0.0},
        timestamp=datetime.now(timezone.utc),
    )
    assert out == []
