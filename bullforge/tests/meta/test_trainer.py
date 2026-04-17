from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
import numpy as np
import pandas as pd
import pytest
from services.model_service.meta.trainer import MetaTrainer

def _ts(hour=14, minute=0):
    return datetime(2026, 4, 17, hour, minute, 0, tzinfo=timezone.utc)

def _mock_db():
    db = AsyncMock()
    feature_rows = []
    for i in range(200):
        feature_rows.append(MagicMock(
            symbol="AAPL", timestamp=_ts(13, i % 60), timeframe="1m",
            features={"sma_20": 150.0 + i * 0.1, "rsi_14": 50.0 + np.sin(i) * 10,
                       "atr_14": 2.0 + np.cos(i) * 0.5, "realized_vol": 0.02,
                       "dist_sma_50": 0.01, "dist_sma_200": -0.005, "dist_sma_200d": 0.008}))
    db.query_feature_rows.return_value = feature_rows
    db.query_prediction_outcomes.return_value = (
        [{"is_correct": True, "predicted_direction": 1, "confidence": 0.8}] * 15 +
        [{"is_correct": False, "predicted_direction": 1, "confidence": 0.6}] * 5)
    labels = [MagicMock(symbol="AAPL", timestamp=_ts(13, i % 60),
                         direction=np.random.choice([-1, 1]), forward_return=np.random.uniform(-0.01, 0.01))
              for i in range(200)]
    db.query_labels.return_value = labels
    db.count_meta_features.return_value = 0
    db.insert_meta_features_batch.return_value = None
    db.insert_model_metadata.return_value = 100
    db.query_all_active_models.return_value = [
        MagicMock(id=mid, symbol="AAPL", timeframe="1m", horizon_minutes=5,
                  model_type="gradient_boost", metrics={"accuracy": 0.65},
                  feature_columns=["sma_20", "rsi_14"])
        for mid in [1, 2, 3]]
    db.deactivate_models.return_value = None
    return db

def _mock_model_store():
    store = MagicMock()
    store.save.return_value = "/models/meta_entry_AAPL_long_lightgbm.joblib"
    return store

@pytest.mark.asyncio
async def test_train_entry_produces_metadata():
    db, store = _mock_db(), _mock_model_store()
    trainer = MetaTrainer(db=db, model_store=store,
                          sub_model_pairs={"long": [(0.001, 0.001)] * 7})
    result = await trainer.train_entry(symbol="AAPL", direction=1, estimator_type="lightgbm", as_of=_ts())
    assert result is not None
    assert result.model_type == "meta_entry"
    assert db.insert_model_metadata.called

@pytest.mark.asyncio
async def test_train_entry_insufficient_data_returns_none():
    db, store = _mock_db(), _mock_model_store()
    db.query_feature_rows.return_value = []
    trainer = MetaTrainer(db=db, model_store=store, sub_model_pairs={"long": [(0.005, 0.003)]})
    result = await trainer.train_entry(symbol="AAPL", direction=1, estimator_type="lightgbm", as_of=_ts())
    assert result is None

@pytest.mark.asyncio
async def test_train_exec_produces_metadata():
    db, store = _mock_db(), _mock_model_store()
    store.save.return_value = "/models/meta_exec_AAPL_long_lightgbm.joblib"
    trainer = MetaTrainer(db=db, model_store=store,
                          sub_model_pairs={"long": [(0.001, 0.001)] * 7})
    result = await trainer.train_exec(symbol="AAPL", direction=1, estimator_type="lightgbm", as_of=_ts())
    assert result is not None
    assert result.model_type == "meta_exec"

def test_model_store_saves_and_loads_calibrated_entry(tmp_path):
    from services.model_service.meta.calibrator import CalibratedEntryEstimator
    from services.model_service.meta.estimators.lightgbm import LightGBMEntryEstimator
    from services.model_service.model_store import ModelStore
    X, y = pd.DataFrame({"f1": np.random.rand(100)}), pd.Series(np.random.choice([0, 1], 100))
    base = LightGBMEntryEstimator()
    cal = CalibratedEntryEstimator(base)
    cal.fit(X, y, X_cal=X[:20], y_cal=y[:20])
    store = ModelStore(str(tmp_path))
    path = store.save(cal, "AAPL", "1m", 0, "meta_entry_lightgbm")
    loaded = store.load(path)
    proba = loaded.predict_proba(X[:5])
    assert proba.shape == (5, 2)
