import json
import pytest
import pytest_asyncio
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from shared.models import ModelMetadata, Prediction, FeatureRow
from services.model_service.inference import InferenceEngine


def _make_model_metadata(symbol="SPY", horizon=5):
    return ModelMetadata(
        id=1,
        symbol=symbol,
        timeframe="1m",
        horizon_minutes=horizon,
        model_type="gradient_boost",
        artifact_path="/path/model.joblib",
        metrics={"accuracy": 0.55},
        feature_columns=["sma_20", "rsi_14", "return_1"],
        trained_at=datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
        train_samples=800,
        test_samples=200,
    )


def _make_mock_model():
    model = MagicMock()
    model.predict.return_value = np.array([1])
    model.predict_proba.return_value = np.array([[0.15, 0.13, 0.72]])
    return model


def _make_feature_row():
    return FeatureRow(
        symbol="SPY",
        timestamp=datetime(2026, 4, 11, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        features={"sma_20": 420.5, "rsi_14": 55.3, "return_1": 0.001},
    )


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.query_all_active_models = AsyncMock(return_value=[
        _make_model_metadata("SPY", 5),
        _make_model_metadata("SPY", 10),
    ])
    db.query_feature_rows = AsyncMock(return_value=[_make_feature_row()])
    return db


@pytest.fixture
def mock_model_store():
    store = MagicMock()
    store.load.return_value = _make_mock_model()
    return store


@pytest.mark.asyncio
async def test_load_models(mock_db, mock_model_store):
    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    assert len(engine._models) == 2
    assert ("SPY", "1m", 5) in engine._models
    assert ("SPY", "1m", 10) in engine._models
    assert mock_model_store.load.call_count == 2


@pytest.mark.asyncio
async def test_load_models_empty(mock_model_store):
    mock_db = AsyncMock()
    mock_db.query_all_active_models = AsyncMock(return_value=[])

    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    assert len(engine._models) == 0


@pytest.mark.asyncio
async def test_predict(mock_db, mock_model_store):
    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    predictions = await engine.predict("SPY", "1m")

    assert len(predictions) == 2
    for pred in predictions:
        assert isinstance(pred, Prediction)
        assert pred.symbol == "SPY"
        assert pred.timeframe == "1m"
        assert pred.predicted_direction == 1
        assert pred.confidence == pytest.approx(0.72)

    horizons = {p.horizon_minutes for p in predictions}
    assert horizons == {5, 10}


@pytest.mark.asyncio
async def test_predict_no_features(mock_db, mock_model_store):
    mock_db.query_feature_rows = AsyncMock(return_value=[])

    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    predictions = await engine.predict("SPY", "1m")
    assert len(predictions) == 0


@pytest.mark.asyncio
async def test_predict_no_model_for_symbol(mock_db, mock_model_store):
    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    predictions = await engine.predict("AAPL", "1m")
    assert len(predictions) == 0


@pytest.mark.asyncio
async def test_predict_fills_missing_features(mock_db, mock_model_store):
    # Feature row is missing "return_1" — should be filled with 0.0
    partial_feature_row = FeatureRow(
        symbol="SPY",
        timestamp=datetime(2026, 4, 11, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        features={"sma_20": 420.5, "rsi_14": 55.3},
    )
    mock_db.query_feature_rows = AsyncMock(return_value=[partial_feature_row])

    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()

    predictions = await engine.predict("SPY", "1m")
    assert len(predictions) == 2  # Still produces predictions
