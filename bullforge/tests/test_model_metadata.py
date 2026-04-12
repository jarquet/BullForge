from datetime import datetime, timezone
import pytest
from shared.models import ModelMetadata, Prediction


def test_model_metadata_creation():
    meta = ModelMetadata(
        symbol="SPY",
        timeframe="1m",
        horizon_minutes=5,
        model_type="gradient_boost",
        artifact_path="/app/models/artifacts/gradient_boost_SPY_1m_5m.joblib",
        metrics={"accuracy": 0.55, "f1_macro": 0.48},
        feature_columns=["sma_20", "rsi_14", "return_1"],
        trained_at=datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
        train_samples=800,
        test_samples=200,
    )
    assert meta.symbol == "SPY"
    assert meta.model_type == "gradient_boost"
    assert meta.metrics["accuracy"] == 0.55
    assert len(meta.feature_columns) == 3
    assert meta.is_active is True
    assert meta.id is None


def test_model_metadata_with_id():
    meta = ModelMetadata(
        symbol="AAPL",
        timeframe="1d",
        horizon_minutes=30,
        model_type="logistic_regression",
        artifact_path="/app/models/artifacts/lr_AAPL.joblib",
        metrics={"accuracy": 0.52},
        feature_columns=["sma_20"],
        trained_at=datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
        train_samples=500,
        test_samples=100,
        is_active=False,
        id=42,
    )
    assert meta.id == 42
    assert meta.is_active is False


def test_model_metadata_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="UTC"):
        ModelMetadata(
            symbol="SPY",
            timeframe="1m",
            horizon_minutes=5,
            model_type="gradient_boost",
            artifact_path="/path",
            metrics={"accuracy": 0.5},
            feature_columns=["sma_20"],
            trained_at=datetime(2026, 4, 11, 14, 0, 0),  # naive — no tzinfo
            train_samples=100,
            test_samples=50,
        )


def test_model_metadata_rejects_bad_horizon():
    with pytest.raises(ValueError, match="positive"):
        ModelMetadata(
            symbol="SPY",
            timeframe="1m",
            horizon_minutes=0,
            model_type="gradient_boost",
            artifact_path="/path",
            metrics={"accuracy": 0.5},
            feature_columns=["sma_20"],
            trained_at=datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
            train_samples=100,
            test_samples=50,
        )


def test_model_metadata_rejects_empty_feature_columns():
    with pytest.raises(ValueError, match="feature_columns"):
        ModelMetadata(
            symbol="SPY",
            timeframe="1m",
            horizon_minutes=5,
            model_type="gradient_boost",
            artifact_path="/path",
            metrics={"accuracy": 0.5},
            feature_columns=[],
            trained_at=datetime(2026, 4, 11, 14, 0, 0, tzinfo=timezone.utc),
            train_samples=100,
            test_samples=50,
        )


def test_prediction_creation():
    pred = Prediction(
        symbol="SPY",
        timestamp=datetime(2026, 4, 11, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        horizon_minutes=5,
        model_id=1,
        predicted_direction=1,
        confidence=0.72,
    )
    assert pred.symbol == "SPY"
    assert pred.predicted_direction == 1
    assert pred.confidence == 0.72


def test_prediction_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="UTC"):
        Prediction(
            symbol="SPY",
            timestamp=datetime(2026, 4, 11, 15, 30, 0),
            timeframe="1m",
            horizon_minutes=5,
            model_id=1,
            predicted_direction=1,
            confidence=0.72,
        )


def test_prediction_rejects_bad_direction():
    with pytest.raises(ValueError, match="direction"):
        Prediction(
            symbol="SPY",
            timestamp=datetime(2026, 4, 11, 15, 30, 0, tzinfo=timezone.utc),
            timeframe="1m",
            horizon_minutes=5,
            model_id=1,
            predicted_direction=2,
            confidence=0.72,
        )


def test_prediction_rejects_bad_confidence():
    with pytest.raises(ValueError, match="confidence"):
        Prediction(
            symbol="SPY",
            timestamp=datetime(2026, 4, 11, 15, 30, 0, tzinfo=timezone.utc),
            timeframe="1m",
            horizon_minutes=5,
            model_id=1,
            predicted_direction=1,
            confidence=1.5,
        )
