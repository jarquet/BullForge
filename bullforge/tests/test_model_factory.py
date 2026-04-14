import pytest
import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline
from services.model_service.model_factory import create_model, SUPPORTED_MODELS


def _make_synthetic_data(n: int = 200):
    np.random.seed(42)
    X = pd.DataFrame({
        "feature_a": np.random.randn(n),
        "feature_b": np.random.randn(n),
        "feature_c": np.random.randn(n),
    })
    y = pd.Series(np.random.choice([-1, 0, 1], n))
    return X, y


def test_supported_models_set():
    assert "gradient_boost" in SUPPORTED_MODELS
    assert "logistic_regression" in SUPPORTED_MODELS


def test_create_gradient_boost():
    model = create_model("gradient_boost")
    assert isinstance(model, Pipeline)
    step_names = [name for name, _ in model.steps]
    assert "scaler" in step_names
    assert "classifier" in step_names


def test_create_logistic_regression():
    model = create_model("logistic_regression")
    assert isinstance(model, Pipeline)
    step_names = [name for name, _ in model.steps]
    assert "scaler" in step_names
    assert "classifier" in step_names


def test_create_model_rejects_unknown():
    with pytest.raises(ValueError, match="Unknown model type"):
        create_model("random_forest")


def test_gradient_boost_fit_predict():
    X, y = _make_synthetic_data()
    model = create_model("gradient_boost")
    model.fit(X, y)

    preds = model.predict(X)
    assert len(preds) == len(X)
    assert set(preds).issubset({-1, 0, 1})


def test_gradient_boost_predict_proba():
    X, y = _make_synthetic_data()
    model = create_model("gradient_boost")
    model.fit(X, y)

    proba = model.predict_proba(X)
    assert proba.shape == (len(X), 3)  # 3 classes
    # Probabilities should sum to ~1
    np.testing.assert_allclose(proba.sum(axis=1), 1.0, atol=1e-6)


def test_logistic_regression_fit_predict():
    X, y = _make_synthetic_data()
    model = create_model("logistic_regression")
    model.fit(X, y)

    preds = model.predict(X)
    assert len(preds) == len(X)
    assert set(preds).issubset({-1, 0, 1})


def test_logistic_regression_predict_proba():
    X, y = _make_synthetic_data()
    model = create_model("logistic_regression")
    model.fit(X, y)

    proba = model.predict_proba(X)
    assert proba.shape == (len(X), 3)
    np.testing.assert_allclose(proba.sum(axis=1), 1.0, atol=1e-6)


def test_supported_models_includes_lstm():
    assert "lstm" in SUPPORTED_MODELS


def test_create_lstm_default_config():
    from services.model_service.lstm_model import LSTMClassifier
    m = create_model("lstm")
    assert isinstance(m, LSTMClassifier)
    assert m.hidden_size == 64
    assert m.sequence_length == 30
    assert m.epochs == 50


def test_create_lstm_runtime_config_override():
    cfg = {
        "lstm_hidden_size": 128, "lstm_sequence_length": 20,
        "lstm_learning_rate": 0.005, "lstm_epochs": 10, "lstm_batch_size": 32,
    }
    m = create_model("lstm", runtime_config=cfg)
    assert m.hidden_size == 128
    assert m.sequence_length == 20
    assert m.lr == 0.005
    assert m.epochs == 10
    assert m.batch_size == 32
