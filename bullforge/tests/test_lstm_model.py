import numpy as np
import pandas as pd
import pytest

from services.model_service.lstm_model import LSTMClassifier


@pytest.fixture
def synthetic_data():
    rng = np.random.default_rng(42)
    n = 500
    X = pd.DataFrame(rng.normal(size=(n, 4)), columns=["a", "b", "c", "d"])
    roll = X["a"].rolling(3).sum().fillna(0)
    y = pd.Series(np.where(roll > 0.3, 1, np.where(roll < -0.3, -1, 0)))
    return X, y


def test_lstm_fit_predict_shapes(synthetic_data):
    X, y = synthetic_data
    m = LSTMClassifier(
        hidden_size=16, sequence_length=10, epochs=3, batch_size=32, lr=0.01,
    )
    m.fit(X, y)
    preds = m.predict(X)
    assert len(preds) == len(X)
    assert set(np.unique(preds)).issubset({-1, 0, 1})


def test_lstm_predict_proba_shape(synthetic_data):
    X, y = synthetic_data
    m = LSTMClassifier(
        hidden_size=16, sequence_length=10, epochs=2, batch_size=32, lr=0.01,
    )
    m.fit(X, y)
    proba = m.predict_proba(X)
    assert proba.shape == (len(X), 3)
    row_sums = proba.sum(axis=1)
    assert np.allclose(row_sums, 1.0, atol=1e-4)


def test_lstm_uses_cpu_when_cuda_unavailable(monkeypatch):
    import torch
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    m = LSTMClassifier(hidden_size=8, sequence_length=5, epochs=1, batch_size=16, lr=0.01)
    assert m.device.type == "cpu"


def test_lstm_early_stopping_runs(synthetic_data):
    X, y = synthetic_data
    m = LSTMClassifier(
        hidden_size=16, sequence_length=10, epochs=100, batch_size=32, lr=0.01,
        early_stopping_patience=2,
    )
    m.fit(X, y)
    assert m.epochs_trained_ < 100
