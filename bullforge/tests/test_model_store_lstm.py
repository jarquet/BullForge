import numpy as np
import pandas as pd
import pytest

from services.model_service.model_store import ModelStore
from services.model_service.lstm_model import LSTMClassifier


def test_save_lstm_uses_pt_extension(tmp_path):
    store = ModelStore(str(tmp_path))
    m = LSTMClassifier(hidden_size=8, sequence_length=5, epochs=1, batch_size=16, lr=0.01)
    rng = np.random.default_rng(0)
    X = pd.DataFrame(rng.normal(size=(100, 3)), columns=list("abc"))
    y = pd.Series(rng.choice([-1, 0, 1], size=100))
    m.fit(X, y)

    path = store.save(m, "SPY", "1m", 5, "lstm")
    assert path.endswith(".pt")
    assert "lstm_SPY_1m_5m" in path


def test_load_lstm_roundtrip(tmp_path):
    store = ModelStore(str(tmp_path))
    m = LSTMClassifier(hidden_size=8, sequence_length=5, epochs=1, batch_size=16, lr=0.01)
    rng = np.random.default_rng(1)
    X = pd.DataFrame(rng.normal(size=(100, 3)), columns=list("abc"))
    y = pd.Series(rng.choice([-1, 0, 1], size=100))
    m.fit(X, y)
    path = store.save(m, "SPY", "1m", 5, "lstm")
    loaded = store.load(path)
    assert isinstance(loaded, LSTMClassifier)
    orig = m.predict(X)
    new = loaded.predict(X)
    assert np.array_equal(orig, new)


def test_save_gradient_boost_still_uses_joblib(tmp_path):
    from services.model_service.model_factory import create_model
    store = ModelStore(str(tmp_path))
    m = create_model("gradient_boost")
    rng = np.random.default_rng(2)
    X = pd.DataFrame(rng.normal(size=(50, 3)), columns=list("abc"))
    y = pd.Series(rng.choice([-1, 0, 1], size=50))
    m.fit(X, y)
    path = store.save(m, "SPY", "1m", 5, "gradient_boost")
    assert path.endswith(".joblib")
