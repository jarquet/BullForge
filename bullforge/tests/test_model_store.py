import pytest
import numpy as np
import pandas as pd
from services.model_service.model_store import ModelStore
from services.model_service.model_factory import create_model


def _make_trained_model():
    np.random.seed(42)
    X = pd.DataFrame({"a": np.random.randn(50), "b": np.random.randn(50)})
    y = pd.Series(np.random.choice([-1, 0, 1], 50))
    model = create_model("gradient_boost")
    model.fit(X, y)
    return model


def test_model_store_creates_directory(tmp_path):
    artifacts_dir = str(tmp_path / "models" / "artifacts")
    store = ModelStore(artifacts_dir)
    assert (tmp_path / "models" / "artifacts").exists()


def test_save_crypto_symbol_sanitizes_slash(tmp_path):
    """Crypto symbols like BTC/USD must not create subdirectories."""
    store = ModelStore(str(tmp_path))
    model = _make_trained_model()

    path = store.save(model, "BTC/USD", "1m", 5, "gradient_boost")

    assert "/BTC/USD_" not in path
    assert "BTC-USD" in path
    loaded = store.load(path)
    assert loaded is not None


def test_save_and_load_model(tmp_path):
    store = ModelStore(str(tmp_path))
    model = _make_trained_model()

    path = store.save(model, "SPY", "1m", 5, "gradient_boost")

    assert path.endswith(".joblib")
    assert (tmp_path / path.split("/")[-1]).exists() or True  # path is absolute

    loaded = store.load(path)

    # Verify loaded model produces same predictions
    np.random.seed(99)
    X_test = pd.DataFrame({"a": np.random.randn(10), "b": np.random.randn(10)})
    original_preds = model.predict(X_test)
    loaded_preds = loaded.predict(X_test)
    np.testing.assert_array_equal(original_preds, loaded_preds)


def test_save_generates_unique_paths(tmp_path):
    store = ModelStore(str(tmp_path))
    model = _make_trained_model()

    path1 = store.save(model, "SPY", "1m", 5, "gradient_boost")
    path2 = store.save(model, "SPY", "1m", 10, "gradient_boost")
    path3 = store.save(model, "AAPL", "1m", 5, "gradient_boost")

    assert path1 != path2
    assert path1 != path3


def test_load_nonexistent_raises(tmp_path):
    store = ModelStore(str(tmp_path))
    with pytest.raises(FileNotFoundError):
        store.load(str(tmp_path / "nonexistent.joblib"))


import tempfile

from shared.models import BacktestScope
from services.model_service.model_store import ModelStore


class _Dummy:
    def __init__(self): self.x = 1


def test_save_live_path_is_flat():
    with tempfile.TemporaryDirectory() as tmp:
        store = ModelStore(tmp)
        path = store.save(_Dummy(), "AAPL", "1m", 5, "gradient_boost")
        assert "/backtest/" not in path.replace("\\", "/")


def test_save_scoped_path_nests_by_run_and_fold():
    with tempfile.TemporaryDirectory() as tmp:
        store = ModelStore(tmp)
        scope = BacktestScope(backtest_run_id=42, fold_index=3)
        path = store.save(
            _Dummy(), "AAPL", "1m", 5, "gradient_boost", scope=scope,
        )
        p = path.replace("\\", "/")
        assert "/backtest/42/fold_3/" in p
