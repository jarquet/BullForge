import numpy as np
import pandas as pd
import pytest
from services.model_service.meta.estimators.lightgbm import LightGBMEntryEstimator, LightGBMExecEstimator
from services.model_service.meta.protocols import MetaEntryEstimator, MetaExecEstimator

def _make_entry_data(n=500):
    rng = np.random.RandomState(42)
    X = pd.DataFrame({"base_L1_conf": rng.uniform(0.4, 0.95, n), "base_L2_conf": rng.uniform(0.4, 0.95, n), "regime_spy_trend": rng.choice([-1, 0, 1], n).astype(float), "time_bucket": rng.randint(0, 26, n).astype(float), "realized_vol": rng.uniform(0.01, 0.05, n)})
    y = pd.Series(rng.choice([0, 1], n, p=[0.45, 0.55]))
    return X, y

def _make_exec_data(n=500, num_pairs=7):
    rng = np.random.RandomState(42)
    X = pd.DataFrame({"base_L1_conf": rng.uniform(0.4, 0.95, n), "base_L2_conf": rng.uniform(0.4, 0.95, n), "regime_spy_trend": rng.choice([-1, 0, 1], n).astype(float), "time_bucket": rng.randint(0, 26, n).astype(float), "realized_vol": rng.uniform(0.01, 0.05, n)})
    y_kelly = pd.Series(rng.uniform(0, 0.5, n))
    y_pair = pd.Series(rng.randint(0, num_pairs, n))
    return X, y_kelly, y_pair

class TestLightGBMEntry:
    def test_implements_protocol(self):
        assert isinstance(LightGBMEntryEstimator(), MetaEntryEstimator)

    def test_fit_predict_proba(self):
        X, y = _make_entry_data()
        est = LightGBMEntryEstimator()
        est.fit(X, y)
        proba = est.predict_proba(X[:5])
        assert proba.shape == (5, 2)
        assert np.allclose(proba.sum(axis=1), 1.0, atol=1e-6)

    def test_predict_proba_range(self):
        X, y = _make_entry_data()
        est = LightGBMEntryEstimator()
        est.fit(X, y)
        proba = est.predict_proba(X)
        assert (proba >= 0.0).all() and (proba <= 1.0).all()

    def test_feature_importances_shape(self):
        X, y = _make_entry_data()
        est = LightGBMEntryEstimator()
        est.fit(X, y)
        assert est.feature_importances().shape == (X.shape[1],)

    def test_sample_weight(self):
        X, y = _make_entry_data()
        weights = np.ones(len(X)); weights[:50] = 3.0
        est = LightGBMEntryEstimator()
        est.fit(X, y, sample_weight=weights)
        assert est.predict_proba(X[:5]).shape == (5, 2)

    def test_get_params(self):
        est = LightGBMEntryEstimator(n_estimators=50)
        assert est.get_params()["n_estimators"] == 50

class TestLightGBMExec:
    def test_implements_protocol(self):
        assert isinstance(LightGBMExecEstimator(num_pairs=7), MetaExecEstimator)

    def test_fit_predict(self):
        X, y_kelly, y_pair = _make_exec_data(num_pairs=7)
        est = LightGBMExecEstimator(num_pairs=7)
        est.fit(X, y_kelly, y_pair)
        kelly_fracs, pair_indices = est.predict(X[:5])
        assert kelly_fracs.shape == (5,)
        assert pair_indices.shape == (5,)
        assert (kelly_fracs >= 0.0).all() and (kelly_fracs <= 1.0).all()
        assert all(0 <= idx < 7 for idx in pair_indices)

    def test_feature_importances_shape(self):
        X, y_kelly, y_pair = _make_exec_data(num_pairs=7)
        est = LightGBMExecEstimator(num_pairs=7)
        est.fit(X, y_kelly, y_pair)
        assert est.feature_importances().shape == (X.shape[1],)
