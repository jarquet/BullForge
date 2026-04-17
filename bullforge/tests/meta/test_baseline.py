import numpy as np
import pandas as pd
import pytest
from services.model_service.meta.estimators.baseline import BaselineEntryEstimator, BaselineExecEstimator
from services.model_service.meta.protocols import MetaEntryEstimator, MetaExecEstimator

def _make_entry_data(n=200):
    rng = np.random.RandomState(0)
    X = pd.DataFrame({
        "base_L1_conf": rng.uniform(0.4, 0.95, n),
        "base_L2_conf": rng.uniform(0.4, 0.95, n),
        "regime_spy_trend": rng.choice([-1, 0, 1], n).astype(float),
        "time_bucket": rng.randint(0, 26, n).astype(float),
        "realized_vol": rng.uniform(0.01, 0.05, n),
    })
    y = pd.Series(rng.choice([0, 1], n))
    return X, y

def _make_exec_data(n=200, num_pairs=7):
    rng = np.random.RandomState(0)
    X = pd.DataFrame({
        "base_L1_conf": rng.uniform(0.4, 0.95, n),
        "base_L2_conf": rng.uniform(0.4, 0.95, n),
        "regime_spy_trend": rng.choice([-1, 0, 1], n).astype(float),
        "time_bucket": rng.randint(0, 26, n).astype(float),
        "realized_vol": rng.uniform(0.01, 0.05, n),
    })
    y_kelly = pd.Series(rng.uniform(0, 0.5, n))
    y_pair = pd.Series(rng.randint(0, num_pairs, n))
    return X, y_kelly, y_pair

class TestBaselineEntry:
    def test_implements_protocol(self):
        assert isinstance(BaselineEntryEstimator(), MetaEntryEstimator)

    def test_fit_predict_proba_shape(self):
        X, y = _make_entry_data()
        est = BaselineEntryEstimator()
        est.fit(X, y)
        proba = est.predict_proba(X[:10])
        assert proba.shape == (10, 2)

    def test_predict_proba_sums_to_one(self):
        X, y = _make_entry_data()
        est = BaselineEntryEstimator()
        est.fit(X, y)
        proba = est.predict_proba(X)
        assert np.allclose(proba.sum(axis=1), 1.0, atol=1e-6)

    def test_predict_proba_range(self):
        X, y = _make_entry_data()
        est = BaselineEntryEstimator()
        est.fit(X, y)
        proba = est.predict_proba(X)
        assert (proba >= 0.0).all() and (proba <= 1.0).all()

    def test_high_confidence_features_give_high_proba(self):
        # Features all near 1.0 should yield high P(positive)
        X_high = pd.DataFrame({
            "f1": [0.9, 0.95, 0.85],
            "f2": [0.92, 0.88, 0.91],
        })
        X_low = pd.DataFrame({
            "f1": [0.05, 0.02, 0.08],
            "f2": [0.03, 0.07, 0.04],
        })
        y = pd.Series([1, 0, 1])
        est = BaselineEntryEstimator()
        est.fit(X_high, y)
        p_high = est.predict_proba(X_high)[:, 1].mean()
        p_low = est.predict_proba(X_low)[:, 1].mean()
        assert p_high > p_low

    def test_feature_importances_shape(self):
        X, y = _make_entry_data()
        est = BaselineEntryEstimator()
        est.fit(X, y)
        assert est.feature_importances().shape == (X.shape[1],)

    def test_get_params(self):
        est = BaselineEntryEstimator()
        assert isinstance(est.get_params(), dict)

class TestBaselineExec:
    def test_implements_protocol(self):
        assert isinstance(BaselineExecEstimator(num_pairs=7), MetaExecEstimator)

    def test_fit_predict_shape(self):
        X, y_kelly, y_pair = _make_exec_data(num_pairs=7)
        est = BaselineExecEstimator(num_pairs=7)
        est.fit(X, y_kelly, y_pair)
        kelly_fracs, pair_indices = est.predict(X[:10])
        assert kelly_fracs.shape == (10,)
        assert pair_indices.shape == (10,)

    def test_predict_kelly_range(self):
        X, y_kelly, y_pair = _make_exec_data(num_pairs=7)
        est = BaselineExecEstimator(num_pairs=7)
        est.fit(X, y_kelly, y_pair)
        kelly_fracs, _ = est.predict(X)
        assert (kelly_fracs >= 0.0).all() and (kelly_fracs <= 1.0).all()

    def test_predict_pair_indices_valid(self):
        X, y_kelly, y_pair = _make_exec_data(num_pairs=7)
        est = BaselineExecEstimator(num_pairs=7)
        est.fit(X, y_kelly, y_pair)
        _, pair_indices = est.predict(X)
        assert all(0 <= idx < 7 for idx in pair_indices)

    def test_predict_returns_constant(self):
        # Baseline should return identical values for every row
        X, y_kelly, y_pair = _make_exec_data(num_pairs=7)
        est = BaselineExecEstimator(num_pairs=7)
        est.fit(X, y_kelly, y_pair)
        kelly_fracs, pair_indices = est.predict(X)
        assert len(set(kelly_fracs.tolist())) == 1
        assert len(set(pair_indices.tolist())) == 1

    def test_feature_importances_shape(self):
        X, y_kelly, y_pair = _make_exec_data(num_pairs=7)
        est = BaselineExecEstimator(num_pairs=7)
        est.fit(X, y_kelly, y_pair)
        assert est.feature_importances().shape == (X.shape[1],)

    def test_get_params(self):
        est = BaselineExecEstimator(num_pairs=7)
        params = est.get_params()
        assert params["num_pairs"] == 7
