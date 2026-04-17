import numpy as np
import pandas as pd
import pytest
from services.model_service.meta.estimators.baseline import BaselineEntryEstimator, BaselineExecEstimator
from services.model_service.meta.protocols import MetaEntryEstimator, MetaExecEstimator


def _make_entry_data(n=200):
    """Realistic feature frame matching MetaFeatureAssembler output."""
    rng = np.random.RandomState(0)
    X = pd.DataFrame({
        "base_1_dir": rng.choice([-1.0, 0.0, 1.0], n),
        "base_1_conf": rng.uniform(0.5, 0.95, n),
        "base_2_dir": rng.choice([-1.0, 0.0, 1.0], n),
        "base_2_conf": rng.uniform(0.5, 0.95, n),
        "regime_spy_trend_1m": rng.choice([-1.0, 0.0, 1.0], n),
        "weekday": rng.randint(0, 5, n).astype(float),
        "minutes_to_close": rng.uniform(0, 390, n),
        "realized_vol": rng.uniform(0.01, 0.05, n),
    })
    y = pd.Series(rng.choice([0, 1], n))
    return X, y


def _make_exec_data(n=200, num_pairs=7):
    rng = np.random.RandomState(0)
    X = pd.DataFrame({
        "base_1_dir": rng.choice([-1.0, 0.0, 1.0], n),
        "base_1_conf": rng.uniform(0.5, 0.95, n),
        "base_2_dir": rng.choice([-1.0, 0.0, 1.0], n),
        "base_2_conf": rng.uniform(0.5, 0.95, n),
        "regime_spy_trend_1m": rng.choice([-1.0, 0.0, 1.0], n),
        "weekday": rng.randint(0, 5, n).astype(float),
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

    def test_high_agreement_gives_high_proba(self):
        """All base models agree strongly long -> P(positive) > 0.7."""
        n = 20
        X = pd.DataFrame({
            "base_1_dir": np.ones(n),
            "base_1_conf": np.full(n, 0.85),
            "base_2_dir": np.ones(n),
            "base_2_conf": np.full(n, 0.85),
            "regime_spy_trend_1m": np.zeros(n),
            "weekday": np.zeros(n),
        })
        y = pd.Series(np.ones(n, dtype=int))
        est = BaselineEntryEstimator()
        est.fit(X, y)
        proba = est.predict_proba(X)
        assert proba[:, 1].mean() > 0.7

    def test_mixed_agreement_flat(self):
        """base_1 bullish, base_2 bearish at equal confidence -> ~0.5."""
        n = 20
        X = pd.DataFrame({
            "base_1_dir": np.ones(n),
            "base_1_conf": np.full(n, 0.8),
            "base_2_dir": -np.ones(n),
            "base_2_conf": np.full(n, 0.8),
            "weekday": np.zeros(n),
        })
        y = pd.Series(np.zeros(n, dtype=int))
        est = BaselineEntryEstimator()
        est.fit(X, y)
        proba = est.predict_proba(X)
        assert np.allclose(proba[:, 1], 0.5, atol=1e-6)

    def test_low_confidence_filtered(self):
        """All base models below min_confidence threshold -> default to 0.5."""
        n = 20
        X = pd.DataFrame({
            "base_1_dir": np.ones(n),
            "base_1_conf": np.full(n, 0.3),   # below default 0.6
            "base_2_dir": np.ones(n),
            "base_2_conf": np.full(n, 0.3),
            "weekday": np.zeros(n),
        })
        y = pd.Series(np.ones(n, dtype=int))
        est = BaselineEntryEstimator()
        est.fit(X, y)
        proba = est.predict_proba(X)
        assert np.allclose(proba[:, 1], 0.5, atol=1e-6)

    def test_temporal_features_ignored(self):
        """Large weekday / minutes_to_close values don't affect output when base_* are neutral."""
        n = 20
        # base models mixed -> neutral
        X = pd.DataFrame({
            "base_1_dir": np.ones(n),
            "base_1_conf": np.full(n, 0.8),
            "base_2_dir": -np.ones(n),
            "base_2_conf": np.full(n, 0.8),
            "weekday": np.full(n, 4.0),
            "minutes_to_close": np.full(n, 390.0),  # would saturate old row-mean to 1.0
        })
        y = pd.Series(np.zeros(n, dtype=int))
        est = BaselineEntryEstimator()
        est.fit(X, y)
        proba = est.predict_proba(X)
        # Should still be ~0.5 because temporal columns are ignored
        assert np.allclose(proba[:, 1], 0.5, atol=1e-6)

    def test_feature_importances_shape(self):
        X, y = _make_entry_data()
        est = BaselineEntryEstimator()
        est.fit(X, y)
        assert est.feature_importances().shape == (X.shape[1],)

    def test_feature_importances_all_zero(self):
        """Baseline has no learned importances — all zeros."""
        X, y = _make_entry_data()
        est = BaselineEntryEstimator()
        est.fit(X, y)
        assert np.all(est.feature_importances() == 0.0)

    def test_get_params(self):
        est = BaselineEntryEstimator()
        params = est.get_params()
        assert isinstance(params, dict)
        assert "min_confidence" in params
        assert "direction_band" in params


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
