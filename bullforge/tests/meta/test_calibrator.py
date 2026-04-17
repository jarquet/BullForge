import numpy as np
import pandas as pd
import pytest
from services.model_service.meta.calibrator import CalibratedEntryEstimator
from services.model_service.meta.estimators.lightgbm import LightGBMEntryEstimator

def _make_data(n=500):
    rng = np.random.RandomState(42)
    X = pd.DataFrame({"f1": rng.uniform(0, 1, n), "f2": rng.uniform(0, 1, n), "f3": rng.uniform(0, 1, n)})
    y = pd.Series((X["f1"] + X["f2"] > 1.0).astype(int))
    return X, y

class TestCalibratedEntryEstimator:
    def test_fit_and_predict_proba(self):
        X, y = _make_data()
        cal = CalibratedEntryEstimator(LightGBMEntryEstimator())
        cal.fit(X.iloc[:400], y.iloc[:400], X_cal=X.iloc[400:], y_cal=y.iloc[400:])
        proba = cal.predict_proba(X.iloc[400:])
        assert proba.shape == (100, 2)
        assert np.allclose(proba.sum(axis=1), 1.0, atol=1e-6)

    def test_calibrated_proba_in_range(self):
        X, y = _make_data()
        cal = CalibratedEntryEstimator(LightGBMEntryEstimator())
        cal.fit(X.iloc[:400], y.iloc[:400], X_cal=X.iloc[400:], y_cal=y.iloc[400:])
        proba = cal.predict_proba(X)
        assert (proba >= 0.0).all() and (proba <= 1.0).all()

    def test_feature_importances_delegates(self):
        X, y = _make_data()
        cal = CalibratedEntryEstimator(LightGBMEntryEstimator())
        cal.fit(X.iloc[:400], y.iloc[:400], X_cal=X.iloc[400:], y_cal=y.iloc[400:])
        assert cal.feature_importances().shape == (3,)

    def test_get_params_includes_calibrated_flag(self):
        cal = CalibratedEntryEstimator(LightGBMEntryEstimator())
        assert cal.get_params()["calibrated"] is True

    def test_calibration_error_reduced(self):
        rng = np.random.RandomState(42)
        n = 1000
        X = pd.DataFrame({"f1": rng.uniform(0, 1, n), "f2": rng.uniform(0, 1, n)})
        y = pd.Series((X["f1"] + X["f2"] > 1.0).astype(int))

        base = LightGBMEntryEstimator(n_estimators=50)
        base.fit(X.iloc[:700], y.iloc[:700])
        raw_proba = base.predict_proba(X.iloc[700:])[:, 1]
        raw_cal_error = _calibration_error(y.iloc[700:].values, raw_proba, n_bins=5)

        cal = CalibratedEntryEstimator(LightGBMEntryEstimator(n_estimators=50))
        cal.fit(X.iloc[:700], y.iloc[:700], X_cal=X.iloc[700:800], y_cal=y.iloc[700:800])
        cal_proba = cal.predict_proba(X.iloc[800:])[:, 1]
        cal_cal_error = _calibration_error(y.iloc[800:].values, cal_proba, n_bins=5)
        assert cal_cal_error <= raw_cal_error + 0.05

def _calibration_error(y_true, y_prob, n_bins=10):
    bins = np.linspace(0, 1, n_bins + 1)
    total_error = total_count = 0.0
    for lo, hi in zip(bins[:-1], bins[1:]):
        mask = (y_prob >= lo) & (y_prob < hi)
        if mask.sum() == 0: continue
        total_error += mask.sum() * abs(y_prob[mask].mean() - y_true[mask].mean())
        total_count += mask.sum()
    return total_error / max(total_count, 1)
