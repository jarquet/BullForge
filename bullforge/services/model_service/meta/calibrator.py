from __future__ import annotations
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from services.model_service.meta.protocols import MetaEntryEstimator

class CalibratedEntryEstimator:
    """Wraps any MetaEntryEstimator with isotonic regression calibration."""
    def __init__(self, base: MetaEntryEstimator):
        self._base = base
        self._calibrator = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
        self._is_fitted = False

    def fit(self, X: pd.DataFrame, y: pd.Series, sample_weight: np.ndarray | None = None,
            *, X_cal: pd.DataFrame | None = None, y_cal: pd.Series | None = None) -> None:
        self._base.fit(X, y, sample_weight=sample_weight)
        if X_cal is not None and y_cal is not None:
            raw_proba = self._base.predict_proba(X_cal)[:, 1]
            self._calibrator.fit(raw_proba, y_cal.values)
            self._is_fitted = True

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        raw = self._base.predict_proba(X)
        if not self._is_fitted:
            return raw
        calibrated_pos = np.clip(self._calibrator.predict(raw[:, 1]), 0.0, 1.0)
        return np.column_stack([1.0 - calibrated_pos, calibrated_pos])

    def feature_importances(self) -> np.ndarray:
        return self._base.feature_importances()

    def get_params(self) -> dict:
        return {**self._base.get_params(), "calibrated": True}
