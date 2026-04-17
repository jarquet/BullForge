from __future__ import annotations
import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier, LGBMRegressor

class LightGBMEntryEstimator:
    def __init__(self, n_estimators: int = 200, max_depth: int = 5, learning_rate: float = 0.05, min_child_samples: int = 20, random_state: int = 42):
        self._params = {"n_estimators": n_estimators, "max_depth": max_depth, "learning_rate": learning_rate, "min_child_samples": min_child_samples, "random_state": random_state}
        self._model = LGBMClassifier(objective="binary", verbose=-1, **self._params)

    def fit(self, X: pd.DataFrame, y: pd.Series, sample_weight: np.ndarray | None = None) -> None:
        self._model.fit(X, y, sample_weight=sample_weight)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        return self._model.predict_proba(X)

    def feature_importances(self) -> np.ndarray:
        return self._model.feature_importances_

    def get_params(self) -> dict:
        return dict(self._params)

class LightGBMExecEstimator:
    def __init__(self, num_pairs: int, n_estimators: int = 200, max_depth: int = 5, learning_rate: float = 0.05, min_child_samples: int = 20, random_state: int = 42):
        self._num_pairs = num_pairs
        self._params = {"n_estimators": n_estimators, "max_depth": max_depth, "learning_rate": learning_rate, "min_child_samples": min_child_samples, "random_state": random_state}
        self._kelly_model = LGBMRegressor(objective="regression", verbose=-1, **self._params)
        self._pair_model = LGBMClassifier(objective="multiclass", num_class=num_pairs, verbose=-1, **self._params)

    def fit(self, X: pd.DataFrame, y_kelly: pd.Series, y_pair: pd.Series, sample_weight: np.ndarray | None = None) -> None:
        self._kelly_model.fit(X, y_kelly, sample_weight=sample_weight)
        self._pair_model.fit(X, y_pair, sample_weight=sample_weight)

    def predict(self, X: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        kelly_raw = self._kelly_model.predict(X)
        kelly_fracs = np.clip(kelly_raw, 0.0, 1.0)
        pair_proba = self._pair_model.predict_proba(X)
        pair_indices = np.argmax(pair_proba, axis=1)
        return kelly_fracs, pair_indices

    def feature_importances(self) -> np.ndarray:
        return (self._kelly_model.feature_importances_ + self._pair_model.feature_importances_) / 2.0

    def get_params(self) -> dict:
        return {"num_pairs": self._num_pairs, **self._params}
