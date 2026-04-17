from __future__ import annotations
import numpy as np
import pandas as pd


class BaselineEntryEstimator:
    """Wraps confidence-weighted voting over base-model columns.

    Serves as the always-available control group in A/B/n comparison.
    Looks at `base_N_dir` and `base_N_conf` columns in the feature vector
    produced by MetaFeatureAssembler, ignoring regime/temporal/symbol features.
    fit() records which (dir, conf) column pairs exist in the feature frame.
    """

    def __init__(self, min_confidence: float = 0.6, direction_band: float = 0.3):
        self._min_confidence = min_confidence
        self._direction_band = direction_band
        self._dir_conf_pairs: list[tuple[str, str]] = []
        self._n_features: int = 0

    def fit(self, X: pd.DataFrame, y: pd.Series, sample_weight: np.ndarray | None = None) -> None:
        self._n_features = X.shape[1]
        # Auto-discover base_N_dir / base_N_conf column pairs from the feature frame
        dir_cols = sorted([c for c in X.columns if c.startswith("base_") and c.endswith("_dir")])
        self._dir_conf_pairs = []
        for dir_col in dir_cols:
            conf_col = dir_col.replace("_dir", "_conf")
            if conf_col in X.columns:
                self._dir_conf_pairs.append((dir_col, conf_col))

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        n = len(X)
        proba = np.zeros((n, 2), dtype=np.float64)

        if not self._dir_conf_pairs:
            proba[:] = [0.5, 0.5]
            return proba

        for i in range(n):
            weighted_sum = 0.0
            total_weight = 0.0
            for dir_col, conf_col in self._dir_conf_pairs:
                direction = float(X.iloc[i][dir_col])
                confidence = float(X.iloc[i][conf_col])
                if confidence >= self._min_confidence:
                    weighted_sum += direction * confidence
                    total_weight += confidence

            if total_weight > 0:
                avg = weighted_sum / total_weight
                strength = min(abs(avg), 1.0)
                if avg > self._direction_band:
                    proba[i, 1] = 0.5 + strength * 0.4
                    proba[i, 0] = 1.0 - proba[i, 1]
                elif avg < -self._direction_band:
                    proba[i, 0] = 0.5 + strength * 0.4
                    proba[i, 1] = 1.0 - proba[i, 0]
                else:
                    proba[i] = [0.5, 0.5]
            else:
                proba[i] = [0.5, 0.5]

        return proba

    def feature_importances(self) -> np.ndarray:
        imp = np.zeros(self._n_features)
        return imp

    def get_params(self) -> dict:
        return {
            "min_confidence": self._min_confidence,
            "direction_band": self._direction_band,
            "dir_conf_pairs": self._dir_conf_pairs,
        }


class BaselineExecEstimator:
    """Median-pair + constant-kelly baseline (no learning).

    fit() records the number of pairs and computes the median kelly from
    training labels.  predict() always returns that constant kelly and the
    median pair index regardless of input features.
    """

    def __init__(self, num_pairs: int, constant_kelly: float = 0.1) -> None:
        self._num_pairs = num_pairs
        self._constant_kelly = constant_kelly
        self._median_pair: int = num_pairs // 2
        self._feature_imp: np.ndarray | None = None
        self._n_features: int = 0

    def fit(self, X: pd.DataFrame, y_kelly: pd.Series, y_pair: pd.Series, sample_weight: np.ndarray | None = None) -> None:
        self._n_features = X.shape[1]
        self._constant_kelly = float(np.median(y_kelly.values))
        self._median_pair = int(np.median(y_pair.values))
        self._feature_imp = np.ones(self._n_features) / self._n_features

    def predict(self, X: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        n = len(X)
        kelly_fracs = np.full(n, self._constant_kelly, dtype=np.float64)
        pair_indices = np.full(n, self._median_pair, dtype=np.int64)
        return kelly_fracs, pair_indices

    def feature_importances(self) -> np.ndarray:
        if self._feature_imp is not None:
            return self._feature_imp
        return np.zeros(self._n_features)

    def get_params(self) -> dict:
        return {"num_pairs": self._num_pairs, "constant_kelly": self._constant_kelly}
