from __future__ import annotations
import numpy as np
import pandas as pd


class BaselineEntryEstimator:
    """Confidence-weighted voting baseline (no learning).

    Averages all numeric feature columns as a proxy confidence score and
    converts that into a two-class probability vector.  Calling fit() merely
    records the column names and the feature-importance weights so the
    interface matches the protocol.
    """

    def __init__(self) -> None:
        self._columns: list[str] = []
        self._feature_imp: np.ndarray | None = None

    def fit(self, X: pd.DataFrame, y: pd.Series, sample_weight: np.ndarray | None = None) -> None:
        self._columns = list(X.columns)
        # Uniform importance across all features
        self._feature_imp = np.ones(len(self._columns)) / len(self._columns)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        # Row-wise mean of all features as a raw confidence signal,
        # clipped to [0, 1] to serve as P(positive class).
        raw = X.values.mean(axis=1)
        p_pos = np.clip(raw, 0.0, 1.0)
        return np.column_stack([1.0 - p_pos, p_pos])

    def feature_importances(self) -> np.ndarray:
        if self._feature_imp is not None:
            return self._feature_imp
        return np.zeros(len(self._columns))

    def get_params(self) -> dict:
        return {}


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
