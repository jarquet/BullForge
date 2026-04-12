from __future__ import annotations

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

SUPPORTED_MODELS = {"gradient_boost", "logistic_regression"}


def create_model(model_type: str) -> Pipeline:
    if model_type == "gradient_boost":
        return Pipeline([
            ("scaler", StandardScaler()),
            ("classifier", GradientBoostingClassifier(
                n_estimators=100,
                max_depth=3,
                learning_rate=0.1,
                random_state=42,
            )),
        ])
    elif model_type == "logistic_regression":
        return Pipeline([
            ("scaler", StandardScaler()),
            ("classifier", LogisticRegression(
                max_iter=1000,
                random_state=42,
            )),
        ])
    else:
        raise ValueError(
            f"Unknown model type: {model_type}. Supported: {SUPPORTED_MODELS}"
        )
