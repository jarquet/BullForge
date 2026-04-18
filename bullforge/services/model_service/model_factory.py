from __future__ import annotations
from typing import Optional

from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from services.model_service.lstm_model import LSTMClassifier

SUPPORTED_MODELS = {"gradient_boost", "logistic_regression", "lstm"}


def create_model(model_type: str, runtime_config: Optional[dict] = None) -> object:
    cfg = runtime_config or {}
    if model_type == "gradient_boost":
        return Pipeline([
            ("scaler", StandardScaler()),
            ("classifier", HistGradientBoostingClassifier(
                max_iter=100, max_depth=3, learning_rate=0.1, random_state=42,
            )),
        ])
    if model_type == "logistic_regression":
        return Pipeline([
            ("scaler", StandardScaler()),
            ("classifier", LogisticRegression(max_iter=1000, random_state=42)),
        ])
    if model_type == "lstm":
        return LSTMClassifier(
            hidden_size=cfg.get("lstm_hidden_size", 64),
            sequence_length=cfg.get("lstm_sequence_length", 30),
            lr=cfg.get("lstm_learning_rate", 0.001),
            epochs=cfg.get("lstm_epochs", 50),
            batch_size=cfg.get("lstm_batch_size", 64),
        )
    raise ValueError(
        f"Unknown model type: {model_type}. Supported: {SUPPORTED_MODELS}"
    )
