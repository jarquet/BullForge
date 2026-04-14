from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import joblib
import torch

from services.model_service.lstm_model import LSTMClassifier

logger = logging.getLogger(__name__)


class ModelStore:
    def __init__(self, artifacts_dir: str):
        self._dir = Path(artifacts_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    def save(self, model: object, symbol: str, timeframe: str,
             horizon_minutes: int, model_type: str) -> str:
        now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        if isinstance(model, LSTMClassifier):
            ext = "pt"
        else:
            ext = "joblib"
        safe_symbol = symbol.replace("/", "-")
        filename = f"{model_type}_{safe_symbol}_{timeframe}_{horizon_minutes}m_{now}.{ext}"
        path = self._dir / filename
        if isinstance(model, LSTMClassifier):
            torch.save(model, path)
        else:
            joblib.dump(model, path)
        logger.info("Saved model artifact to %s", path)
        return str(path)

    def load(self, artifact_path: str) -> object:
        path = Path(artifact_path)
        if not path.exists():
            raise FileNotFoundError(f"Model artifact not found: {artifact_path}")
        if path.suffix == ".pt":
            model = torch.load(path, weights_only=False, map_location="cpu")
            if isinstance(model, LSTMClassifier) and model._net is not None:
                model.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
                model._net = model._net.to(model.device)
        else:
            model = joblib.load(path)
        logger.info("Loaded model artifact from %s", path)
        return model
