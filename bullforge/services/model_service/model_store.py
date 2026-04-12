from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import joblib

logger = logging.getLogger(__name__)


class ModelStore:
    def __init__(self, artifacts_dir: str):
        self._dir = Path(artifacts_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    def save(
        self,
        model: object,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        model_type: str,
    ) -> str:
        now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        filename = f"{model_type}_{symbol}_{timeframe}_{horizon_minutes}m_{now}.joblib"
        path = self._dir / filename
        joblib.dump(model, path)
        logger.info("Saved model artifact to %s", path)
        return str(path)

    def load(self, artifact_path: str) -> object:
        path = Path(artifact_path)
        if not path.exists():
            raise FileNotFoundError(f"Model artifact not found: {artifact_path}")
        model = joblib.load(path)
        logger.info("Loaded model artifact from %s", path)
        return model
