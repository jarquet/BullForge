from __future__ import annotations

import logging
from typing import Dict, List, Tuple

import pandas as pd

from shared.db import Database
from shared.models import ModelMetadata, Prediction
from services.model_service.model_store import ModelStore

logger = logging.getLogger(__name__)


class InferenceEngine:
    def __init__(self, db: Database, model_store: ModelStore):
        self._db = db
        self._model_store = model_store
        self._models: Dict[
            Tuple[str, str, int], Tuple[object, ModelMetadata]
        ] = {}

    async def load_models(self) -> None:
        active_models = await self._db.query_all_active_models()
        self._models.clear()

        for meta in active_models:
            try:
                model = self._model_store.load(meta.artifact_path)
                key = (meta.symbol, meta.timeframe, meta.horizon_minutes)
                self._models[key] = (model, meta)
                logger.info(
                    "Loaded model: %s %s %dm (%s, id=%d)",
                    meta.symbol, meta.timeframe, meta.horizon_minutes,
                    meta.model_type, meta.id,
                )
            except FileNotFoundError:
                logger.warning(
                    "Artifact not found for model id=%d: %s",
                    meta.id, meta.artifact_path,
                )

        logger.info("Loaded %d models for inference", len(self._models))

    async def predict(
        self, symbol: str, timeframe: str
    ) -> List[Prediction]:
        matching = [
            (key, model, meta)
            for key, (model, meta) in self._models.items()
            if key[0] == symbol and key[1] == timeframe
        ]

        if not matching:
            return []

        feature_rows = await self._db.query_feature_rows(
            symbol, timeframe, limit=1,
        )
        if not feature_rows:
            logger.debug("No features available for %s %s", symbol, timeframe)
            return []

        latest_features = feature_rows[0]
        predictions = []

        for key, model, meta in matching:
            horizon = key[2]
            feature_dict = {
                col: latest_features.features.get(col, 0.0)
                for col in meta.feature_columns
            }
            X = pd.DataFrame([feature_dict])

            direction = int(model.predict(X)[0])
            proba = model.predict_proba(X)[0]
            confidence = float(proba.max())

            predictions.append(Prediction(
                symbol=symbol,
                timestamp=latest_features.timestamp,
                timeframe=timeframe,
                horizon_minutes=horizon,
                model_id=meta.id,
                predicted_direction=direction,
                confidence=confidence,
            ))

        return predictions
