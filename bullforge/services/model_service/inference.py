from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd

from shared.db import Database
from shared.models import BacktestScope, ModelMetadata, Prediction
from services.model_service.model_store import ModelStore

logger = logging.getLogger(__name__)


class InferenceEngine:
    def __init__(
        self,
        db: Database,
        model_store: ModelStore,
        *,
        scope: Optional[BacktestScope] = None,
    ):
        self._db = db
        self._model_store = model_store
        self._scope = scope
        self._models: Dict[
            Tuple[str, str, int], Tuple[object, ModelMetadata]
        ] = {}

    async def load_models(self) -> None:
        active_models = await self._db.query_all_active_models(scope=self._scope)
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

        # Fetch a window large enough for sequence models (LSTM seq_len=30).
        # Gradient-boost ignores history; LSTM needs the window.
        feature_rows = await self._db.query_feature_rows(
            symbol, timeframe, limit=60,
        )
        if not feature_rows:
            logger.debug("No features available for %s %s", symbol, timeframe)
            return []

        # query_feature_rows returns DESC; flip to ascending so the last row is latest.
        feature_rows = list(reversed(feature_rows))
        latest_features = feature_rows[-1]
        predictions = []

        for key, model, meta in matching:
            horizon = key[2]
            rows = [
                {col: fr.features.get(col, 0.0) for col in meta.feature_columns}
                for fr in feature_rows
            ]
            X = pd.DataFrame(rows)

            try:
                direction = int(model.predict(X)[-1])
                proba = model.predict_proba(X)[-1]
            except ValueError as e:
                logger.warning(
                    "Skipping %s %s %dm (id=%d): %s",
                    symbol, timeframe, horizon, meta.id, e,
                )
                continue
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
