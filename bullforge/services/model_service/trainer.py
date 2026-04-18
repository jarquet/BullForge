from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
)

from shared.db import Database
from shared.models import BacktestScope, ModelMetadata
from services.model_service.dataset import DatasetBuilder, time_based_split
from services.model_service.model_factory import create_model
from services.model_service.model_store import ModelStore

logger = logging.getLogger(__name__)


class Trainer:
    def __init__(
        self,
        db: Database,
        model_store: ModelStore,
        min_samples: int = 500,
        min_samples_1d: int = 84,
        *,
        scope: Optional[BacktestScope] = None,
    ):
        self._db = db
        self._model_store = model_store
        self._scope = scope
        self._dataset_builder = DatasetBuilder(
            db, min_samples=min_samples, min_samples_1d=min_samples_1d,
        )

    async def train(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        *,
        as_of: datetime,
        model_type: str = "gradient_boost",
        runtime_config: Optional[dict] = None,
    ) -> Optional[ModelMetadata]:
        logger.info(
            "Training %s model for %s %s %dm...",
            model_type, symbol, timeframe, horizon_minutes,
        )

        result = await self._dataset_builder.build(
            symbol, timeframe, horizon_minutes, as_of=as_of,
        )
        if result is None:
            return None

        X, y = result
        X_train, X_test, y_train, y_test = time_based_split(X, y, test_ratio=0.2)

        model = create_model(model_type, runtime_config=runtime_config)
        model.fit(X_train, y_train)

        metrics = self._evaluate(model, X_test, y_test)
        logger.info(
            "Model %s %s %s %dm — accuracy=%.3f f1_macro=%.3f",
            model_type, symbol, timeframe, horizon_minutes,
            metrics["accuracy"], metrics["f1_macro"],
        )

        artifact_path = self._model_store.save(
            model, symbol, timeframe, horizon_minutes, model_type,
            scope=self._scope,
        )

        trained_at = datetime.now(timezone.utc)
        feature_columns = list(X.columns)

        await self._db.deactivate_models(
            symbol, timeframe, horizon_minutes, scope=self._scope,
        )

        meta = ModelMetadata(
            symbol=symbol,
            timeframe=timeframe,
            horizon_minutes=horizon_minutes,
            model_type=model_type,
            artifact_path=artifact_path,
            metrics=metrics,
            feature_columns=feature_columns,
            trained_at=trained_at,
            train_samples=len(X_train),
            test_samples=len(X_test),
            is_active=True,
        )

        row_id = await self._db.insert_model_metadata(meta, scope=self._scope)
        meta = ModelMetadata(
            id=row_id,
            symbol=meta.symbol,
            timeframe=meta.timeframe,
            horizon_minutes=meta.horizon_minutes,
            model_type=meta.model_type,
            artifact_path=meta.artifact_path,
            metrics=meta.metrics,
            feature_columns=meta.feature_columns,
            trained_at=meta.trained_at,
            train_samples=meta.train_samples,
            test_samples=meta.test_samples,
            is_active=meta.is_active,
        )

        logger.info(
            "Model saved: id=%d artifact=%s", row_id, artifact_path,
        )
        return meta

    def _evaluate(
        self,
        model: object,
        X_test: pd.DataFrame,
        y_test: pd.Series,
    ) -> dict[str, float]:
        y_pred = model.predict(X_test)
        dist = Counter(y_test)
        return {
            "accuracy": float(accuracy_score(y_test, y_pred)),
            "precision_macro": float(
                precision_score(y_test, y_pred, average="macro", zero_division=0)
            ),
            "recall_macro": float(
                recall_score(y_test, y_pred, average="macro", zero_division=0)
            ),
            "f1_macro": float(
                f1_score(y_test, y_pred, average="macro", zero_division=0)
            ),
            "support_neg": float(dist.get(-1, 0)),
            "support_flat": float(dist.get(0, 0)),
            "support_pos": float(dist.get(1, 0)),
        }
