from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd

from shared.db import Database

logger = logging.getLogger(__name__)


class DatasetBuilder:
    def __init__(self, db: Database, min_samples: int = 500):
        self._db = db
        self._min_samples = min_samples

    async def build(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        *,
        as_of: datetime,
        limit: int = 10000,
    ) -> Optional[Tuple[pd.DataFrame, pd.Series]]:
        rows = await self._db.query_training_data(
            symbol, timeframe, horizon_minutes, as_of=as_of, limit=limit,
        )

        if len(rows) < self._min_samples:
            logger.warning(
                "Insufficient training data for %s %s %dm: %d < %d",
                symbol, timeframe, horizon_minutes,
                len(rows), self._min_samples,
            )
            return None

        records = []
        directions = []
        for row in rows:
            features = row["features"]
            if isinstance(features, str):
                features = json.loads(features)
            records.append(features)
            directions.append(row["direction"])

        X = pd.DataFrame(records)
        y = pd.Series(directions, name="direction")

        # Drop rows with NaN features (from warmup period)
        nan_mask = X.notna().all(axis=1)
        if not nan_mask.all():
            logger.info(
                "Dropping %d rows with NaN features for %s %s %dm",
                (~nan_mask).sum(), symbol, timeframe, horizon_minutes,
            )
            X = X.loc[nan_mask].reset_index(drop=True)
            y = y.loc[nan_mask].reset_index(drop=True)

        if len(X) < self._min_samples:
            logger.warning(
                "Insufficient training data after NaN cleanup for %s %s %dm: %d < %d",
                symbol, timeframe, horizon_minutes,
                len(X), self._min_samples,
            )
            return None

        return X, y


def time_based_split(
    X: pd.DataFrame,
    y: pd.Series,
    test_ratio: float = 0.2,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    split_idx = int(len(X) * (1 - test_ratio))
    X_train = X.iloc[:split_idx]
    X_test = X.iloc[split_idx:]
    y_train = y.iloc[:split_idx]
    y_test = y.iloc[split_idx:]
    return X_train, X_test, y_train, y_test
