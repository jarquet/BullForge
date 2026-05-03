from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd

from shared.db import Database
from services.feature_engine.exclusion import is_evaluation_window_in_exclusion

logger = logging.getLogger(__name__)


class DatasetBuilder:
    def __init__(
        self,
        db: Database,
        min_samples: int = 500,
        min_samples_1d: int = 84,
    ):
        self._db = db
        self._min_samples = min_samples
        self._min_samples_1d = min_samples_1d

    def _threshold(self, timeframe: str) -> int:
        return self._min_samples_1d if timeframe == "1d" else self._min_samples

    async def build(
        self,
        symbol: str,
        timeframe: str,
        horizon_minutes: int,
        *,
        as_of: datetime,
        limit: int = 10000,
        open_buffer_min: Optional[int] = None,
        close_buffer_min: Optional[int] = None,
    ) -> Optional[Tuple[pd.DataFrame, pd.Series]]:
        rows = await self._db.query_training_data(
            symbol, timeframe, horizon_minutes, as_of=as_of, limit=limit,
        )

        # Drop training rows whose forward-return target lands in the equity
        # exclusion zone — the live RiskManager would refuse to act on those
        # outcomes, so models shouldn't be fit on them. Both buffer kwargs
        # must be ints for the filter to apply; either being None disables
        # it (preserves backward compat for tests / startup paths that don't
        # supply a runtime config). Crypto symbols pass through unfiltered
        # because is_trading_allowed bypasses them.
        if open_buffer_min is not None and close_buffer_min is not None:
            kept = [
                r for r in rows
                if not is_evaluation_window_in_exclusion(
                    symbol, r["timestamp"], horizon_minutes,
                    open_buffer_min=open_buffer_min,
                    close_buffer_min=close_buffer_min,
                )
            ]
            if len(kept) < len(rows):
                logger.info(
                    "Exclusion zone dropped %d/%d rows for %s %s %dm",
                    len(rows) - len(kept), len(rows),
                    symbol, timeframe, horizon_minutes,
                )
            rows = kept

        threshold = self._threshold(timeframe)
        if len(rows) < threshold:
            logger.warning(
                "Insufficient training data for %s %s %dm: %d < %d",
                symbol, timeframe, horizon_minutes,
                len(rows), threshold,
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

        if len(X) < threshold:
            logger.warning(
                "Insufficient training data after NaN cleanup for %s %s %dm: %d < %d",
                symbol, timeframe, horizon_minutes,
                len(X), threshold,
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
