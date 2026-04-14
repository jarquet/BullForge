from __future__ import annotations

import json
import logging
from datetime import timezone
from typing import List, Optional

import numpy as np
import pandas as pd

from shared.config import Settings
from shared.db import Database
from shared.models import Bar, FeatureRow, Label
from shared.redis import RedisClient
from services.feature_engine.registry import FeatureRegistry
from services.feature_engine.labels import compute_forward_return_labels

logger = logging.getLogger(__name__)


class FeatureEngine:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        registry: FeatureRegistry,
        label_horizons: List[int],
        label_threshold: float = 0.0,
        min_bars: int = 50,
        redis_client: Optional[RedisClient] = None,
    ):
        self._settings = settings
        self._db = db
        self._registry = registry
        self._label_horizons = label_horizons
        self._label_threshold = label_threshold
        self._min_bars = min_bars
        self._redis_client = redis_client

    def _bars_to_dataframe(self, bars: List[Bar]) -> pd.DataFrame:
        if not bars:
            return pd.DataFrame()
        # Bars come from DB ordered DESC, reverse for chronological order
        bars_sorted = sorted(bars, key=lambda b: b.timestamp)
        return pd.DataFrame({
            "symbol": [b.symbol for b in bars_sorted],
            "timestamp": [b.timestamp for b in bars_sorted],
            "open": [b.open for b in bars_sorted],
            "high": [b.high for b in bars_sorted],
            "low": [b.low for b in bars_sorted],
            "close": [b.close for b in bars_sorted],
            "volume": [b.volume for b in bars_sorted],
        })

    async def compute_features_for_symbol(
        self, symbol: str, timeframe: str, limit: int = 500
    ) -> None:
        bars = await self._db.query_bars(symbol, timeframe, limit=limit)
        if len(bars) < self._min_bars:
            logger.warning(
                "Insufficient bars for %s %s: %d < %d",
                symbol, timeframe, len(bars), self._min_bars,
            )
            return

        df = self._bars_to_dataframe(bars)
        features_df = self._registry.compute_all(df)
        if features_df.empty:
            logger.warning("No features computed for %s %s", symbol, timeframe)
            return

        # Drop rows where any feature is NaN (warmup period)
        valid_mask = features_df.notna().all(axis=1)
        valid_df = features_df[valid_mask]
        valid_timestamps = df.loc[valid_mask, "timestamp"]

        feature_rows = []
        for idx in valid_df.index:
            row_features = valid_df.loc[idx].dropna().to_dict()
            if row_features:
                feature_rows.append(
                    FeatureRow(
                        symbol=symbol,
                        timestamp=valid_timestamps.loc[idx],
                        timeframe=timeframe,
                        features=row_features,
                    )
                )

        await self._db.insert_feature_rows(feature_rows)
        logger.info(
            "Computed %d feature rows for %s %s",
            len(feature_rows), symbol, timeframe,
        )

        if self._redis_client:
            await self._redis_client.publish(
                "feature:updates",
                json.dumps({"symbol": symbol, "timeframe": timeframe}),
            )

    def compute_for_bar(
        self, bars: List[Bar], target_ts, timeframe: str
    ) -> dict:
        """Compute features at `target_ts` using ONLY bars with timestamp < target_ts.

        Returns a {name: value} dict of the last feature row (the one at target_ts's
        predecessor). NaN and None values are dropped.
        """
        historical = [b for b in bars if b.timestamp < target_ts]
        df = self._bars_to_dataframe(historical)
        if df.empty:
            return {}
        features_df = self._registry.compute_all(df)
        if features_df.empty:
            return {}
        last = features_df.iloc[-1].dropna().to_dict()
        return {k: float(v) for k, v in last.items()}

    async def compute_labels_for_symbol(
        self, symbol: str, timeframe: str, limit: int = 500
    ) -> None:
        bars = await self._db.query_bars(symbol, timeframe, limit=limit)
        if len(bars) < self._min_bars:
            logger.warning(
                "Insufficient bars for labels %s %s: %d < %d",
                symbol, timeframe, len(bars), self._min_bars,
            )
            return

        df = self._bars_to_dataframe(bars)
        labels_df = compute_forward_return_labels(
            df, horizons=self._label_horizons, threshold=self._label_threshold
        )

        labels = []
        for h in self._label_horizons:
            ret_col = f"fwd_return_{h}"
            dir_col = f"fwd_direction_{h}"
            valid = labels_df.dropna(subset=[ret_col, dir_col])
            for idx in valid.index:
                labels.append(
                    Label(
                        symbol=symbol,
                        timestamp=df.loc[idx, "timestamp"],
                        timeframe=timeframe,
                        horizon_minutes=h,
                        forward_return=float(valid.loc[idx, ret_col]),
                        direction=int(valid.loc[idx, dir_col]),
                    )
                )

        await self._db.insert_labels(labels)
        logger.info(
            "Computed %d labels for %s %s",
            len(labels), symbol, timeframe,
        )

    async def compute_all(self) -> None:
        all_symbols = (
            self._settings.trading_symbols_equity
            + self._settings.trading_symbols_crypto
        )

        # Use a large limit for initial bulk computation to process all
        # available historical bars, not just the last 500.
        bulk_limit = 300_000

        for symbol in all_symbols:
            for timeframe in ["1m", "1d"]:
                await self.compute_features_for_symbol(
                    symbol, timeframe, limit=bulk_limit,
                )
                await self.compute_labels_for_symbol(
                    symbol, timeframe, limit=bulk_limit,
                )

        logger.info("Feature engine compute_all complete")
