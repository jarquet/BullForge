from __future__ import annotations

from datetime import datetime
from typing import Optional, Set

from shared.models import TradingSignal
from shared.runtime_config import RuntimeConfig


class SignalAggregator:
    def __init__(
        self,
        *,
        runtime_config: RuntimeConfig,
        overrides: Optional[dict] = None,
    ):
        self._rc = runtime_config
        self._overrides = overrides or {}
        self._disabled_model_ids: Set[int] = set()

    def update_disabled_models(self, disabled_ids: Set[int]) -> None:
        self._disabled_model_ids = disabled_ids

    async def _param(self, key: str):
        v = self._overrides.get(key)
        if v is not None:
            return v
        return await self._rc.get(f"execution_{key}")

    async def aggregate(
        self,
        symbol: str,
        timestamp: datetime,
        predictions: list[dict],
    ) -> Optional[TradingSignal]:
        if not predictions:
            return None

        active_preds = [
            p for p in predictions
            if p.get("model_id") not in self._disabled_model_ids
        ]

        min_confidence = await self._param("min_confidence")
        confident = [
            p for p in active_preds if p["confidence"] >= min_confidence
        ]
        min_agreement = await self._param("min_agreement")
        if len(confident) < min_agreement:
            return None

        weighted_sum = sum(
            p["direction"] * p["confidence"] for p in confident
        )
        total_weight = sum(p["confidence"] for p in confident)
        avg = weighted_sum / total_weight

        band = await self._param("direction_band")
        if avg > band:
            direction = 1
        elif avg < -band:
            direction = -1
        else:
            direction = 0

        strength = min(abs(avg), 1.0)

        return TradingSignal(
            symbol=symbol,
            timestamp=timestamp,
            direction=direction,
            strength=strength,
            contributing_predictions=len(confident),
        )
