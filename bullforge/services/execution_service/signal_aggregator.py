from __future__ import annotations

from datetime import datetime
from typing import Optional, Set

from shared.models import TradingSignal


class SignalAggregator:
    def __init__(self, min_confidence: float = 0.6, min_agreement: int = 2):
        self._min_confidence = min_confidence
        self._min_agreement = min_agreement
        self._disabled_model_ids: Set[int] = set()

    def update_disabled_models(self, disabled_ids: Set[int]) -> None:
        self._disabled_model_ids = disabled_ids

    def aggregate(
        self,
        symbol: str,
        timestamp: datetime,
        predictions: list[dict],
    ) -> Optional[TradingSignal]:
        if not predictions:
            return None

        # Filter out predictions from disabled models
        active_preds = [
            p for p in predictions
            if p.get("model_id") not in self._disabled_model_ids
        ]

        confident = [
            p for p in active_preds if p["confidence"] >= self._min_confidence
        ]
        if len(confident) < self._min_agreement:
            return None

        weighted_sum = sum(
            p["direction"] * p["confidence"] for p in confident
        )
        total_weight = sum(p["confidence"] for p in confident)
        avg = weighted_sum / total_weight

        if avg > 0.3:
            direction = 1
        elif avg < -0.3:
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
