from __future__ import annotations

import logging
from typing import List, Tuple

from shared.config import Settings
from shared.db import Database

logger = logging.getLogger(__name__)


class FidelityTracker:
    def __init__(self, db: Database, settings: Settings):
        self._db = db
        self._warning_margin = settings.fidelity_warning_margin
        self._critical_margin = settings.fidelity_critical_margin
        self._min_trades = settings.fidelity_min_trades

    async def evaluate_model(
        self, model_id: int, backtested_accuracy: float,
    ) -> Tuple[str, str]:
        outcomes = await self._db.query_prediction_outcomes(
            model_id, limit=self._min_trades,
        )

        if len(outcomes) < self._min_trades:
            return (
                "active",
                f"Insufficient data: {len(outcomes)}/{self._min_trades} predictions",
            )

        correct = sum(1 for o in outcomes if o["is_correct"])
        live_accuracy = correct / len(outcomes)

        if live_accuracy < backtested_accuracy - self._critical_margin:
            return (
                "disabled",
                f"Live accuracy {live_accuracy:.2%} below backtested "
                f"{backtested_accuracy:.2%} by {backtested_accuracy - live_accuracy:.2%}",
            )

        if live_accuracy < backtested_accuracy - self._warning_margin:
            return (
                "warned",
                f"Live accuracy {live_accuracy:.2%} degrading from "
                f"backtested {backtested_accuracy:.2%}",
            )

        return ("active", "Within expected range")

    async def evaluate_all_models(
        self,
    ) -> List[Tuple[int, str, float]]:
        models = await self._db.query_all_active_models()
        results = []

        for model in models:
            backtested = model.metrics.get("accuracy", 0.5)
            status, reason = await self.evaluate_model(model.id, backtested)

            outcomes = await self._db.query_prediction_outcomes(
                model.id, limit=self._min_trades,
            )
            live_accuracy = (
                sum(1 for o in outcomes if o["is_correct"]) / len(outcomes)
                if outcomes
                else 0.0
            )

            results.append((model.id, status, live_accuracy))

            if status != "active":
                logger.warning(
                    "Model %d fidelity: %s — %s (live=%.2f%%, backtested=%.2f%%)",
                    model.id, status, reason,
                    live_accuracy * 100, backtested * 100,
                )

        return results
