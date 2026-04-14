from __future__ import annotations

import json
import logging
from typing import List

from shared.config import Settings
from shared.db import Database
from shared.models import ModelStatus
from shared.redis import RedisClient

logger = logging.getLogger(__name__)


class HaltManager:
    def __init__(
        self, db: Database, redis_client: RedisClient, settings: Settings,
    ):
        self._db = db
        self._redis = redis_client
        self._drawdown_threshold = settings.drawdown_halt_threshold
        self._full_halt = False

    @property
    def is_halted(self) -> bool:
        return self._full_halt

    async def evaluate(
        self,
        model_statuses: List[ModelStatus],
        current_drawdown: float,
    ) -> List[dict]:
        alerts: List[dict] = []

        if not model_statuses:
            return alerts

        # Tier 4: Full halt on drawdown
        if current_drawdown >= self._drawdown_threshold:
            self._full_halt = True
            await self._db.insert_halt_event(
                4, f"Drawdown {current_drawdown:.2%} exceeds threshold {self._drawdown_threshold:.2%}",
            )
            alerts.append({
                "tier": 4,
                "type": "drawdown_halt",
                "reason": f"Drawdown {current_drawdown:.2%} exceeds threshold",
            })
            logger.critical("TIER 4 HALT: %s", alerts[-1]["reason"])
            return alerts

        disabled = [m for m in model_statuses if m.status == "disabled"]
        warned = [m for m in model_statuses if m.status == "warned"]
        total = len(model_statuses)

        # Tier 4: Majority of ALL models disabled
        if len(disabled) > total / 2:
            self._full_halt = True
            reason = f"Majority of models disabled ({len(disabled)}/{total})"
            await self._db.insert_halt_event(4, reason)
            alerts.append({
                "tier": 4,
                "type": "majority_halt",
                "reason": reason,
            })
            logger.critical("TIER 4 HALT: %s", reason)
            return alerts

        # Tier 3: Half or more models disabled (approaching majority)
        if total >= 2 and len(disabled) >= total / 2:
            reason = f"Half or more models disabled ({len(disabled)}/{total})"
            await self._db.insert_halt_event(3, reason)
            alerts.append({
                "tier": 3,
                "type": "direction_halt",
                "reason": reason,
            })
            logger.error("TIER 3 HALT: %s", reason)

        # Tier 2: Individual model disabled
        for m in disabled:
            alerts.append({
                "tier": 2,
                "type": "model_disabled",
                "model_id": m.model_id,
                "reason": m.reason,
            })
            logger.warning("TIER 2: Model %d disabled — %s", m.model_id, m.reason)

        # Tier 1: Individual model warned
        for m in warned:
            alerts.append({
                "tier": 1,
                "type": "model_warned",
                "model_id": m.model_id,
                "reason": m.reason,
            })
            logger.warning("TIER 1: Model %d warned — %s", m.model_id, m.reason)

        return alerts

    async def trigger_halt(self, reason: str, tier: int = 4) -> None:
        """Trigger a full halt. tier=4 for automated circuit breakers,
        tier=5 for manual halts (distinguishable in halt_events audit)."""
        self._full_halt = True
        await self._db.insert_halt_event(tier, reason)
        logger.critical("HALT tier=%d: %s", tier, reason)
        await self._redis.publish("models:status", json.dumps({"_halt": "full"}))

    async def resolve_halt(self) -> None:
        self._full_halt = False
        halts = await self._db.get_active_halts()
        for h in halts:
            await self._db.resolve_halt_event(h["id"])
        logger.info("Halt resolved — %d events cleared", len(halts))

    async def publish_status(self, statuses: List[ModelStatus]) -> None:
        payload = {
            str(m.model_id): m.status
            for m in statuses
        }
        await self._redis.publish("models:status", json.dumps(payload))
