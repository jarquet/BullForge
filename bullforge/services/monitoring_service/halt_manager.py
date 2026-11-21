from __future__ import annotations

import asyncio
import json
import logging
from typing import List, Optional

from shared.config import Settings
from shared.db import Database
from shared.halt import HALT_CHANNEL, HaltState, state_from_active_halts
from shared.models import ModelStatus
from shared.redis import RedisClient

logger = logging.getLogger(__name__)


class HaltManager:
    """Evaluates the tiered halt policy and owns the full-halt flag.

    Tiers 4 (automated) and 5 (manual ``/halt``) are full halts: they latch
    until ``resolve_halt()`` and are published on ``HALT_CHANNEL`` so
    execution_service blocks new entries. Tiers 1-3 only alert.
    """

    def __init__(
        self, db: Database, redis_client: RedisClient, settings: Settings,
    ):
        self._db = db
        self._redis = redis_client
        self._drawdown_threshold = settings.drawdown_halt_threshold
        self._full_halt = False
        self._halt_tier: Optional[int] = None
        self._halt_reason = ""
        # The monitoring loop and Telegram handlers share this object across
        # await points; the lock keeps a /resume from landing mid-tick.
        self._lock = asyncio.Lock()

    @property
    def is_halted(self) -> bool:
        return self._full_halt

    def _set_halted(self, tier: int, reason: str) -> None:
        self._full_halt = True
        self._halt_tier = tier
        self._halt_reason = reason

    async def _publish(self, state: HaltState) -> None:
        await self._redis.publish(HALT_CHANNEL, state.to_message())

    async def restore_state(self) -> None:
        """Pick up an unresolved tier-4+ halt after a monitoring restart."""
        try:
            rows = await self._db.get_active_halts()
        except Exception:
            # Monitoring never publishes "off" on its own, so not knowing
            # here cannot un-halt execution; it only skips the re-assert.
            logger.exception("Could not read halt_events; assuming no active halt")
            return
        state = state_from_active_halts(rows)
        if state.halted:
            self._set_halted(state.tier, state.reason)
            logger.warning(
                "Restored active halt tier=%s: %s", state.tier, state.reason,
            )

    async def evaluate(
        self,
        model_statuses: List[ModelStatus],
        current_drawdown: float,
        prev_status_map: dict[int, str] | None = None,
    ) -> List[dict]:
        async with self._lock:
            try:
                return await self._evaluate_tiers(
                    model_statuses, current_drawdown, prev_status_map,
                )
            finally:
                if self._full_halt:
                    # Re-assert every tick: pub/sub is at-most-once, so a
                    # subscriber that missed the transition converges within
                    # one interval. Only resolve_halt() ever publishes "off".
                    await self._publish(HaltState(
                        True, self._halt_tier, self._halt_reason,
                    ))

    async def _evaluate_tiers(
        self,
        model_statuses: List[ModelStatus],
        current_drawdown: float,
        prev_status_map: dict[int, str] | None,
    ) -> List[dict]:
        # Alerts are edge-triggered against prev_status_map so acknowledged
        # conditions do not re-notify every monitoring tick.
        alerts: List[dict] = []

        if not model_statuses:
            return alerts

        prev = prev_status_map or {}
        was_halted = self._full_halt

        # Tier 4: Full halt on drawdown
        if current_drawdown >= self._drawdown_threshold:
            if not was_halted:
                reason = (
                    f"Drawdown {current_drawdown:.2%} exceeds threshold "
                    f"{self._drawdown_threshold:.2%}"
                )
                self._set_halted(4, reason)
                await self._db.insert_halt_event(4, reason)
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
            if not was_halted:
                reason = f"Majority of models disabled ({len(disabled)}/{total})"
                self._set_halted(4, reason)
                await self._db.insert_halt_event(4, reason)
                alerts.append({
                    "tier": 4,
                    "type": "majority_halt",
                    "reason": reason,
                })
                logger.critical("TIER 4 HALT: %s", reason)
            return alerts

        # Tier 3: Half or more disabled — only fire when the threshold is
        # newly crossed this tick (prev disabled count was below threshold).
        # Alert only: disabled models are already out of the ensemble
        # (tier 2), and tier 4 stops entries one more model later.
        if total >= 2 and len(disabled) >= total / 2:
            prev_disabled_count = sum(
                1 for m in model_statuses if prev.get(m.model_id) == "disabled"
            )
            if prev_disabled_count < total / 2:
                reason = f"Half or more models disabled ({len(disabled)}/{total})"
                await self._db.insert_halt_event(3, reason)
                alerts.append({
                    "tier": 3,
                    "type": "direction_halt",
                    "reason": reason,
                })
                logger.error("TIER 3 HALT: %s", reason)

        # Tier 2: Only models that just transitioned into "disabled".
        for m in disabled:
            if prev.get(m.model_id) != "disabled":
                alerts.append({
                    "tier": 2,
                    "type": "model_disabled",
                    "model_id": m.model_id,
                    "reason": m.reason,
                })
                logger.warning(
                    "TIER 2: Model %d disabled — %s", m.model_id, m.reason,
                )

        # Tier 1: Only models that just transitioned into "warned".
        for m in warned:
            if prev.get(m.model_id) != "warned":
                alerts.append({
                    "tier": 1,
                    "type": "model_warned",
                    "model_id": m.model_id,
                    "reason": m.reason,
                })
                logger.warning(
                    "TIER 1: Model %d warned — %s", m.model_id, m.reason,
                )

        return alerts

    async def trigger_halt(self, reason: str, tier: int = 4) -> None:
        """Trigger a full halt. tier=4 for automated circuit breakers,
        tier=5 for manual halts (distinguishable in halt_events audit)."""
        async with self._lock:
            self._set_halted(tier, reason)
            logger.critical("HALT tier=%d: %s", tier, reason)
            try:
                await self._db.insert_halt_event(tier, reason)
            finally:
                # The brake engages even if the audit write fails.
                await self._publish(HaltState(True, tier, reason))

    async def resolve_halt(self, reason: str = "Resumed by operator") -> None:
        """Resolve every active halt event, then publish "off". It always
        publishes, so a repeated /resume also clears a subscriber that
        missed the first one."""
        async with self._lock:
            halts = await self._db.get_active_halts()
            for h in halts:
                await self._db.resolve_halt_event(h["id"])
            self._full_halt = False
            self._halt_tier = None
            self._halt_reason = ""
            logger.info("Halt resolved — %d events cleared", len(halts))
            await self._publish(HaltState(False, None, reason))

    async def publish_status(self, statuses: List[ModelStatus]) -> None:
        payload = {
            str(m.model_id): m.status
            for m in statuses
        }
        await self._redis.publish("models:status", json.dumps(payload))
