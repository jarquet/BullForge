"""execution_service's view of the full-halt flag.

``OrderExecutor`` consults the gate before opening a position; exits are never
gated. The gate follows ``HALT_CHANNEL`` messages from monitoring_service and,
on startup, the unresolved tier-4+ rows in ``halt_events``, so a halt survives
an execution_service restart.
"""
from __future__ import annotations

import logging

from shared.halt import HALT_CHANNEL, HaltState, state_from_active_halts

logger = logging.getLogger(__name__)


class HaltGate:
    def __init__(self) -> None:
        self._state = HaltState(halted=False)
        # Bumped on every applied message, so a DB snapshot that was read
        # while a message arrived can tell it is stale.
        self._messages_seen = 0

    @property
    def halted(self) -> bool:
        return self._state.halted

    @property
    def state(self) -> HaltState:
        return self._state

    def apply(self, state: HaltState) -> None:
        if state.halted != self._state.halted:
            if state.halted:
                logger.critical(
                    "Trading HALTED (tier=%s): %s. New entries blocked; "
                    "exits still run.", state.tier, state.reason,
                )
            else:
                logger.warning("Trading halt lifted: %s", state.reason)
        self._state = state

    async def handle_message(self, message: str) -> None:
        try:
            state = HaltState.from_message(message)
        except ValueError:
            logger.warning(
                "Ignoring malformed %s message: %r", HALT_CHANNEL, message,
            )
            return
        self._messages_seen += 1
        self.apply(state)

    async def load_from_db(self, db) -> None:
        """Seed the state from active ``halt_events`` rows. A read failure
        fails closed: entries stay blocked until a message or a restart."""
        seen = self._messages_seen
        try:
            rows = await db.get_active_halts()
            state = state_from_active_halts(rows)
        except Exception:
            logger.exception("Could not read halt_events at startup")
            state = HaltState(
                halted=True,
                reason="Halt state unknown: halt_events read failed at startup",
            )
        if self._messages_seen != seen:
            logger.info(
                "%s message arrived during startup read; keeping it", HALT_CHANNEL,
            )
            return
        self.apply(state)

    async def start(self, db, redis_client) -> None:
        """Subscribe, then read the DB. In this order no message can slip
        between the snapshot and the subscription."""
        await redis_client.subscribe(HALT_CHANNEL, self.handle_message)
        await self.load_from_db(db)
