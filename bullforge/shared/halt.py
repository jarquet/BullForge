"""Full-halt control message shared by monitoring_service and execution_service.

``HaltManager`` (monitoring) publishes a small on/off message on
``HALT_CHANNEL``; ``HaltGate`` (execution) applies it and blocks new entries
while halted. ``halt_events`` stays the durable record: a full halt is any
unresolved row with ``tier >= FULL_HALT_MIN_TIER`` (4 = automated circuit
breaker, 5 = manual ``/halt``). Tier 3 is an alert tier and never blocks.

Wire format (JSON)::

    {"halted": true,  "tier": 4,    "reason": "...", "ts": "<ISO-8601 UTC>"}
    {"halted": false, "tier": null, "reason": "...", "ts": "<ISO-8601 UTC>"}

``halted`` is required and must be a JSON boolean. ``ts`` is informational.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

HALT_CHANNEL = "trading:halt"
FULL_HALT_MIN_TIER = 4


def is_full_halt_tier(tier: int) -> bool:
    return tier >= FULL_HALT_MIN_TIER


@dataclass(frozen=True)
class HaltState:
    halted: bool
    tier: Optional[int] = None
    reason: str = ""

    def to_message(self, *, now: Optional[datetime] = None) -> str:
        ts = (now or datetime.now(timezone.utc)).isoformat()
        return json.dumps({
            "halted": self.halted,
            "tier": self.tier,
            "reason": self.reason,
            "ts": ts,
        })

    @classmethod
    def from_message(cls, message: str) -> "HaltState":
        """Parse a ``HALT_CHANNEL`` message. Raises ``ValueError`` when the
        message is not a halt message, so callers never guess a state."""
        data = json.loads(message)  # JSONDecodeError is a ValueError
        if not isinstance(data, dict):
            raise ValueError(f"halt message must be an object: {message!r}")
        halted = data.get("halted")
        if not isinstance(halted, bool):
            raise ValueError(f"halt message needs a boolean 'halted': {message!r}")
        tier = data.get("tier")
        if tier is not None and (isinstance(tier, bool) or not isinstance(tier, int)):
            raise ValueError(f"halt message 'tier' must be an int or null: {message!r}")
        reason = data.get("reason") or ""
        return cls(halted=halted, tier=tier, reason=str(reason))


def state_from_active_halts(rows: Iterable[dict]) -> HaltState:
    """Derive the halt state from ``Database.get_active_halts()`` rows, which
    come newest first. The newest full-halt row supplies tier and reason."""
    for row in rows:
        if is_full_halt_tier(row["tier"]):
            return HaltState(halted=True, tier=row["tier"], reason=row["reason"])
    return HaltState(halted=False)
