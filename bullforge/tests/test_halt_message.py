"""The full-halt control message on ``trading:halt``, shared by
monitoring_service (publisher) and execution_service (subscriber)."""
import json
from datetime import datetime, timezone

import pytest

from shared.halt import (
    FULL_HALT_MIN_TIER,
    HALT_CHANNEL,
    HaltState,
    is_full_halt_tier,
    state_from_active_halts,
)


def _row(id_, tier, reason="r", hour=14):
    return {
        "id": id_, "tier": tier, "reason": reason, "direction": None,
        "activated_at": datetime(2026, 4, 12, hour, 0, 0, tzinfo=timezone.utc),
    }


def test_channel_is_dedicated():
    # Not models:status: that channel carries per-model snapshots only.
    assert HALT_CHANNEL == "trading:halt"


@pytest.mark.parametrize("tier,expected", [(1, False), (2, False), (3, False),
                                           (4, True), (5, True)])
def test_full_halt_tiers(tier, expected):
    assert FULL_HALT_MIN_TIER == 4
    assert is_full_halt_tier(tier) is expected


def test_halt_on_round_trip():
    state = HaltState(halted=True, tier=5, reason="Manual halt via Telegram")
    msg = state.to_message(now=datetime(2026, 4, 12, 14, 0, tzinfo=timezone.utc))

    payload = json.loads(msg)
    assert payload == {
        "halted": True, "tier": 5, "reason": "Manual halt via Telegram",
        "ts": "2026-04-12T14:00:00+00:00",
    }
    assert HaltState.from_message(msg) == state


def test_halt_off_round_trip():
    msg = HaltState(halted=False, reason="Resumed").to_message()

    payload = json.loads(msg)
    assert payload["halted"] is False
    assert payload["tier"] is None
    assert HaltState.from_message(msg) == HaltState(halted=False, reason="Resumed")


def test_from_message_tolerates_missing_optional_fields():
    assert HaltState.from_message('{"halted": true}') == HaltState(halted=True)


@pytest.mark.parametrize("raw", [
    "not json",
    "[]",
    '"halted"',
    "{}",
    '{"halted": "yes"}',
    '{"halted": 1}',
    '{"halted": true, "tier": "4"}',
    '{"halted": true, "tier": true}',
    '{"_halt": "full"}',
    '{"1": "disabled"}',
])
def test_from_message_rejects_malformed(raw):
    with pytest.raises(ValueError):
        HaltState.from_message(raw)


def test_state_from_no_active_halts():
    assert state_from_active_halts([]) == HaltState(halted=False)


def test_state_from_tier3_only_is_not_halted():
    # Tier 3 is an alert tier; it never blocks entries.
    assert state_from_active_halts([_row(1, 3)]).halted is False


def test_state_from_active_full_halt_uses_newest_row():
    # get_active_halts returns rows newest first.
    rows = [
        _row(3, 3, "half disabled", hour=16),
        _row(2, 5, "Manual halt via Telegram", hour=15),
        _row(1, 4, "Drawdown 12% exceeds threshold", hour=14),
    ]
    assert state_from_active_halts(rows) == HaltState(
        halted=True, tier=5, reason="Manual halt via Telegram",
    )
