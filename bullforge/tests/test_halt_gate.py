"""execution_service's view of the full-halt flag (``trading:halt``)."""
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.execution_service.halt_gate import HaltGate
from shared.halt import HALT_CHANNEL, HaltState


def _row(id_, tier, reason="r"):
    return {
        "id": id_, "tier": tier, "reason": reason, "direction": None,
        "activated_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
    }


ON = HaltState(halted=True, tier=4, reason="Drawdown 12% exceeds threshold")
OFF = HaltState(halted=False, reason="Resumed")


def _db(rows=None, exc=None):
    db = MagicMock()
    db.get_active_halts = AsyncMock(return_value=rows or [], side_effect=exc)
    return db


def test_starts_not_halted():
    gate = HaltGate()
    assert gate.halted is False
    assert gate.state == HaltState(halted=False)


@pytest.mark.asyncio
async def test_on_then_off_messages():
    gate = HaltGate()

    await gate.handle_message(ON.to_message())
    assert gate.halted is True
    assert gate.state == ON

    await gate.handle_message(OFF.to_message())
    assert gate.halted is False


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [
    "garbage", "{}", '{"halted": "false"}', '{"_halt": "full"}', '{"1": "active"}',
])
async def test_malformed_message_leaves_state_unchanged(raw):
    gate = HaltGate()
    await gate.handle_message(ON.to_message())

    await gate.handle_message(raw)

    assert gate.state == ON


@pytest.mark.asyncio
@pytest.mark.parametrize("rows,expected", [
    ([], False),
    ([_row(1, 3)], False),
    ([_row(2, 4)], True),
    ([_row(3, 3), _row(2, 5)], True),
])
async def test_load_from_db(rows, expected):
    gate = HaltGate()
    await gate.load_from_db(_db(rows))
    assert gate.halted is expected


@pytest.mark.asyncio
async def test_load_from_db_keeps_tier_and_reason():
    gate = HaltGate()
    await gate.load_from_db(_db([_row(1, 5, "Manual halt via Telegram")]))
    assert gate.state == HaltState(
        halted=True, tier=5, reason="Manual halt via Telegram",
    )


@pytest.mark.asyncio
async def test_load_from_db_failure_fails_closed():
    gate = HaltGate()

    await gate.load_from_db(_db(exc=RuntimeError("db down")))

    assert gate.halted is True
    assert "unknown" in gate.state.reason.lower()


@pytest.mark.asyncio
async def test_message_during_load_wins_over_db_snapshot():
    # The DB read can start before /resume commits and finish after its
    # "off" message arrives. The message is newer, so it must win.
    gate = HaltGate()

    async def stale_read():
        await gate.handle_message(OFF.to_message())
        return [_row(1, 4)]

    db = MagicMock()
    db.get_active_halts = AsyncMock(side_effect=stale_read)

    await gate.load_from_db(db)

    assert gate.halted is False


@pytest.mark.asyncio
async def test_halt_message_during_load_wins_over_empty_snapshot():
    gate = HaltGate()

    async def stale_read():
        await gate.handle_message(ON.to_message())
        return []

    db = MagicMock()
    db.get_active_halts = AsyncMock(side_effect=stale_read)

    await gate.load_from_db(db)

    assert gate.state == ON


@pytest.mark.asyncio
async def test_start_subscribes_before_reading_db():
    # Subscribing first means no message is lost between the DB read and
    # the subscription.
    order = []
    redis_client = MagicMock()
    redis_client.subscribe = AsyncMock(
        side_effect=lambda ch, _h: order.append(("subscribe", ch)),
    )
    db = MagicMock()

    async def read():
        order.append(("read",))
        return [_row(1, 4)]

    db.get_active_halts = AsyncMock(side_effect=read)
    gate = HaltGate()

    await gate.start(db, redis_client)

    assert order == [("subscribe", HALT_CHANNEL), ("read",)]
    assert redis_client.subscribe.call_args.args[1] == gate.handle_message
    assert gate.halted is True
