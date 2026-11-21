import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from shared.models import ModelStatus
from services.monitoring_service.halt_manager import HaltManager


def _status(model_id, status="active", reason="OK"):
    return ModelStatus(
        model_id=model_id,
        status=status,
        reason=reason,
        updated_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.insert_halt_event = AsyncMock(return_value=1)
    db.get_active_halts = AsyncMock(return_value=[])
    db.resolve_halt_event = AsyncMock()
    return db


@pytest.fixture
def mock_redis():
    redis_client = MagicMock()
    redis_client.publish = AsyncMock()
    return redis_client


@pytest.fixture
def manager(mock_db, mock_redis, settings):
    return HaltManager(mock_db, mock_redis, settings)


@pytest.mark.asyncio
async def test_no_alerts_when_all_active(manager, mock_db):
    statuses = [_status(1), _status(2), _status(3)]
    alerts = await manager.evaluate(statuses, current_drawdown=0.02)

    assert len(alerts) == 0
    mock_db.insert_halt_event.assert_not_called()


@pytest.mark.asyncio
async def test_tier1_warning(manager):
    statuses = [_status(1, "warned", "Degrading"), _status(2), _status(3)]
    alerts = await manager.evaluate(statuses, current_drawdown=0.02)

    assert len(alerts) == 1
    assert alerts[0]["tier"] == 1
    assert alerts[0]["model_id"] == 1


@pytest.mark.asyncio
async def test_tier2_disable(manager, mock_db):
    statuses = [_status(1, "disabled", "Critical"), _status(2), _status(3)]
    alerts = await manager.evaluate(statuses, current_drawdown=0.02)

    tier2_alerts = [a for a in alerts if a["tier"] == 2]
    assert len(tier2_alerts) == 1
    assert tier2_alerts[0]["model_id"] == 1


@pytest.mark.asyncio
async def test_tier3_halt_direction(manager, mock_db):
    # 2 of 4 models disabled — exactly half → tier 3 (not tier 4 which needs >half)
    statuses = [
        _status(1, "disabled", "Critical"),
        _status(2, "disabled", "Critical"),
        _status(3),
        _status(4),
    ]
    alerts = await manager.evaluate(statuses, current_drawdown=0.02)

    tier3_alerts = [a for a in alerts if a["tier"] == 3]
    assert len(tier3_alerts) == 1
    mock_db.insert_halt_event.assert_called()


@pytest.mark.asyncio
async def test_tier4_full_halt_drawdown(manager, mock_db):
    statuses = [_status(1), _status(2), _status(3)]
    alerts = await manager.evaluate(statuses, current_drawdown=0.15)

    tier4_alerts = [a for a in alerts if a["tier"] == 4]
    assert len(tier4_alerts) == 1
    assert "drawdown" in tier4_alerts[0]["reason"].lower()
    mock_db.insert_halt_event.assert_called()


@pytest.mark.asyncio
async def test_tier4_full_halt_majority_disabled(manager, mock_db):
    statuses = [
        _status(1, "disabled", "Critical"),
        _status(2, "disabled", "Critical"),
        _status(3, "disabled", "Critical"),
    ]
    alerts = await manager.evaluate(statuses, current_drawdown=0.02)

    tier4_alerts = [a for a in alerts if a["tier"] == 4]
    assert len(tier4_alerts) == 1
    assert "majority" in tier4_alerts[0]["reason"].lower()


@pytest.mark.asyncio
async def test_publish_status(manager, mock_redis):
    statuses = [_status(1, "active"), _status(2, "disabled", "Bad")]

    await manager.publish_status(statuses)

    mock_redis.publish.assert_called_once()
    call_args = mock_redis.publish.call_args
    assert call_args[0][0] == "models:status"


@pytest.mark.asyncio
async def test_is_halted(manager):
    assert manager.is_halted is False

    statuses = [_status(1)]
    await manager.evaluate(statuses, current_drawdown=0.15)

    assert manager.is_halted is True


@pytest.mark.asyncio
async def test_trigger_halt(manager, mock_db, mock_redis):
    assert manager.is_halted is False

    await manager.trigger_halt("Manual halt via Telegram")

    assert manager.is_halted is True
    mock_db.insert_halt_event.assert_called_once_with(
        4, "Manual halt via Telegram",
    )
    mock_redis.publish.assert_called()


@pytest.mark.asyncio
async def test_resolve_halt(manager, mock_db):
    # First halt, then resolve
    manager._full_halt = True
    mock_db.get_active_halts = AsyncMock(return_value=[
        {"id": 1, "tier": 4, "reason": "test", "direction": None,
         "activated_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc)},
    ])

    await manager.resolve_halt()

    assert manager.is_halted is False
    mock_db.resolve_halt_event.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_resolve_halt_multiple_events(manager, mock_db):
    manager._full_halt = True
    mock_db.get_active_halts = AsyncMock(return_value=[
        {"id": 1, "tier": 4, "reason": "test1", "direction": None,
         "activated_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc)},
        {"id": 2, "tier": 3, "reason": "test2", "direction": "long",
         "activated_at": datetime(2026, 4, 12, 13, 0, 0, tzinfo=timezone.utc)},
    ])

    await manager.resolve_halt()

    assert manager.is_halted is False
    assert mock_db.resolve_halt_event.call_count == 2


@pytest.mark.asyncio
async def test_tier1_no_realert_when_status_unchanged(manager):
    # Already-warned models must not re-fire on the next monitoring tick.
    statuses = [_status(1, "warned", "Degrading"), _status(2)]
    prev = {1: "warned", 2: "active"}

    alerts = await manager.evaluate(
        statuses, current_drawdown=0.02, prev_status_map=prev,
    )

    assert [a for a in alerts if a["tier"] == 1] == []


@pytest.mark.asyncio
async def test_tier2_no_realert_when_status_unchanged(manager, mock_db):
    # Already-disabled models must not re-fire every tick.
    statuses = [_status(1, "disabled", "Critical"), _status(2), _status(3)]
    prev = {1: "disabled", 2: "active", 3: "active"}

    alerts = await manager.evaluate(
        statuses, current_drawdown=0.02, prev_status_map=prev,
    )

    assert [a for a in alerts if a["tier"] == 2] == []


@pytest.mark.asyncio
async def test_tier1_refires_on_active_to_warned_transition(manager):
    statuses = [_status(1, "warned", "Degrading")]
    prev = {1: "active"}

    alerts = await manager.evaluate(
        statuses, current_drawdown=0.02, prev_status_map=prev,
    )

    tier1 = [a for a in alerts if a["tier"] == 1]
    assert len(tier1) == 1
    assert tier1[0]["model_id"] == 1


@pytest.mark.asyncio
async def test_tier3_no_realert_when_already_over_threshold(manager, mock_db):
    # Both disabled models were already disabled last tick; no new alert.
    statuses = [
        _status(1, "disabled", "Critical"),
        _status(2, "disabled", "Critical"),
        _status(3),
        _status(4),
    ]
    prev = {1: "disabled", 2: "disabled", 3: "active", 4: "active"}

    alerts = await manager.evaluate(
        statuses, current_drawdown=0.02, prev_status_map=prev,
    )

    assert [a for a in alerts if a["tier"] == 3] == []
    mock_db.insert_halt_event.assert_not_called()


@pytest.mark.asyncio
async def test_tier4_drawdown_no_realert_when_already_halted(manager, mock_db):
    # Set the halted flag to simulate the state persisting from a prior tick.
    manager._full_halt = True
    statuses = [_status(1), _status(2), _status(3)]

    alerts = await manager.evaluate(statuses, current_drawdown=0.15)

    assert [a for a in alerts if a["tier"] == 4] == []
    mock_db.insert_halt_event.assert_not_called()


# ── Full-halt control message on trading:halt ─────────────────────────

from shared.halt import HALT_CHANNEL, HaltState  # noqa: E402


def _halt_messages(mock_redis):
    return [
        HaltState.from_message(c.args[1])
        for c in mock_redis.publish.call_args_list
        if c.args[0] == HALT_CHANNEL
    ]


def _channels(mock_redis):
    return [c.args[0] for c in mock_redis.publish.call_args_list]


@pytest.mark.asyncio
async def test_tier4_drawdown_publishes_halt_on(manager, mock_redis):
    await manager.evaluate([_status(1), _status(2)], current_drawdown=0.15)

    msgs = _halt_messages(mock_redis)
    assert len(msgs) == 1
    assert msgs[0].halted is True
    assert msgs[0].tier == 4
    assert "drawdown" in msgs[0].reason.lower()


@pytest.mark.asyncio
async def test_tier4_majority_publishes_halt_on(manager, mock_redis):
    statuses = [_status(i, "disabled", "Critical") for i in (1, 2, 3)]
    await manager.evaluate(statuses, current_drawdown=0.02)

    msgs = _halt_messages(mock_redis)
    assert [(m.halted, m.tier) for m in msgs] == [(True, 4)]
    assert "majority" in msgs[0].reason.lower()


@pytest.mark.asyncio
async def test_tier3_does_not_halt_or_publish(manager, mock_redis):
    # Tier 3 stays an alert tier: disabled models are already out of the
    # ensemble (tier 2), and blocking here would make tier 4's majority
    # trigger redundant.
    statuses = [
        _status(1, "disabled", "Critical"), _status(2, "disabled", "Critical"),
        _status(3), _status(4),
    ]
    alerts = await manager.evaluate(statuses, current_drawdown=0.02)

    assert [a["tier"] for a in alerts if a["tier"] >= 3] == [3]
    assert manager.is_halted is False
    assert _halt_messages(mock_redis) == []


@pytest.mark.asyncio
async def test_no_halt_message_when_healthy(manager, mock_redis):
    await manager.evaluate([_status(1), _status(2)], current_drawdown=0.01)
    assert _halt_messages(mock_redis) == []


@pytest.mark.asyncio
async def test_halt_reasserted_every_tick_while_halted(manager, mock_db, mock_redis):
    # Pub/sub is at-most-once, so the halt is re-published each tick for a
    # subscriber that missed the transition. The audit row is written once.
    await manager.evaluate([_status(1)], current_drawdown=0.15)
    await manager.evaluate([_status(1)], current_drawdown=0.15)

    assert mock_db.insert_halt_event.call_count == 1
    msgs = _halt_messages(mock_redis)
    assert [(m.halted, m.tier) for m in msgs] == [(True, 4), (True, 4)]
    assert msgs[0].reason == msgs[1].reason


@pytest.mark.asyncio
async def test_halt_stays_latched_after_condition_clears(manager, mock_redis):
    await manager.evaluate([_status(1)], current_drawdown=0.15)
    await manager.evaluate([_status(1)], current_drawdown=0.01)

    assert manager.is_halted is True
    assert [m.halted for m in _halt_messages(mock_redis)] == [True, True]


@pytest.mark.asyncio
async def test_halt_reasserted_with_no_model_statuses(manager, mock_redis):
    await manager.trigger_halt("Manual halt via Telegram", tier=5)
    mock_redis.publish.reset_mock()

    await manager.evaluate([], current_drawdown=0.0)

    assert _halt_messages(mock_redis) == [
        HaltState(halted=True, tier=5, reason="Manual halt via Telegram"),
    ]


@pytest.mark.asyncio
async def test_trigger_halt_publishes_on_trading_halt_only(manager, mock_redis):
    await manager.trigger_halt("Manual halt via Telegram", tier=5)

    assert _halt_messages(mock_redis) == [
        HaltState(halted=True, tier=5, reason="Manual halt via Telegram"),
    ]
    # models:status carries per-model snapshots only; no {"_halt": ...}.
    assert "models:status" not in _channels(mock_redis)


@pytest.mark.asyncio
async def test_resolve_halt_publishes_halt_off(manager, mock_db, mock_redis):
    await manager.trigger_halt("Manual halt via Telegram", tier=5)
    mock_db.get_active_halts = AsyncMock(return_value=[
        {"id": 1, "tier": 5, "reason": "Manual halt via Telegram",
         "direction": None,
         "activated_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc)},
    ])

    await manager.resolve_halt()

    msgs = _halt_messages(mock_redis)
    assert msgs[-1].halted is False
    assert msgs[-1].tier is None
    mock_db.resolve_halt_event.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_resolve_halt_publishes_off_even_without_active_events(
    manager, mock_redis,
):
    # /resume doubles as a way to clear a subscriber stuck halted.
    await manager.resolve_halt()
    assert [m.halted for m in _halt_messages(mock_redis)] == [False]


@pytest.mark.asyncio
async def test_resolve_halt_publishes_off_after_resolving_rows(
    manager, mock_db, mock_redis,
):
    # A subscriber that restarts right after "off" must find no active rows.
    order = []
    mock_db.get_active_halts = AsyncMock(return_value=[
        {"id": 7, "tier": 4, "reason": "x", "direction": None,
         "activated_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc)},
    ])
    mock_db.resolve_halt_event = AsyncMock(
        side_effect=lambda _id: order.append("resolve"),
    )
    mock_redis.publish = AsyncMock(
        side_effect=lambda ch, _m: order.append(ch),
    )

    await manager.resolve_halt()

    assert order == ["resolve", HALT_CHANNEL]


@pytest.mark.asyncio
async def test_no_reassert_after_resolve(manager, mock_redis):
    await manager.trigger_halt("Manual halt via Telegram", tier=5)
    await manager.resolve_halt()
    mock_redis.publish.reset_mock()

    await manager.evaluate([_status(1), _status(2)], current_drawdown=0.01)

    assert _halt_messages(mock_redis) == []


@pytest.mark.asyncio
async def test_restore_state_from_active_full_halt(manager, mock_db, mock_redis):
    mock_db.get_active_halts = AsyncMock(return_value=[
        {"id": 2, "tier": 3, "reason": "half", "direction": None,
         "activated_at": datetime(2026, 4, 12, 15, 0, 0, tzinfo=timezone.utc)},
        {"id": 1, "tier": 5, "reason": "Manual halt via Telegram",
         "direction": None,
         "activated_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc)},
    ])

    await manager.restore_state()
    assert manager.is_halted is True

    # The next tick re-asserts the restored halt and writes no new row.
    await manager.evaluate([_status(1), _status(2)], current_drawdown=0.15)
    mock_db.insert_halt_event.assert_not_called()
    assert _halt_messages(mock_redis) == [
        HaltState(halted=True, tier=5, reason="Manual halt via Telegram"),
    ]


@pytest.mark.asyncio
async def test_restore_state_ignores_tier3_rows(manager, mock_db):
    mock_db.get_active_halts = AsyncMock(return_value=[
        {"id": 1, "tier": 3, "reason": "half", "direction": None,
         "activated_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc)},
    ])

    await manager.restore_state()

    assert manager.is_halted is False


@pytest.mark.asyncio
async def test_restore_state_db_error_is_logged_not_raised(manager, mock_db, mock_redis):
    # Monitoring never publishes "off" on its own, so an unknown state here
    # cannot un-halt execution; it just skips the re-assert.
    mock_db.get_active_halts = AsyncMock(side_effect=RuntimeError("db down"))

    await manager.restore_state()

    assert manager.is_halted is False
    assert _halt_messages(mock_redis) == []


@pytest.mark.asyncio
async def test_resume_during_tick_is_serialized(manager, mock_db, mock_redis):
    # /resume arrives while a tick is mid-way through recording a new halt.
    # It must run after the tick, so its "off" is the last word and it
    # resolves the row the tick just wrote.
    import asyncio

    tasks = []

    async def insert_and_yield(tier, reason):
        tasks.append(asyncio.create_task(manager.resolve_halt()))
        await asyncio.sleep(0)
        mock_db.get_active_halts.return_value = [
            {"id": 9, "tier": tier, "reason": reason, "direction": None,
             "activated_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc)},
        ]
        return 9

    mock_db.insert_halt_event = AsyncMock(side_effect=insert_and_yield)

    await manager.evaluate([_status(1)], current_drawdown=0.15)
    await asyncio.gather(*tasks)

    assert [m.halted for m in _halt_messages(mock_redis)] == [True, False]
    assert manager.is_halted is False
    mock_db.resolve_halt_event.assert_awaited_once_with(9)
