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
