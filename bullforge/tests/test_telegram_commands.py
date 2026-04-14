import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from services.monitoring_service.telegram_bot import (
    handle_status, handle_positions, handle_trades,
    handle_models, handle_halt, handle_resume,
)


@pytest.fixture
def context():
    return MagicMock()


@pytest.fixture
def update():
    u = MagicMock()
    u.effective_chat.id = 99
    u.message.reply_text = AsyncMock()
    u.message.reply_markdown = AsyncMock()
    return u


@pytest.fixture
def deps():
    db = AsyncMock()
    db.query_latest_metric_snapshot = AsyncMock(return_value={
        "metrics": {"trades_today": 5, "win_rate": 0.6, "pnl_today": 100.0},
    })
    db.get_all_model_statuses = AsyncMock(return_value=[])
    db.query_latest_bar_timestamps = AsyncMock(return_value=[])
    db.query_latest_feature_timestamps = AsyncMock(return_value=[])
    db.get_active_halts = AsyncMock(return_value=[])
    db.query_model_details = AsyncMock(return_value=[])
    db.query_closed_trades = AsyncMock(return_value=[])

    broker_client = AsyncMock()
    broker_client.get_account = AsyncMock(return_value={
        "equity": 100000.0, "cash": 20000.0, "buying_power": 200000.0,
    })
    broker_client.get_positions = AsyncMock(return_value=[])

    rc = AsyncMock()
    rc.get = AsyncMock(return_value="2026-04-13T01:00:00+00:00")

    halt_manager = MagicMock()
    halt_manager.trigger_halt = AsyncMock()
    halt_manager.resolve_halt = AsyncMock()

    return dict(db=db, broker_client=broker_client,
                runtime_config=rc, halt_manager=halt_manager)


@pytest.mark.asyncio
async def test_status_happy_path(update, context, deps):
    await handle_status(update, context, **deps)
    update.message.reply_text.assert_awaited_once()
    sent = update.message.reply_text.call_args[0][0]
    assert "BullForge Status" in sent


@pytest.mark.asyncio
async def test_positions_empty(update, context, deps):
    await handle_positions(update, context, **deps)
    sent = update.message.reply_text.call_args[0][0]
    assert "No open positions" in sent


@pytest.mark.asyncio
async def test_positions_broker_timeout(update, context, deps):
    deps["broker_client"].get_positions = AsyncMock(side_effect=TimeoutError())
    await handle_positions(update, context, **deps)
    sent = update.message.reply_text.call_args[0][0]
    assert "Execution service unavailable" in sent


@pytest.mark.asyncio
async def test_halt_triggers_manager(update, context, deps):
    await handle_halt(update, context, **deps)
    deps["halt_manager"].trigger_halt.assert_awaited()
    sent = update.message.reply_text.call_args[0][0]
    assert "halted" in sent.lower()


@pytest.mark.asyncio
async def test_resume_clears_halts(update, context, deps):
    await handle_resume(update, context, **deps)
    deps["halt_manager"].resolve_halt.assert_awaited()


@pytest.mark.asyncio
async def test_trades_empty(update, context, deps):
    await handle_trades(update, context, **deps)
    sent = update.message.reply_text.call_args[0][0]
    assert "No recent trades" in sent


@pytest.mark.asyncio
async def test_models_command(update, context, deps):
    deps["db"].query_model_details = AsyncMock(return_value=[
        {"model_id": 1, "symbol": "SPY", "model_type": "gradient_boost",
         "timeframe": "1m", "horizon_minutes": 5, "status": "active",
         "metrics": {"accuracy": 0.6},
         "trained_at": datetime.now(timezone.utc)},
    ])
    await handle_models(update, context, **deps)
    sent = update.message.reply_text.call_args[0][0]
    assert "SPY" in sent
