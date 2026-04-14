import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from services.monitoring_service.telegram_bot import TelegramBot


def _make_settings():
    s = MagicMock()
    s.telegram_bot_token = "fake-token"
    s.telegram_chat_id = "12345"
    return s


def _make_callback_query(data):
    query = AsyncMock()
    query.data = data
    query.answer = AsyncMock()
    query.edit_message_reply_markup = AsyncMock()
    query.edit_message_text = AsyncMock()
    return query


def _make_update(callback_data):
    update = MagicMock()
    update.callback_query = _make_callback_query(callback_data)
    return update


def _make_message_update(text="/status"):
    update = MagicMock()
    update.message = AsyncMock()
    update.message.reply_text = AsyncMock()
    update.effective_chat = MagicMock()
    update.effective_chat.id = 12345
    return update


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.upsert_model_status = AsyncMock()
    db.get_all_model_statuses = AsyncMock(return_value=[])
    db.get_active_halts = AsyncMock(return_value=[])
    return db


@pytest.fixture
def mock_halt_manager():
    hm = AsyncMock()
    hm.is_halted = False
    hm.trigger_halt = AsyncMock()
    hm.resolve_halt = AsyncMock()
    hm.publish_status = AsyncMock()
    return hm


@pytest.fixture
def mock_redis():
    r = MagicMock()
    r.publish = AsyncMock()
    r.subscribe = AsyncMock()
    return r


@pytest.fixture
def bot(mock_db, mock_halt_manager, mock_redis):
    settings = _make_settings()
    b = TelegramBot(settings, mock_db, mock_halt_manager, mock_redis)
    b._bot = AsyncMock()
    b._bot.send_message = AsyncMock()
    return b


@pytest.mark.asyncio
async def test_send_alert(bot):
    alert = {"tier": 1, "type": "model_warned", "model_id": 5, "reason": "Test"}

    await bot.send_alert(alert)

    bot._bot.send_message.assert_called_once()
    call_kwargs = bot._bot.send_message.call_args[1]
    assert call_kwargs["chat_id"] == "12345"
    assert "Model 5" in call_kwargs["text"]
    assert call_kwargs["reply_markup"] is not None


@pytest.mark.asyncio
async def test_on_alert_from_redis(bot):
    alert = {"tier": 2, "type": "model_disabled", "model_id": 3, "reason": "Critical"}

    await bot._on_alert(json.dumps(alert))

    bot._bot.send_message.assert_called_once()


@pytest.mark.asyncio
async def test_callback_acknowledge(bot):
    update = _make_update("ack")

    await bot._on_callback(update, None)

    update.callback_query.answer.assert_called_once_with("Acknowledged")
    update.callback_query.edit_message_reply_markup.assert_called_once_with(
        reply_markup=None,
    )


@pytest.mark.asyncio
async def test_callback_disable_model(bot, mock_db):
    update = _make_update("disable:7")

    await bot._on_callback(update, None)

    mock_db.upsert_model_status.assert_called_once_with(
        7, "disabled", "Manually disabled via Telegram",
    )
    update.callback_query.answer.assert_called_once_with("Model 7 disabled")
    update.callback_query.edit_message_reply_markup.assert_called_once_with(
        reply_markup=None,
    )


@pytest.mark.asyncio
async def test_callback_reenable_model(bot, mock_db):
    update = _make_update("reenable:3")

    await bot._on_callback(update, None)

    mock_db.upsert_model_status.assert_called_once_with(
        3, "active", "Re-enabled via Telegram",
    )
    update.callback_query.answer.assert_called_once_with("Model 3 re-enabled")


@pytest.mark.asyncio
async def test_callback_halt_confirm_shows_confirmation(bot):
    update = _make_update("halt_confirm")

    await bot._on_callback(update, None)

    update.callback_query.edit_message_text.assert_called_once()
    call_kwargs = update.callback_query.edit_message_text.call_args[1]
    assert "sure" in call_kwargs["text"].lower() or "halt" in call_kwargs["text"].lower()
    assert call_kwargs["reply_markup"] is not None
    update.callback_query.answer.assert_called_once()


@pytest.mark.asyncio
async def test_callback_halt_yes(bot, mock_halt_manager):
    update = _make_update("halt_yes")

    await bot._on_callback(update, None)

    mock_halt_manager.trigger_halt.assert_called_once_with(
        "Manual full halt via Telegram", tier=5,
    )
    update.callback_query.edit_message_text.assert_called_once()
    assert "halted" in update.callback_query.edit_message_text.call_args[1]["text"].lower()


@pytest.mark.asyncio
async def test_callback_halt_no(bot):
    update = _make_update("halt_no")

    await bot._on_callback(update, None)

    update.callback_query.edit_message_text.assert_called_once()
    assert "cancelled" in update.callback_query.edit_message_text.call_args[1]["text"].lower()


@pytest.mark.asyncio
async def test_cmd_status(bot, mock_db, mock_halt_manager):
    mock_halt_manager.is_halted = False
    mock_db.get_all_model_statuses = AsyncMock(return_value=[
        MagicMock(model_id=1, status="active"),
    ])
    mock_db.get_active_halts = AsyncMock(return_value=[])

    update = _make_message_update("/status")
    await bot._cmd_status(update, None)

    update.message.reply_text.assert_called_once()
    text = update.message.reply_text.call_args[0][0]
    assert "ACTIVE" in text
    assert "Model 1" in text


@pytest.mark.asyncio
async def test_cmd_start(bot):
    update = _make_message_update("/start")
    await bot._cmd_start(update, None)

    update.message.reply_text.assert_called_once()
    text = update.message.reply_text.call_args[0][0]
    assert "BullForge" in text
