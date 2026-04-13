# Phase 5B: Telegram Bot — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Telegram bot that receives monitoring alerts via Redis, sends formatted messages with inline action buttons, and lets the user disable/re-enable models or trigger a full halt from their phone.

**Architecture:** The bot runs inside the monitoring service process alongside the monitoring loop. It subscribes to the `monitoring:alerts` Redis channel (published by Phase 5A's `HaltManager`). Each alert is formatted into a Telegram message with inline keyboard buttons matching the spec's alert-type table. Button presses trigger callback handlers that call `Database` and `HaltManager` methods for state changes. Destructive actions (Full Halt) require a confirmation step. The bot uses `python-telegram-bot` v20+ in polling mode — no inbound network exposure required.

**Tech Stack:** python-telegram-bot v20+, asyncio, Redis pub/sub (from shared/redis.py), pytest, pytest-asyncio

**Depends on:** Phase 5A (monitoring service) must be implemented first. This plan modifies the monitoring service to add Telegram integration.

---

## File Structure

```
bullforge/
  shared/
    config.py                              — (modify) Add telegram_bot_token, telegram_chat_id

  services/
    monitoring_service/
      telegram_alerts.py                   — Pure functions: format alert dicts into text + button layouts
      telegram_bot.py                      — TelegramBot class: bot lifecycle, send_alert, callback handlers
      halt_manager.py                      — (modify) Add trigger_halt() and resolve_halt() public methods
      main.py                              — (modify) Conditionally start Telegram bot alongside monitoring loop
      requirements.txt                     — (modify) Add python-telegram-bot

  tests/
    test_telegram_alerts.py                — Tests for alert formatting (pure Python, no telegram dependency)
    test_telegram_bot.py                   — Tests for TelegramBot callbacks and send_alert
    test_halt_manager.py                   — (modify) Add tests for trigger_halt and resolve_halt
```

---

## Task 1: Config Settings & Dependency

**Files:**
- Modify: `shared/config.py`
- Modify: `services/monitoring_service/requirements.txt`
- Modify: `tests/test_config.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_config.py`:

```python
def test_telegram_settings_defaults(settings):
    assert settings.telegram_bot_token == ""
    assert settings.telegram_chat_id == ""
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd bullforge && python -m pytest tests/test_config.py::test_telegram_settings_defaults -v`
Expected: FAIL — `AttributeError: 'Settings' object has no attribute 'telegram_bot_token'`

- [ ] **Step 3: Add telegram settings to Settings**

In `shared/config.py`, append these fields inside the `Settings` class after the monitoring settings block:

```python
    # Telegram bot
    telegram_bot_token: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", "")
    )
    telegram_chat_id: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", "")
    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd bullforge && python -m pytest tests/test_config.py -v`
Expected: ALL PASS

- [ ] **Step 5: Add python-telegram-bot to requirements.txt**

Append to `services/monitoring_service/requirements.txt`:

```
python-telegram-bot>=20.7
```

- [ ] **Step 6: Install the dependency for local testing**

Run: `cd bullforge && pip install "python-telegram-bot>=20.7"`

- [ ] **Step 7: Commit**

```bash
git add shared/config.py services/monitoring_service/requirements.txt tests/test_config.py
git commit -m "feat: add Telegram bot config settings and dependency"
```

---

## Task 2: HaltManager Extensions

**Files:**
- Modify: `services/monitoring_service/halt_manager.py`
- Modify: `tests/test_halt_manager.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_halt_manager.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_halt_manager.py::test_trigger_halt -v`
Expected: FAIL — `AttributeError: 'HaltManager' object has no attribute 'trigger_halt'`

- [ ] **Step 3: Add trigger_halt and resolve_halt to HaltManager**

In `services/monitoring_service/halt_manager.py`, add these two methods to the `HaltManager` class after the `publish_status` method:

```python
    async def trigger_halt(self, reason: str) -> None:
        self._full_halt = True
        await self._db.insert_halt_event(4, reason)
        logger.critical("MANUAL HALT: %s", reason)
        # Publish empty status to signal full halt
        await self._redis.publish("models:status", json.dumps({"_halt": "full"}))

    async def resolve_halt(self) -> None:
        self._full_halt = False
        halts = await self._db.get_active_halts()
        for h in halts:
            await self._db.resolve_halt_event(h["id"])
        logger.info("Halt resolved — %d events cleared", len(halts))
```

- [ ] **Step 4: Run all halt manager tests**

Run: `cd bullforge && python -m pytest tests/test_halt_manager.py -v`
Expected: ALL PASS

- [ ] **Step 5: Run the full test suite**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add services/monitoring_service/halt_manager.py tests/test_halt_manager.py
git commit -m "feat: add trigger_halt and resolve_halt to HaltManager"
```

---

## Task 3: Alert Formatter

**Files:**
- Create: `services/monitoring_service/telegram_alerts.py`
- Create: `tests/test_telegram_alerts.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_telegram_alerts.py`:

```python
from services.monitoring_service.telegram_alerts import format_alert, format_confirmation


def test_format_model_warned():
    alert = {"tier": 1, "type": "model_warned", "model_id": 5, "reason": "Accuracy degrading"}
    text, buttons = format_alert(alert)

    assert "Tier 1" in text
    assert "Model 5" in text
    assert "Accuracy degrading" in text
    assert len(buttons) == 1
    assert len(buttons[0]) == 2
    assert buttons[0][0] == ("Disable Model", "disable:5")
    assert buttons[0][1] == ("Acknowledge", "ack")


def test_format_model_disabled():
    alert = {"tier": 2, "type": "model_disabled", "model_id": 3, "reason": "Critical threshold"}
    text, buttons = format_alert(alert)

    assert "Tier 2" in text
    assert "Model 3" in text
    assert len(buttons[0]) == 2
    assert buttons[0][0] == ("Acknowledge", "ack")
    assert buttons[0][1] == ("Re-enable", "reenable:3")


def test_format_direction_halt():
    alert = {"tier": 3, "type": "direction_halt", "reason": "Half of models disabled"}
    text, buttons = format_alert(alert)

    assert "Tier 3" in text
    assert len(buttons[0]) == 2
    assert buttons[0][0] == ("Acknowledge", "ack")
    assert buttons[0][1] == ("Full Halt", "halt_confirm")


def test_format_drawdown_halt():
    alert = {"tier": 4, "type": "drawdown_halt", "reason": "Drawdown 12% exceeds threshold"}
    text, buttons = format_alert(alert)

    assert "Tier 4" in text
    assert "Drawdown" in text
    assert len(buttons[0]) == 2
    assert buttons[0][0] == ("Full Halt", "halt_confirm")
    assert buttons[0][1] == ("Acknowledge", "ack")


def test_format_majority_halt():
    alert = {"tier": 4, "type": "majority_halt", "reason": "Majority of models disabled"}
    text, buttons = format_alert(alert)

    assert "System halted" in text.lower() or "HALT" in text
    assert len(buttons[0]) == 1
    assert buttons[0][0] == ("Acknowledge", "ack")


def test_format_unknown_type():
    alert = {"tier": 0, "type": "unknown", "reason": "Something happened"}
    text, buttons = format_alert(alert)

    assert "Something happened" in text
    assert buttons[0][0] == ("Acknowledge", "ack")


def test_format_confirmation_full_halt():
    text, buttons = format_confirmation("full_halt")

    assert "halt" in text.lower()
    assert "sure" in text.lower() or "confirm" in text.lower()
    assert len(buttons[0]) == 2
    assert buttons[0][0][1] == "halt_yes"
    assert buttons[0][1][1] == "halt_no"


def test_format_status_active():
    from services.monitoring_service.telegram_alerts import format_status

    text = format_status(
        is_halted=False,
        model_statuses=[
            {"model_id": 1, "status": "active"},
            {"model_id": 2, "status": "warned"},
        ],
        active_halts=[],
    )

    assert "ACTIVE" in text
    assert "Model 1" in text
    assert "Model 2" in text


def test_format_status_halted():
    from services.monitoring_service.telegram_alerts import format_status

    text = format_status(
        is_halted=True,
        model_statuses=[],
        active_halts=[{"tier": 4, "reason": "Manual halt"}],
    )

    assert "HALTED" in text
    assert "Manual halt" in text
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_telegram_alerts.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement alert formatter**

Create `services/monitoring_service/telegram_alerts.py`:

```python
from __future__ import annotations

from typing import List, Tuple

# Button layout type: list of rows, each row is list of (label, callback_data)
ButtonLayout = List[List[Tuple[str, str]]]


def format_alert(alert: dict) -> Tuple[str, ButtonLayout]:
    alert_type = alert.get("type", "")
    reason = alert.get("reason", "")
    model_id = alert.get("model_id")

    if alert_type == "model_warned":
        text = (
            f"[WARNING] Tier 1: Model {model_id} degraded\n\n"
            f"{reason}"
        )
        buttons = [
            [("Disable Model", f"disable:{model_id}"), ("Acknowledge", "ack")],
        ]

    elif alert_type == "model_disabled":
        text = (
            f"[ALERT] Tier 2: Model {model_id} auto-disabled\n\n"
            f"{reason}"
        )
        buttons = [
            [("Acknowledge", "ack"), ("Re-enable", f"reenable:{model_id}")],
        ]

    elif alert_type == "direction_halt":
        text = (
            f"[CRITICAL] Tier 3: Direction halted\n\n"
            f"{reason}"
        )
        buttons = [
            [("Acknowledge", "ack"), ("Full Halt", "halt_confirm")],
        ]

    elif alert_type == "drawdown_halt":
        text = (
            f"[CRITICAL] Tier 4: Drawdown threshold breached\n\n"
            f"{reason}"
        )
        buttons = [
            [("Full Halt", "halt_confirm"), ("Acknowledge", "ack")],
        ]

    elif alert_type == "majority_halt":
        text = (
            f"[HALT] System halted\n\n"
            f"{reason}"
        )
        buttons = [
            [("Acknowledge", "ack")],
        ]

    else:
        text = f"[INFO] {reason}"
        buttons = [
            [("Acknowledge", "ack")],
        ]

    return text, buttons


def format_confirmation(action: str) -> Tuple[str, ButtonLayout]:
    if action == "full_halt":
        text = (
            "Are you sure you want to halt ALL trading?\n\n"
            "This will stop all signal processing. "
            "Open positions keep their bracket orders on Alpaca."
        )
        buttons = [
            [("Yes, Halt Everything", "halt_yes"), ("Cancel", "halt_no")],
        ]
    else:
        text = f"Confirm action: {action}?"
        buttons = [
            [("Confirm", f"confirm_{action}"), ("Cancel", "cancel")],
        ]

    return text, buttons


def format_status(
    is_halted: bool,
    model_statuses: List[dict],
    active_halts: List[dict],
) -> str:
    lines = ["BullForge System Status", ""]

    if is_halted:
        lines.append("[HALTED] Trading is stopped")
    else:
        lines.append("[ACTIVE] Trading is running")

    if model_statuses:
        lines.append(f"\nModels ({len(model_statuses)}):")
        status_icons = {"active": "[OK]", "warned": "[WARN]", "disabled": "[OFF]"}
        for m in model_statuses:
            icon = status_icons.get(m["status"], "[?]")
            lines.append(f"  {icon} Model {m['model_id']}: {m['status']}")

    if active_halts:
        lines.append(f"\nActive halts ({len(active_halts)}):")
        for h in active_halts:
            lines.append(f"  Tier {h['tier']}: {h['reason']}")

    return "\n".join(lines)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_telegram_alerts.py -v`
Expected: ALL PASS (10 tests)

- [ ] **Step 5: Run the full test suite**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add services/monitoring_service/telegram_alerts.py tests/test_telegram_alerts.py
git commit -m "feat: alert formatter for Telegram messages with button layouts"
```

---

## Task 4: TelegramBot Class & Callback Handlers

**Files:**
- Create: `services/monitoring_service/telegram_bot.py`
- Create: `tests/test_telegram_bot.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_telegram_bot.py`:

```python
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
        "Manual full halt via Telegram",
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_telegram_bot.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement TelegramBot**

Create `services/monitoring_service/telegram_bot.py`:

```python
from __future__ import annotations

import json
import logging
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from shared.db import Database
from shared.config import Settings
from shared.redis import RedisClient
from services.monitoring_service.halt_manager import HaltManager
from services.monitoring_service.telegram_alerts import (
    format_alert,
    format_confirmation,
    format_status,
)

logger = logging.getLogger(__name__)


def _build_keyboard(
    buttons: list[list[tuple[str, str]]],
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(text=label, callback_data=data)
            for label, data in row
        ]
        for row in buttons
    ])


class TelegramBot:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        halt_manager: HaltManager,
        redis_client: RedisClient,
    ):
        self._token = settings.telegram_bot_token
        self._chat_id = settings.telegram_chat_id
        self._db = db
        self._halt_manager = halt_manager
        self._redis = redis_client
        self._app: Optional[Application] = None
        self._bot = None

    async def start(self) -> None:
        self._app = Application.builder().token(self._token).build()
        self._app.add_handler(CommandHandler("start", self._cmd_start))
        self._app.add_handler(CommandHandler("status", self._cmd_status))
        self._app.add_handler(CallbackQueryHandler(self._on_callback))

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling()
        self._bot = self._app.bot

        await self._redis.subscribe("monitoring:alerts", self._on_alert)

        logger.info("Telegram bot started (polling mode)")

    async def stop(self) -> None:
        if self._app:
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()
            logger.info("Telegram bot stopped")

    async def send_alert(self, alert: dict) -> None:
        text, buttons = format_alert(alert)
        keyboard = _build_keyboard(buttons)
        await self._bot.send_message(
            chat_id=self._chat_id,
            text=text,
            reply_markup=keyboard,
        )
        logger.info("Sent alert to Telegram: %s", alert.get("type"))

    async def _on_alert(self, message: str) -> None:
        try:
            alert = json.loads(message)
            await self.send_alert(alert)
        except Exception:
            logger.exception("Failed to process alert for Telegram")

    async def _on_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        query = update.callback_query
        data = query.data

        try:
            if data == "ack":
                await query.answer("Acknowledged")
                await query.edit_message_reply_markup(reply_markup=None)

            elif data.startswith("disable:"):
                model_id = int(data.split(":")[1])
                await self._db.upsert_model_status(
                    model_id, "disabled", "Manually disabled via Telegram",
                )
                await query.answer(f"Model {model_id} disabled")
                await query.edit_message_reply_markup(reply_markup=None)
                logger.info("Model %d disabled via Telegram", model_id)

            elif data.startswith("reenable:"):
                model_id = int(data.split(":")[1])
                await self._db.upsert_model_status(
                    model_id, "active", "Re-enabled via Telegram",
                )
                await query.answer(f"Model {model_id} re-enabled")
                await query.edit_message_reply_markup(reply_markup=None)
                logger.info("Model %d re-enabled via Telegram", model_id)

            elif data == "halt_confirm":
                text, buttons = format_confirmation("full_halt")
                keyboard = _build_keyboard(buttons)
                await query.edit_message_text(
                    text=text, reply_markup=keyboard,
                )
                await query.answer()

            elif data == "halt_yes":
                await self._halt_manager.trigger_halt(
                    "Manual full halt via Telegram",
                )
                await query.edit_message_text(
                    text="[HALTED] All trading has been stopped.\n\n"
                    "Open positions retain their bracket orders on Alpaca.",
                )
                await query.answer("System halted")
                logger.critical("Full halt triggered via Telegram")

            elif data == "halt_no":
                await query.edit_message_text(text="Halt cancelled.")
                await query.answer("Cancelled")

            else:
                await query.answer("Unknown action")

        except Exception:
            logger.exception("Error handling callback: %s", data)
            await query.answer("Error processing action")

    async def _cmd_start(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        chat_id = update.effective_chat.id
        await update.message.reply_text(
            f"BullForge monitoring bot active.\n"
            f"Chat ID: {chat_id}\n\n"
            f"Commands:\n"
            f"/status — View system status\n"
        )

    async def _cmd_status(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        try:
            model_statuses_raw = await self._db.get_all_model_statuses()
            active_halts = await self._db.get_active_halts()

            model_dicts = [
                {"model_id": m.model_id, "status": m.status}
                for m in model_statuses_raw
            ]

            text = format_status(
                is_halted=self._halt_manager.is_halted,
                model_statuses=model_dicts,
                active_halts=active_halts,
            )
            await update.message.reply_text(text)
        except Exception:
            logger.exception("Error handling /status command")
            await update.message.reply_text("Error fetching status.")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_telegram_bot.py -v`
Expected: ALL PASS (12 tests)

- [ ] **Step 5: Run the full test suite**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add services/monitoring_service/telegram_bot.py tests/test_telegram_bot.py
git commit -m "feat: TelegramBot with alert sending, callback handlers, and commands"
```

---

## Task 5: Monitoring Service Integration

**Files:**
- Modify: `services/monitoring_service/main.py`

- [ ] **Step 1: Add Telegram bot startup to main.py**

In `services/monitoring_service/main.py`, add the import after the existing monitoring imports:

```python
from services.monitoring_service.telegram_bot import TelegramBot
```

Then in the `main()` function, after the `halt_manager` initialization and before the health check setup, add:

```python
    # Start Telegram bot (optional — only if token is configured)
    telegram_bot = None
    if settings.telegram_bot_token and settings.telegram_chat_id:
        telegram_bot = TelegramBot(settings, db, halt_manager, redis_client)
        await telegram_bot.start()
        logger.info("Telegram bot started")
    else:
        logger.info("Telegram bot disabled (no token configured)")
```

In the shutdown section at the end of `main()`, before `await redis_client.disconnect()`, add:

```python
    if telegram_bot:
        await telegram_bot.stop()
```

- [ ] **Step 2: Verify the module imports correctly**

Run: `cd bullforge && python -c "from services.monitoring_service.main import main; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Run the full test suite**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add services/monitoring_service/main.py
git commit -m "feat: conditionally start Telegram bot in monitoring service"
```

---

## Task 6: Docker Compose & Dockerfile Updates

**Files:**
- Modify: `docker-compose.yml`
- Modify: `services/monitoring_service/Dockerfile`

- [ ] **Step 1: Add Telegram env vars to monitoring_service in docker-compose.yml**

In `docker-compose.yml`, update the `monitoring_service` environment section to include:

```yaml
  monitoring_service:
    build:
      context: .
      dockerfile: services/monitoring_service/Dockerfile
    env_file:
      - .env
    environment:
      TIMESCALEDB_HOST: timescaledb
      REDIS_HOST: redis
      TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN:-}
      TELEGRAM_CHAT_ID: ${TELEGRAM_CHAT_ID:-}
    ports:
      - "18083:8083"
    depends_on:
      timescaledb:
        condition: service_healthy
      redis:
        condition: service_healthy
      execution_service:
        condition: service_started
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8083/health')"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 15s
    restart: unless-stopped
```

- [ ] **Step 2: Update Dockerfile to include telegram modules**

In `services/monitoring_service/Dockerfile`, ensure the COPY step for the monitoring service includes all new files. The existing Dockerfile from Phase 5A should already copy the full `services/monitoring_service/` directory, so no changes are needed unless the Dockerfile uses explicit file copies. Verify it contains:

```dockerfile
COPY services/monitoring_service/ /app/services/monitoring_service/
```

- [ ] **Step 3: Verify docker-compose syntax**

Run: `cd bullforge && docker compose config --quiet`
Expected: No errors

- [ ] **Step 4: Run the full test suite one final time**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add Telegram bot env vars to Docker Compose monitoring service"
```

---

## Summary

| Component | Description |
|---|---|
| **telegram_alerts.py** | Pure formatting functions: alert dict to (text, button layout), confirmation prompts, system status |
| **telegram_bot.py** | TelegramBot class: bot lifecycle, send_alert, 6 callback handlers (ack, disable, re-enable, halt_confirm, halt_yes, halt_no), /start and /status commands |
| **HaltManager extensions** | `trigger_halt(reason)` for manual halts, `resolve_halt()` to clear all active halts |
| **Config** | `telegram_bot_token` and `telegram_chat_id` settings (empty = bot disabled) |

**Alert type mapping from spec:**

| Alert Type | Bot Format | Buttons |
|---|---|---|
| Sub-model degraded | `[WARNING] Tier 1: Model N degraded` | Disable Model, Acknowledge |
| Sub-model auto-disabled | `[ALERT] Tier 2: Model N auto-disabled` | Acknowledge, Re-enable |
| Direction halted | `[CRITICAL] Tier 3: Direction halted` | Acknowledge, Full Halt |
| Drawdown threshold | `[CRITICAL] Tier 4: Drawdown threshold breached` | Full Halt, Acknowledge |
| System halted | `[HALT] System halted` | Acknowledge |

**Destructive action flow:** Full Halt button shows confirmation prompt ("Are you sure?") with Yes/Cancel before executing.

**Phase 5C (Dashboard)** will query the same `metric_snapshots`, `model_status`, and `halt_events` tables to render the web UI.
