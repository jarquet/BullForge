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
