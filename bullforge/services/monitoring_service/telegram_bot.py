from __future__ import annotations

import dataclasses
import json
import logging
from datetime import datetime, timezone
from functools import partial
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
    format_models_summary,
    format_positions,
    format_status,
    format_status_expanded,
    format_trades,
)

logger = logging.getLogger(__name__)


# Alert types whose publication on `monitoring:alerts` should NOT result in
# a Telegram message. They remain on the Redis channel for other consumers.
TELEGRAM_SUPPRESSED_ALERT_TYPES = frozenset({"model_warned", "model_disabled"})


HELP_TEXT = (
    "BullForge commands:\n"
    "/help — this list\n"
    "/status — equity, exposure %, trades/win-rate/PnL today, "
    "model counts (active/warned/disabled), max bar age, "
    "active halt count, last retrain time\n"
    "/positions — open broker positions with symbol, side, "
    "quantity, and market value\n"
    "/trades — last 5 closed trades with entry/exit prices, "
    "PnL, and hold duration\n"
    "/models — configured models with type, symbol, timeframe, "
    "horizon, status, and latest metrics\n"
    "/halt — trigger a manual full trading halt (tier 5)\n"
    "/resume — clear all active halts and resume trading\n"
    "/start — welcome message with this chat's ID"
)


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
            alert_type = alert.get("type")
            if alert_type in TELEGRAM_SUPPRESSED_ALERT_TYPES:
                logger.debug(
                    "Suppressed Telegram delivery for alert type %s",
                    alert_type,
                )
                return
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
                    "Manual full halt via Telegram", tier=5,
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
            f"Type /help for the full command list."
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

    def attach_handlers(
        self, *, db, broker_client, runtime_config, halt_manager,
    ) -> None:
        """Register Phase 6C module-level command handlers on the running app."""
        if self._app is None:
            raise RuntimeError("attach_handlers called before start()")
        register_handlers(
            self._app,
            db=db,
            broker_client=broker_client,
            runtime_config=runtime_config,
            halt_manager=halt_manager,
        )


# -----------------------------------------------------------------------------
# Phase 6C module-level command handlers (DI via kwargs)
# -----------------------------------------------------------------------------


async def handle_status(
    update, context, *, db, broker_client, runtime_config, halt_manager,
):
    try:
        account = await broker_client.get_account()
    except TimeoutError:
        await update.message.reply_text(
            "Execution service unavailable (broker relay timeout)."
        )
        return
    try:
        positions = await broker_client.get_positions()
    except TimeoutError:
        positions = []

    trading_snap = await db.query_latest_metric_snapshot("trading", "", "all")
    trading_stats = (trading_snap or {}).get("metrics", {}) or {}

    statuses = await db.get_all_model_statuses()
    counts = {"active": 0, "warned": 0, "disabled": 0}
    for s in statuses:
        counts[s.status] = counts.get(s.status, 0) + 1

    bar_rows = await db.query_latest_bar_timestamps()
    now = datetime.now(timezone.utc)
    ages = [
        (now - r["last_bar"]).total_seconds()
        for r in bar_rows if r.get("last_bar")
    ]
    freshness = (
        f"Max bar age: {max(ages) / 60:.1f}m" if ages else "no bars yet"
    )

    halts = await db.get_active_halts()
    try:
        last_retrain = await runtime_config.get("last_retrain_time")
    except Exception:
        last_retrain = ""

    equity = float(account.get("equity", 0.0))
    exposure_val = sum(
        abs(float(p.get("market_value", 0))) for p in positions
    )
    exposure_pct = (exposure_val / equity) if equity else 0.0

    msg = format_status_expanded(
        account=account,
        positions_count=len(positions),
        exposure_pct=exposure_pct,
        trading_stats=trading_stats,
        model_counts=counts,
        freshness_summary=freshness,
        active_halts_count=len(halts),
        last_retrain_iso=last_retrain or "",
        now=now,
    )
    await update.message.reply_text(msg)


async def handle_positions(
    update, context, *, db, broker_client, runtime_config, halt_manager,
):
    try:
        positions = await broker_client.get_positions()
    except TimeoutError:
        await update.message.reply_text(
            "Execution service unavailable (broker relay timeout)."
        )
        return
    await update.message.reply_text(format_positions(positions))


async def handle_trades(
    update, context, *, db, broker_client, runtime_config, halt_manager,
):
    raw = await db.query_closed_trades(limit=5)
    trades: list[dict] = []
    for t in raw:
        if dataclasses.is_dataclass(t):
            trades.append(dataclasses.asdict(t))
        elif isinstance(t, dict):
            trades.append(t)
        else:
            # Best-effort attribute access for non-dataclass objects.
            trades.append({
                k: getattr(t, k, None)
                for k in (
                    "symbol", "side", "quantity", "entry_price",
                    "exit_price", "pnl", "hold_duration_seconds", "exit_time",
                )
            })
    await update.message.reply_text(format_trades(trades))


async def handle_models(
    update, context, *, db, broker_client, runtime_config, halt_manager,
):
    details = await db.query_model_details()
    enriched = []
    for d in details:
        enriched.append({
            **d,
            "label": (
                f"{d['model_type']} \u00b7 {d['symbol']} \u00b7 "
                f"{d['timeframe']} \u00b7 {d['horizon_minutes']}m"
            ),
        })
    await update.message.reply_text(format_models_summary(enriched))


async def handle_halt(
    update, context, *, db, broker_client, runtime_config, halt_manager,
):
    # Inline confirmation dialog is a documented follow-up per plan.
    # tier=5 distinguishes manual Telegram halts from automated tier-4 circuit breakers.
    await halt_manager.trigger_halt("Manual halt via Telegram", tier=5)
    await update.message.reply_text(
        "Trading halted. Use /resume to restart."
    )


async def handle_resume(
    update, context, *, db, broker_client, runtime_config, halt_manager,
):
    await halt_manager.resolve_halt()
    await update.message.reply_text("Trading resumed. All halts cleared.")


async def handle_help(
    update, context, *, db, broker_client, runtime_config, halt_manager,
):
    await update.message.reply_text(HELP_TEXT)


async def _wrap(
    fn, update: Update, context: ContextTypes.DEFAULT_TYPE, *, deps,
):
    await fn(update, context, **deps)


def register_handlers(
    app: Application, *, db, broker_client, runtime_config, halt_manager,
) -> None:
    deps = dict(
        db=db,
        broker_client=broker_client,
        runtime_config=runtime_config,
        halt_manager=halt_manager,
    )
    app.add_handler(
        CommandHandler("status", partial(_wrap, handle_status, deps=deps))
    )
    app.add_handler(
        CommandHandler(
            "positions", partial(_wrap, handle_positions, deps=deps),
        )
    )
    app.add_handler(
        CommandHandler("trades", partial(_wrap, handle_trades, deps=deps))
    )
    app.add_handler(
        CommandHandler("models", partial(_wrap, handle_models, deps=deps))
    )
    app.add_handler(
        CommandHandler("halt", partial(_wrap, handle_halt, deps=deps))
    )
    app.add_handler(
        CommandHandler("resume", partial(_wrap, handle_resume, deps=deps))
    )
    app.add_handler(
        CommandHandler("help", partial(_wrap, handle_help, deps=deps))
    )
