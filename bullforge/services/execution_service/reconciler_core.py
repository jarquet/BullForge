from __future__ import annotations

import json
import logging
from typing import Optional

from shared.db import Database
from shared.models import Order, Trade

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = {"filled", "cancelled", "rejected", "expired"}


class ReconcilerCore:
    def __init__(self, db: Database, redis_client=None):
        self._db = db
        self._redis = redis_client

    async def apply_alpaca_order(
        self, alpaca_order: dict, source: str,
    ) -> None:
        result = await self._db.upsert_order_from_alpaca(
            alpaca_order, source=source,
        )
        old_status = result.old_status
        new_status = result.new_status
        order = result.order

        status_changed = old_status != new_status
        if status_changed:
            logger.info(
                "reconciler alpaca_id=%s old=%s new=%s source=%s origin=%s",
                order.alpaca_order_id, old_status, new_status, source,
                order.origin,
            )

        if (
            old_status == "filled"
            and new_status in TERMINAL_STATUSES
            and new_status != "filled"
        ):
            logger.error(
                "anomaly: order %s went from filled -> %s",
                order.alpaca_order_id, new_status,
            )
            await self._publish("execution:orders:anomaly", order)
            await self._publish("execution:orders", order)
            return

        if status_changed and new_status == "filled":
            await self._handle_fill(order)

        if status_changed:
            await self._publish("execution:orders", order)

    async def _handle_fill(self, order: Order) -> None:
        if order.filled_price is None or order.filled_at is None:
            logger.warning(
                "fill event for %s missing price/time; skipping trade bookkeeping",
                order.alpaca_order_id,
            )
            return

        open_trade = await self._db.get_open_trade(order.symbol)

        is_closing = open_trade is not None and (
            (open_trade.side == "long" and order.side == "sell")
            or (open_trade.side == "short" and order.side == "buy")
        )

        if is_closing:
            pnl = (order.filled_price - open_trade.entry_price) * open_trade.quantity
            if open_trade.side == "short":
                pnl = -pnl
            hold_secs = int(
                (order.filled_at - open_trade.entry_time).total_seconds()
            )
            await self._db.update_trade_exit(
                trade_id=open_trade.id,
                exit_time=order.filled_at,
                exit_price=order.filled_price,
                exit_order_id=order.id,
                pnl=pnl,
                hold_duration_seconds=hold_secs,
            )
        else:
            trade_side = "long" if order.side == "buy" else "short"
            trade = Trade(
                symbol=order.symbol,
                side=trade_side,
                entry_time=order.filled_at,
                entry_price=order.filled_price,
                quantity=order.filled_quantity or order.quantity,
                entry_order_id=order.id,
            )
            await self._db.insert_trade(trade)

    async def _publish(self, channel: str, order: Order) -> None:
        if self._redis is None:
            return
        payload = json.dumps({
            "symbol": order.symbol,
            "side": order.side,
            "quantity": order.quantity,
            "status": order.status,
            "filled_price": order.filled_price,
            "alpaca_order_id": order.alpaca_order_id,
            "origin": order.origin,
        })
        await self._redis.publish(channel, payload)
