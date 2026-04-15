from __future__ import annotations

import json
import logging
from typing import Optional

from shared.db import Database
from shared.models import Order

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
        # Implemented in Task 8
        pass

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
