from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from shared.db import Database
from shared.models import Order, TradingSignal
from shared.redis import RedisClient
from services.execution_service.alpaca_broker import AlpacaBroker
from services.execution_service.risk_manager import RiskManager
from services.execution_service.position_tracker import PositionTracker

logger = logging.getLogger(__name__)


class OrderExecutor:
    def __init__(
        self,
        broker: AlpacaBroker,
        risk_manager: RiskManager,
        position_tracker: PositionTracker,
        db: Database,
        redis_client: Optional[RedisClient] = None,
        fill_check_delay: float = 1.0,
        fill_check_retries: int = 3,
    ):
        self._broker = broker
        self._risk = risk_manager
        self._tracker = position_tracker
        self._db = db
        self._redis = redis_client
        self._fill_check_delay = fill_check_delay
        self._fill_check_retries = fill_check_retries

    async def process_signal(
        self, signal: TradingSignal, current_price: float,
    ) -> Optional[Order]:
        await self._tracker.refresh()
        existing = self._tracker.get_position(signal.symbol)

        if existing:
            should_exit, reason = self._risk.check_exit(signal, existing)
            if should_exit:
                return await self._execute_close(signal, existing)
            return None

        if signal.direction == 0:
            return None

        account = self._tracker.account
        positions = list(self._tracker.positions.values())
        approved, reason, qty = self._risk.check_entry(
            signal, account, positions, current_price,
        )

        if not approved:
            logger.info("Entry rejected for %s: %s", signal.symbol, reason)
            return None

        return await self._execute_open(signal, qty)

    async def _execute_open(
        self, signal: TradingSignal, qty: int,
    ) -> Optional[Order]:
        side = "buy" if signal.direction == 1 else "sell"
        try:
            alpaca_id = await self._broker.submit_market_order(
                signal.symbol, qty, side,
            )
        except Exception as e:
            logger.error("Failed to submit order for %s: %s", signal.symbol, e)
            return None

        now = datetime.now(timezone.utc)
        order = Order(
            symbol=signal.symbol,
            side=side,
            quantity=float(qty),
            order_type="market",
            submitted_at=now,
            signal_strength=signal.strength,
            alpaca_order_id=alpaca_id,
            status="submitted",
        )
        order_id = await self._db.insert_order(order)

        filled_order = await self._check_fill(order, order_id)
        await self._publish_order_event(filled_order)
        return filled_order

    async def _execute_close(
        self, signal: TradingSignal, position: dict,
    ) -> Optional[Order]:
        alpaca_id = await self._broker.close_position(signal.symbol)
        if not alpaca_id:
            logger.warning("Failed to close position for %s", signal.symbol)
            return None

        qty = abs(float(position["qty"]))
        side = "sell" if float(position["qty"]) > 0 else "buy"
        now = datetime.now(timezone.utc)

        order = Order(
            symbol=signal.symbol,
            side=side,
            quantity=qty,
            order_type="market",
            submitted_at=now,
            signal_strength=signal.strength,
            alpaca_order_id=alpaca_id,
            status="submitted",
        )
        order_id = await self._db.insert_order(order)

        filled_order = await self._check_fill(order, order_id)
        await self._publish_order_event(filled_order)
        return filled_order

    async def _check_fill(self, order: Order, order_id: int) -> Order:
        for attempt in range(self._fill_check_retries):
            if self._fill_check_delay > 0:
                await asyncio.sleep(self._fill_check_delay)

            try:
                alpaca_order = await self._broker.get_order(order.alpaca_order_id)
            except Exception as e:
                logger.warning(
                    "Fill check failed for %s (attempt %d/%d): %s",
                    order.alpaca_order_id, attempt + 1,
                    self._fill_check_retries, e,
                )
                continue

            status = alpaca_order["status"]

            if status in ("filled", "partially_filled"):
                await self._db.update_order_status(
                    order_id,
                    status=status,
                    filled_at=alpaca_order.get("filled_at"),
                    filled_price=alpaca_order.get("filled_avg_price"),
                    filled_quantity=alpaca_order.get("filled_qty"),
                )
                return dataclasses.replace(
                    order,
                    status=status,
                    filled_at=alpaca_order.get("filled_at"),
                    filled_price=alpaca_order.get("filled_avg_price"),
                    filled_quantity=alpaca_order.get("filled_qty"),
                    id=order_id,
                )

            if status in ("cancelled", "rejected"):
                await self._db.update_order_status(order_id, status=status)
                return dataclasses.replace(order, status=status, id=order_id)

        logger.warning(
            "Fill check exhausted %d retries for %s",
            self._fill_check_retries, order.alpaca_order_id,
        )
        return dataclasses.replace(order, id=order_id)

    async def _publish_order_event(self, order: Order) -> None:
        if not self._redis:
            return
        await self._redis.publish(
            "execution:orders",
            json.dumps({
                "symbol": order.symbol,
                "side": order.side,
                "quantity": order.quantity,
                "status": order.status,
                "filled_price": order.filled_price,
            }),
        )
