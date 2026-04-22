from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from shared.db import Database
from shared.models import Order, Trade
from services.execution_service.day_stats import compute_day_stats

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = {"filled", "cancelled", "rejected", "expired"}

_MONITORING_CHANNEL = "monitoring:reconciler"
_ALERTS_CHANNEL = "monitoring:alerts"


class ReconcilerCore:
    def __init__(self, db: Database, redis_client=None, broker=None):
        self._db = db
        self._redis = redis_client
        self._broker = broker

    async def _publish_counter(self, name: str, **labels) -> None:
        if self._redis is None:
            return
        payload = json.dumps({"counter": name, "labels": labels, "value": 1})
        await self._redis.publish(_MONITORING_CHANNEL, payload)

    async def record_stream_reconnect(self) -> None:
        await self._publish_counter("reconciler_stream_reconnects_total")

    async def apply_alpaca_order(
        self, alpaca_order: dict, source: str,
    ) -> None:
        result = await self._db.upsert_order_from_alpaca(
            alpaca_order, source=source,
        )
        old_status = result.old_status
        new_status = result.new_status
        inserted = result.inserted
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
            await self._publish_counter(
                "reconciler_anomalies_total", kind="filled_reverted",
            )
            await self._publish("execution:orders:anomaly", order)
            await self._publish("execution:orders", order)
            return

        if status_changed:
            old_label = "none" if inserted else (old_status or "none")
            await self._publish_counter(
                "reconciler_events_total",
                source=source,
                transition=f"{old_label}->{new_status}",
            )

        if inserted and source == "poll":
            await self._publish_counter("reconciler_orphans_ingested_total")

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
            await self._publish_position_closed(
                open_trade=open_trade,
                order=order,
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

    async def _publish_position_closed(
        self,
        *,
        open_trade: Trade,
        order: Order,
        pnl: float,
        hold_duration_seconds: int,
    ) -> None:
        if self._redis is None or self._broker is None:
            return
        try:
            day = await compute_day_stats(
                self._db, self._broker, datetime.now(timezone.utc),
            )
            notional = open_trade.entry_price * open_trade.quantity
            pnl_pct = (pnl / notional) if notional else 0.0
            payload = {
                "type": "position_closed",
                "tier": 0,
                "trade_id": open_trade.id,
                "symbol": open_trade.symbol,
                "side": open_trade.side,
                "quantity": float(open_trade.quantity),
                "entry_time": _iso(open_trade.entry_time),
                "entry_price": float(open_trade.entry_price),
                "exit_time": _iso(order.filled_at),
                "exit_price": float(order.filled_price),
                "pnl": float(pnl),
                "pnl_pct": float(pnl_pct),
                "hold_duration_seconds": int(hold_duration_seconds),
                "account": day,
            }
            await self._redis.publish(_ALERTS_CHANNEL, json.dumps(payload))
        except Exception:
            logger.exception("Failed to publish position_closed alert")


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")
