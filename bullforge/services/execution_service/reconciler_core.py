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

# Absolute tolerance (per unit of fill quantity) for matching a closing fill
# against open trade rows. Fractional equity/crypto quantities are floats,
# and a float sum of open lots can exceed the broker-reported close quantity
# by ~1e-16 relative (0.1 + 0.2 > 0.3), which used to push an exact full
# close into the partial-drain branch and strand the last lot as open.
_QTY_REL_TOL = 1e-9

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

        if old_status in TERMINAL_STATUSES and new_status not in TERMINAL_STATUSES:
            # Stale snapshot (e.g. a late "accepted" stream event after the
            # submit path already saw "filled"). The DB layer refuses the
            # regression; never treat it as a transition here either, or a
            # later fill event would book the same fill a second time.
            logger.info(
                "reconciler ignoring stale snapshot alpaca_id=%s stored=%s "
                "incoming=%s source=%s",
                order.alpaca_order_id, old_status, new_status, source,
            )
            return

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

        open_trades = await self._db.get_open_trades(order.symbol)

        # A sell closes longs and a buy closes shorts. Only rows on that side
        # are candidates: the old check looked at open_trades[0] alone and
        # then drained every open row regardless of side, so one stray row
        # of the wrong side (oldest first) flipped a closing fill into a new
        # entry, or closed a long with a buy.
        closing_side = "long" if order.side == "sell" else "short"
        closable = [t for t in open_trades if t.side == closing_side]

        if not closable:
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
            return

        # Closing fill — drain N rows (FIFO by entry_time).
        fq = float(order.filled_quantity or order.quantity)
        total_open = sum(float(t.quantity) for t in closable)
        tol = _QTY_REL_TOL * max(1.0, fq)

        if fq >= total_open - tol:
            rows_to_close = list(closable)
            if fq > total_open + tol:
                await self._publish_counter(
                    "reconciler_anomalies_total", kind="over_close",
                )
                await self._publish("execution:orders:anomaly", order)
        else:
            rows_to_close = []
            running = 0.0
            for t in closable:
                if running + float(t.quantity) <= fq + tol:
                    rows_to_close.append(t)
                    running += float(t.quantity)
                else:
                    break
            residual = fq - running
            if residual > tol:
                await self._publish_counter(
                    "reconciler_anomalies_total",
                    kind="partial_drain_residual",
                )
                await self._publish("execution:orders:anomaly", order)

        for t in rows_to_close:
            pnl = (order.filled_price - t.entry_price) * t.quantity
            if t.side == "short":
                pnl = -pnl
            hold_secs = int(
                (order.filled_at - t.entry_time).total_seconds()
            )
            await self._db.update_trade_exit(
                trade_id=t.id,
                exit_time=order.filled_at,
                exit_price=order.filled_price,
                exit_order_id=order.id,
                pnl=pnl,
                hold_duration_seconds=hold_secs,
            )
            await self._publish_position_closed(
                open_trade=t,
                order=order,
                pnl=pnl,
                hold_duration_seconds=hold_secs,
            )

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
