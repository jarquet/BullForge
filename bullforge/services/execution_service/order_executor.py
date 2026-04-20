from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timezone
from typing import Optional

from shared.db import Database
from shared.models import BacktestScope, Order, TradingSignal
from services.execution_service.alpaca_broker import AlpacaBroker
from services.execution_service.reconciler_core import ReconcilerCore
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
        core: ReconcilerCore,
        *,
        scope: Optional[BacktestScope] = None,
    ):
        self._broker = broker
        self._risk = risk_manager
        self._tracker = position_tracker
        self._db = db
        self._core = core
        self._scope = scope

    async def process_signal(
        self, signal: TradingSignal, current_price: float,
        *, max_qty_cap: Optional[float] = None,
    ) -> Optional[Order]:
        await self._tracker.refresh()
        existing = self._tracker.get_position(signal.symbol)

        if existing:
            should_exit, _reason = self._risk.check_exit(signal, existing)
            if should_exit:
                return await self._execute_close(signal, existing)
            return None

        if signal.direction == 0:
            return None

        account = self._tracker.account
        positions = list(self._tracker.positions.values())
        approved, reason, qty = await self._risk.check_entry(
            signal, account, positions, current_price,
        )
        if not approved:
            logger.info("Entry rejected for %s: %s", signal.symbol, reason)
            return None

        if max_qty_cap is not None and qty > max_qty_cap:
            qty = max_qty_cap
            if qty * current_price < 1.0:
                logger.info(
                    "Entry capped below $1 notional for %s; skipping submit",
                    signal.symbol,
                )
                return None

        return await self._execute_open(signal, qty)

    async def _execute_open(self, signal: TradingSignal, qty: float) -> Optional[Order]:
        side = "buy" if signal.direction == 1 else "sell"
        try:
            alpaca_id = await self._broker.submit_market_order(signal.symbol, qty, side)
        except Exception as e:
            logger.error("Failed to submit order for %s: %s", signal.symbol, e)
            return None

        now = datetime.now(timezone.utc)
        order = Order(
            symbol=signal.symbol, side=side, quantity=qty,
            order_type="market", submitted_at=now,
            signal_strength=signal.strength,
            alpaca_order_id=alpaca_id, status="submitted",
        )

        if self._scope is not None:
            # Backtest: SimulatedBroker is synchronous and the reconciler is a
            # no-op, so the terminal status is already known. Skip the orders
            # INSERT entirely for non-fills to avoid wasted DB round-trips.
            snap = await self._broker.get_order(alpaca_id)
            if snap.get("status") != "filled":
                logger.info(
                    "skipping order insert for %s %s: broker status=%s",
                    signal.symbol, alpaca_id, snap.get("status"),
                )
                return None

        order_id = await self._db.insert_order_if_new(order, scope=self._scope)

        try:
            snap = await self._broker.get_order(alpaca_id)
            snap.setdefault("symbol", signal.symbol)
            snap.setdefault("side", side)
            snap.setdefault("qty", qty)
            snap.setdefault("order_type", "market")
            snap.setdefault("submitted_at", now)
            await self._core.apply_alpaca_order(snap, source="submit")
        except Exception:
            logger.exception(
                "submit-path reconciliation failed for %s; stream/sweeper will recover",
                alpaca_id,
            )

        if order_id is None:
            # Stream already inserted this row; fetch it as the authoritative record.
            return await self._db.get_order_by_alpaca_id(alpaca_id, scope=self._scope)
        return replace(order, id=order_id)

    async def _execute_close(self, signal: TradingSignal, position: dict) -> Optional[Order]:
        alpaca_id = await self._broker.close_position(signal.symbol)
        if not alpaca_id:
            logger.warning("Failed to close position for %s", signal.symbol)
            return None

        qty = abs(float(position["qty"]))
        side = "sell" if float(position["qty"]) > 0 else "buy"
        now = datetime.now(timezone.utc)

        order = Order(
            symbol=signal.symbol, side=side, quantity=qty,
            order_type="market", submitted_at=now,
            signal_strength=signal.strength,
            alpaca_order_id=alpaca_id, status="submitted",
        )

        if self._scope is not None:
            snap = await self._broker.get_order(alpaca_id)
            if snap.get("status") != "filled":
                logger.info(
                    "skipping order insert for %s %s: broker status=%s",
                    signal.symbol, alpaca_id, snap.get("status"),
                )
                return None

        order_id = await self._db.insert_order_if_new(order, scope=self._scope)

        try:
            snap = await self._broker.get_order(alpaca_id)
            snap.setdefault("symbol", signal.symbol)
            snap.setdefault("side", side)
            snap.setdefault("qty", qty)
            snap.setdefault("order_type", "market")
            snap.setdefault("submitted_at", now)
            await self._core.apply_alpaca_order(snap, source="submit")
        except Exception:
            logger.exception(
                "close-path reconciliation failed for %s; stream/sweeper will recover",
                alpaca_id,
            )

        if order_id is None:
            # Stream already inserted this row; fetch it as the authoritative record.
            return await self._db.get_order_by_alpaca_id(alpaca_id, scope=self._scope)
        return replace(order, id=order_id)
