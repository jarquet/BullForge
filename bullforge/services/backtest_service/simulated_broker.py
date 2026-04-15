from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from services.backtest_service.fill_model import FillModel, FillRequest


class SimulatedBroker:
    """In-memory broker that mirrors AlpacaBroker's async interface.

    Intended for backtests. Callers (BacktestEngine) must:
      1. set_clock(ts) before each bar
      2. set_next_bar(symbol, bar) for any symbol that may receive an order
         this step (the order will fill against this bar's open)
      3. mark_to_market(symbol, price) once per bar to update unrealized PnL
    """

    def __init__(self, *, starting_cash: float, fill_model: FillModel):
        self._cash = starting_cash
        self._starting_cash = starting_cash
        self._fill_model = fill_model
        self._positions: dict[str, dict] = {}
        self._orders: dict[str, dict] = {}
        self._next_bars: dict[str, dict] = {}
        self._clock: Optional[datetime] = None

    def set_clock(self, ts: datetime) -> None:
        if ts.tzinfo is None or ts.tzinfo != timezone.utc:
            raise ValueError("clock must be UTC")
        self._clock = ts

    def set_next_bar(self, symbol: str, bar: dict) -> None:
        self._next_bars[symbol] = bar

    def mark_to_market(self, symbol: str, price: float) -> None:
        pos = self._positions.get(symbol)
        if pos is None:
            return
        qty = pos["qty"]
        pos["current_price"] = price
        pos["market_value"] = price * qty
        pos["unrealized_pl"] = (price - pos["avg_entry_price"]) * qty

    async def get_account(self) -> dict:
        position_value = sum(
            abs(p["market_value"]) for p in self._positions.values()
        )
        equity = self._cash + sum(
            p["market_value"] for p in self._positions.values()
        )
        return {
            "equity": equity,
            "buying_power": max(equity - position_value, 0.0),
            "cash": self._cash,
            "portfolio_value": equity,
        }

    async def get_positions(self) -> list[dict]:
        return [dict(p) for p in self._positions.values()]

    async def get_position(self, symbol: str) -> Optional[dict]:
        pos = self._positions.get(symbol)
        return dict(pos) if pos is not None else None

    async def submit_market_order(
        self, symbol: str, qty: float, side: str,
    ) -> str:
        if self._clock is None:
            raise RuntimeError("set_clock() must be called before submitting")
        order_id = str(uuid.uuid4())
        bar = self._next_bars.get(symbol)
        if bar is None:
            self._orders[order_id] = self._snapshot(
                order_id, symbol, side, qty, "rejected", None, None,
            )
            return order_id

        result = self._fill_model.fill(
            FillRequest(side=side, qty=qty), bar,
        )
        if not result.filled:
            self._orders[order_id] = self._snapshot(
                order_id, symbol, side, qty, "rejected", None, None,
            )
            return order_id

        self._apply_fill(symbol, side, result.qty, result.price)
        self._orders[order_id] = self._snapshot(
            order_id, symbol, side, qty,
            "filled", result.qty, result.price,
        )
        return order_id

    async def get_order(self, order_id: str) -> dict:
        return dict(self._orders[order_id])

    async def close_position(self, symbol: str) -> Optional[str]:
        pos = self._positions.get(symbol)
        if pos is None:
            return None
        qty = abs(pos["qty"])
        side = "sell" if pos["qty"] > 0 else "buy"
        return await self.submit_market_order(symbol, qty, side)

    async def list_open_orders(self, after) -> list[dict]:
        # Fills are synchronous in submit_market_order, so no order is ever open.
        return []

    def _apply_fill(
        self, symbol: str, side: str, qty: float, price: float,
    ) -> None:
        signed_qty = qty if side == "buy" else -qty
        self._cash -= signed_qty * price
        existing = self._positions.get(symbol)
        if existing is None:
            self._positions[symbol] = {
                "symbol": symbol,
                "qty": signed_qty,
                "side": "long" if signed_qty > 0 else "short",
                "avg_entry_price": price,
                "current_price": price,
                "unrealized_pl": 0.0,
                "market_value": signed_qty * price,
            }
            return
        new_qty = existing["qty"] + signed_qty
        if new_qty == 0:
            del self._positions[symbol]
            return
        if (existing["qty"] > 0) == (signed_qty > 0):
            total_cost = (
                existing["avg_entry_price"] * existing["qty"]
                + price * signed_qty
            )
            existing["avg_entry_price"] = total_cost / new_qty
        elif new_qty != 0:
            # Signs flipped (position reversal): residual position opens at the fill price.
            existing["avg_entry_price"] = price
        existing["qty"] = new_qty
        existing["side"] = "long" if new_qty > 0 else "short"
        existing["current_price"] = price
        existing["market_value"] = new_qty * price
        existing["unrealized_pl"] = (
            (price - existing["avg_entry_price"]) * new_qty
        )

    def _snapshot(
        self, order_id, symbol, side, qty, status, filled_qty, filled_price,
    ) -> dict:
        return {
            "id": order_id,
            "symbol": symbol,
            "side": side,
            "qty": float(qty),
            "order_type": "market",
            "status": status,
            "submitted_at": self._clock,
            "filled_qty": filled_qty,
            "filled_avg_price": filled_price,
            "filled_at": self._clock if status == "filled" else None,
        }
