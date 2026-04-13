from __future__ import annotations

import asyncio
import logging
from datetime import timezone
from typing import Optional

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from shared.config import Settings

logger = logging.getLogger(__name__)


class AlpacaBroker:
    def __init__(self, settings: Settings):
        self._client = TradingClient(
            api_key=settings.alpaca_api_key,
            secret_key=settings.alpaca_secret_key,
            paper=True,
        )

    # --- Async public API (delegates to sync via to_thread) ---

    async def get_account(self) -> dict:
        return await asyncio.to_thread(self._sync_get_account)

    async def get_positions(self) -> list[dict]:
        return await asyncio.to_thread(self._sync_get_positions)

    async def get_position(self, symbol: str) -> Optional[dict]:
        return await asyncio.to_thread(self._sync_get_position, symbol)

    async def submit_market_order(
        self, symbol: str, qty: float, side: str
    ) -> str:
        return await asyncio.to_thread(
            self._sync_submit_market_order, symbol, qty, side,
        )

    async def get_order(self, alpaca_order_id: str) -> dict:
        return await asyncio.to_thread(self._sync_get_order, alpaca_order_id)

    async def close_position(self, symbol: str) -> Optional[str]:
        return await asyncio.to_thread(self._sync_close_position, symbol)

    # --- Sync helpers ---

    def _sync_get_account(self) -> dict:
        account = self._client.get_account()
        return {
            "equity": float(account.equity),
            "buying_power": float(account.buying_power),
            "cash": float(account.cash),
            "portfolio_value": float(account.portfolio_value),
        }

    def _sync_get_positions(self) -> list[dict]:
        positions = self._client.get_all_positions()
        return [self._format_position(p) for p in positions]

    def _sync_get_position(self, symbol: str) -> Optional[dict]:
        try:
            p = self._client.get_open_position(symbol)
            return self._format_position(p)
        except Exception:
            return None

    def _sync_submit_market_order(
        self, symbol: str, qty: float, side: str
    ) -> str:
        order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
        request = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=order_side,
            time_in_force=TimeInForce.DAY,
        )
        order = self._client.submit_order(request)
        return str(order.id)

    def _sync_get_order(self, alpaca_order_id: str) -> dict:
        order = self._client.get_order_by_id(alpaca_order_id)
        filled_at = None
        if order.filled_at:
            filled_at = order.filled_at
            if filled_at.tzinfo is None:
                filled_at = filled_at.replace(tzinfo=timezone.utc)
        status = order.status.value if hasattr(order.status, "value") else str(order.status)
        return {
            "id": str(order.id),
            "status": status,
            "filled_qty": float(order.filled_qty) if order.filled_qty else None,
            "filled_avg_price": float(order.filled_avg_price) if order.filled_avg_price else None,
            "filled_at": filled_at,
        }

    def _sync_close_position(self, symbol: str) -> Optional[str]:
        try:
            order = self._client.close_position(symbol)
            return str(order.id) if order else None
        except Exception as e:
            logger.warning("Failed to close position %s: %s", symbol, e)
            return None

    @staticmethod
    def _format_position(p) -> dict:
        return {
            "symbol": p.symbol,
            "qty": float(p.qty),
            "side": str(p.side),
            "avg_entry_price": float(p.avg_entry_price),
            "current_price": float(p.current_price),
            "unrealized_pl": float(p.unrealized_pl),
            "market_value": float(p.market_value),
        }
