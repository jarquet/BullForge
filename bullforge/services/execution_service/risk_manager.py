from __future__ import annotations

import logging
import math
from typing import Optional, Tuple

from shared.models import TradingSignal
from shared.runtime_config import RuntimeConfig
from shared.symbol_utils import is_crypto
from shared.trading_window import is_trading_allowed

logger = logging.getLogger(__name__)


def broker_feasible_qty(symbol: str, direction: int, qty: float) -> float:
    """Round a sized quantity to something Alpaca will accept.

    Alpaca rejects fractional short sales on equities
    (``{"code":42210000,"message":"fractional orders cannot be sold short"}``),
    and notional-based sizing (equity * pct / price) is almost always
    fractional. Equity shorts are therefore floored to whole shares; a
    result of 0 means the short cannot be placed at this size. Longs and
    crypto keep their fractional quantity.
    """
    if direction == -1 and not is_crypto(symbol):
        # The epsilon keeps 4.9999999999 (float noise) from flooring to 4.
        return float(math.floor(qty + 1e-9))
    return qty


class RiskManager:
    def __init__(
        self,
        *,
        runtime_config: RuntimeConfig,
        overrides: Optional[dict] = None,
    ):
        if runtime_config is None:
            raise ValueError(
                "RiskManager requires a RuntimeConfig; the equity "
                "trading-window gate depends on it and must not be bypassed.",
            )
        self._rc = runtime_config
        self._overrides = overrides or {}

    async def _param(self, key: str):
        v = self._overrides.get(key)
        if v is not None:
            return v
        return await self._rc.get(f"execution_{key}")

    async def check_entry(
        self,
        signal: TradingSignal,
        account: dict,
        positions: list[dict],
        current_price: float,
    ) -> Tuple[bool, str, float]:
        """
        Check whether a new position entry is allowed.
        Returns (approved, reason, quantity).
        """
        open_buf = int(await self._rc.get("equity_window_open_buffer_minutes"))
        close_buf = int(await self._rc.get("equity_window_close_buffer_minutes"))
        allowed, reason = is_trading_allowed(
            signal.symbol, signal.timestamp,
            open_buffer_min=open_buf, close_buffer_min=close_buf,
        )
        if not allowed:
            return False, f"trading window: {reason}", 0

        if signal.direction == 0:
            return False, "Flat signal, no entry", 0

        # Alpaca crypto is long-only on both paper and live: a sell
        # without inventory bounces with code 40310000. Reject shorts
        # here so the order never reaches the broker.
        if signal.direction == -1 and is_crypto(signal.symbol):
            return False, "crypto is long-only (Alpaca rejects shorts)", 0

        min_strength = await self._param("min_signal_strength")
        if signal.strength < min_strength:
            return False, f"Signal too weak ({signal.strength:.2f})", 0

        for pos in positions:
            if pos["symbol"] == signal.symbol:
                return False, "Already have position in this symbol", 0

        max_positions = await self._param("max_positions")
        if len(positions) >= max_positions:
            return False, "Max positions reached", 0

        max_exposure = await self._param("max_exposure")
        total_exposure = sum(abs(p["market_value"]) for p in positions)
        equity = account["equity"]
        if equity > 0 and total_exposure / equity >= max_exposure:
            return False, "Max portfolio exposure reached", 0

        max_position_pct = await self._param("max_position_pct")
        qty = (equity * max_position_pct) / current_price
        qty = broker_feasible_qty(signal.symbol, signal.direction, qty)
        if qty <= 0:
            return False, "Short below one whole share (Alpaca rejects fractional shorts)", 0
        if qty * current_price < 1.0:
            # Alpaca rejects orders below $1 notional on both crypto and
            # fractional-equity paths. Guarding here avoids a wasted submit.
            return False, "Position value too small (below $1 notional)", 0

        return True, "Approved", qty

    def check_exit(
        self,
        signal: TradingSignal,
        position: dict,
    ) -> Tuple[bool, str]:
        """
        Check whether an existing position should be closed.
        Returns (should_exit, reason).
        """
        pos_qty = float(position["qty"])
        pos_direction = 1 if pos_qty > 0 else -1

        if signal.direction == -pos_direction:
            return True, "Opposing signal"

        if signal.direction == 0:
            return True, "Flat signal"

        return False, "Signal matches position"
