from __future__ import annotations

import logging
from typing import Optional, Tuple

from shared.models import TradingSignal
from shared.runtime_config import RuntimeConfig
from shared.trading_window import is_trading_allowed

logger = logging.getLogger(__name__)


class RiskManager:
    def __init__(
        self,
        max_position_value: float = 10000.0,
        max_total_positions: int = 5,
        max_portfolio_exposure: float = 0.5,
        min_signal_strength: float = 0.5,
        runtime_config: Optional[RuntimeConfig] = None,
    ):
        self._max_position_value = max_position_value
        self._max_total_positions = max_total_positions
        self._max_portfolio_exposure = max_portfolio_exposure
        self._min_signal_strength = min_signal_strength
        self._rc = runtime_config

    async def check_entry(
        self,
        signal: TradingSignal,
        account: dict,
        positions: list[dict],
        current_price: float,
    ) -> Tuple[bool, str, int]:
        """
        Check whether a new position entry is allowed.
        Returns (approved, reason, quantity).
        """
        if self._rc is not None:
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

        if signal.strength < self._min_signal_strength:
            return False, f"Signal too weak ({signal.strength:.2f})", 0

        for pos in positions:
            if pos["symbol"] == signal.symbol:
                return False, "Already have position in this symbol", 0

        if len(positions) >= self._max_total_positions:
            return False, "Max positions reached", 0

        total_exposure = sum(abs(p["market_value"]) for p in positions)
        equity = account["equity"]
        if equity > 0 and total_exposure / equity >= self._max_portfolio_exposure:
            return False, "Max portfolio exposure reached", 0

        qty = int(self._max_position_value / current_price)
        if qty <= 0:
            return False, "Position value too small for one share", 0

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
