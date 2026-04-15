from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FillRequest:
    side: str  # "buy" or "sell"
    qty: float


@dataclass(frozen=True)
class FillResult:
    filled: bool
    qty: float
    price: float
    reason: str = ""


class FillModel:
    """Fills an order at the *next* bar's open with slippage and a volume cap.

    Why next-bar open: the signal is computed from a closed bar at time T;
    submitting against that same bar's close would peek at unrealized data.
    """

    def __init__(self, *, slippage_bps: float, max_volume_pct: float):
        if slippage_bps < 0:
            raise ValueError("slippage_bps must be >= 0")
        if not (0.0 < max_volume_pct <= 1.0):
            raise ValueError("max_volume_pct must be in (0, 1]")
        self._slip = slippage_bps / 10_000.0
        self._max_vol_pct = max_volume_pct

    def fill(self, req: FillRequest, next_bar: dict) -> FillResult:
        max_qty = next_bar["volume"] * self._max_vol_pct
        if req.qty > max_qty:
            return FillResult(
                filled=False,
                qty=0,
                price=0.0,
                reason=f"qty {req.qty} > volume cap {max_qty:.0f}",
            )
        sign = 1 if req.side == "buy" else -1
        price = next_bar["open"] * (1.0 + sign * self._slip)
        return FillResult(filled=True, qty=req.qty, price=price)
