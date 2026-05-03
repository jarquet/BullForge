from __future__ import annotations

from dataclasses import dataclass

from shared.symbol_utils import is_crypto


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

    The volume cap is asset-class aware: equities use ``max_volume_pct`` and
    crypto pairs (slash-in-symbol convention, e.g. ``BTC/USD``) use
    ``max_volume_pct_crypto``. Defaults are 0.10 for equities (deep multi-venue
    tape) and 0.25 for crypto (single-venue, thinner tape, takers not makers).
    See ``bullforge/docs/research/crypto-bar-volume-units.md`` for the
    rationale and units analysis (crypto bar ``volume`` is base-asset
    quantity, dimensionally consistent with order qty).
    """

    def __init__(
        self,
        *,
        slippage_bps: float,
        max_volume_pct: float,
        max_volume_pct_crypto: float = 0.25,
    ):
        if slippage_bps < 0:
            raise ValueError("slippage_bps must be >= 0")
        if not (0.0 < max_volume_pct <= 1.0):
            raise ValueError("max_volume_pct must be in (0, 1]")
        if not (0.0 < max_volume_pct_crypto <= 1.0):
            raise ValueError("max_volume_pct_crypto must be in (0, 1]")
        self._slip = slippage_bps / 10_000.0
        self._max_vol_pct_equity = max_volume_pct
        self._max_vol_pct_crypto = max_volume_pct_crypto

    def cap_for(self, symbol: str) -> float:
        """Return the volume-cap fraction for ``symbol`` based on asset class."""
        return (
            self._max_vol_pct_crypto
            if is_crypto(symbol)
            else self._max_vol_pct_equity
        )

    def fill(
        self, req: FillRequest, next_bar: dict, *, symbol: str,
    ) -> FillResult:
        pct = self.cap_for(symbol)
        max_qty = next_bar["volume"] * pct
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
