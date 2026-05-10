from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Set

from shared.models import TradingSignal
from shared.runtime_config import RuntimeConfig


@dataclass
class _HysteresisState:
    """Per-symbol debounce state for SignalAggregator.

    Lives in-memory on the SignalAggregator instance. Resets to defaults
    on execution_service restart — see the design doc's 'restart-flap'
    risk note. Allocated lazily on first prediction message per symbol.
    """
    committed_direction: int = 0
    candidate_direction: Optional[int] = None
    candidate_streak: int = 0


class SignalAggregator:
    def __init__(
        self,
        *,
        runtime_config: RuntimeConfig,
        overrides: Optional[dict] = None,
    ):
        self._rc = runtime_config
        self._overrides = overrides or {}
        self._disabled_model_ids: Set[int] = set()
        self._symbol_states: dict[str, _HysteresisState] = {}

    def update_disabled_models(self, disabled_ids: Set[int]) -> None:
        self._disabled_model_ids = disabled_ids

    async def _param(self, key: str):
        v = self._overrides.get(key)
        if v is not None:
            return v
        return await self._rc.get(f"execution_{key}")

    async def _apply_hysteresis(
        self, symbol: str, raw_direction: int,
    ) -> Optional[int]:
        """Debounce raw direction through per-symbol streak state.

        Returns the direction to emit, or None to suppress this tick.
        With hysteresis_bars <= 0, returns raw_direction unchanged and
        does not allocate or touch any per-symbol state.
        """
        hysteresis_bars = await self._param("hysteresis_bars")
        if hysteresis_bars <= 0:
            return raw_direction

        state = self._symbol_states.setdefault(symbol, _HysteresisState())

        if raw_direction == state.committed_direction:
            # Raw matches what we're already emitting — clear any pending
            # change and emit committed. This is the reset path that
            # makes a single contradictory bar followed by a return to
            # the committed direction collapse to a no-op.
            state.candidate_direction = None
            state.candidate_streak = 0
            return state.committed_direction

        if raw_direction == state.candidate_direction:
            state.candidate_streak += 1
        else:
            state.candidate_direction = raw_direction
            state.candidate_streak = 1

        if state.candidate_streak >= hysteresis_bars:
            state.committed_direction = state.candidate_direction
            state.candidate_direction = None
            state.candidate_streak = 0
            return state.committed_direction

        return None  # streak still building — silent hold

    async def aggregate(
        self,
        symbol: str,
        timestamp: datetime,
        predictions: list[dict],
    ) -> Optional[TradingSignal]:
        if not predictions:
            return None

        active_preds = [
            p for p in predictions
            if p.get("model_id") not in self._disabled_model_ids
        ]

        min_confidence = await self._param("min_confidence")
        confident = [
            p for p in active_preds if p["confidence"] >= min_confidence
        ]
        min_agreement = await self._param("min_agreement")
        if len(confident) < min_agreement:
            return None

        weighted_sum = sum(
            p["direction"] * p["confidence"] for p in confident
        )
        total_weight = sum(p["confidence"] for p in confident)
        avg = weighted_sum / total_weight

        band = await self._param("direction_band")
        if avg > band:
            raw_direction = 1
        elif avg < -band:
            raw_direction = -1
        else:
            raw_direction = 0

        committed = await self._apply_hysteresis(symbol, raw_direction)
        if committed is None:
            return None
        direction = committed

        strength = min(abs(avg), 1.0)

        return TradingSignal(
            symbol=symbol,
            timestamp=timestamp,
            direction=direction,
            strength=strength,
            contributing_predictions=len(confident),
        )
