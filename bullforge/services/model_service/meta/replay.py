from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

from shared.db import Database
from shared.models import MetaFeatureRow
from services.model_service.meta.feature_assembler import MetaFeatureAssembler
from services.model_service.meta.regime import RegimeBuilder


@dataclass(frozen=True)
class SimResult:
    hit_target: bool
    hit_stop: bool
    exit_idx: int
    realized_return: float


class PairSimulator:
    """Walks bars forward from `entry_idx`, returns first-touch outcome.

    Stop wins on same-bar collisions (conservative; matches typical
    intra-bar stop-fill behavior). Returns idealized fills (±target_pct /
    −stop_pct) regardless of intra-bar gap-through; gap-through slippage
    is not modeled.
    """

    def simulate(
        self,
        bars: pd.DataFrame,
        *,
        entry_idx: int,
        direction: int,
        target_pct: float,
        stop_pct: float,
        max_hold_bars: int,
    ) -> SimResult:
        if direction not in (1, -1):
            raise ValueError(
                f"direction must be 1 (long) or -1 (short); got {direction!r}"
            )

        if entry_idx + 1 >= len(bars):
            return SimResult(False, False, entry_idx, 0.0)

        entry_price = float(bars.iloc[entry_idx]["close"])
        if direction == 1:
            target_price = entry_price * (1.0 + target_pct)
            stop_price = entry_price * (1.0 - stop_pct)
        elif direction == -1:
            target_price = entry_price * (1.0 - target_pct)
            stop_price = entry_price * (1.0 + stop_pct)

        last_idx = min(entry_idx + max_hold_bars, len(bars) - 1)
        for i in range(entry_idx + 1, last_idx + 1):
            high = float(bars.iloc[i]["high"])
            low = float(bars.iloc[i]["low"])

            if direction == 1:
                hit_stop = low <= stop_price
                hit_target = high >= target_price
            else:
                hit_stop = high >= stop_price
                hit_target = low <= target_price

            if hit_stop:
                return SimResult(False, True, i, -stop_pct)
            if hit_target:
                return SimResult(True, False, i, target_pct)

        # Timeout: exit at last considered bar's close
        exit_close = float(bars.iloc[last_idx]["close"])
        realized = ((exit_close - entry_price) / entry_price) * direction
        return SimResult(False, False, last_idx, float(realized))


def compute_atr_pct(
    bars: pd.DataFrame, *, idx: int, period: int = 14,
) -> float:
    """ATR-period at row `idx`, expressed as fraction of that row's close.

    Returns NaN if insufficient prior bars exist for the rolling mean.
    """
    if idx < period:
        return float("nan")
    sub = bars.iloc[: idx + 1]
    high = sub["high"]
    low = sub["low"]
    close = sub["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean().iloc[-1]
    if pd.isna(atr) or close.iloc[-1] == 0:
        return float("nan")
    return float(atr / close.iloc[-1])


SlotKey = tuple[str, int, str]  # (timeframe, horizon_minutes, model_type)


async def _rolling_stats(
    db: Database,
    *,
    symbol: str,
    slot_to_active_id: dict[SlotKey, int],
    limit: int = 200,
) -> dict[int, dict]:
    """Aggregate prediction outcomes per slot (timeframe, horizon,
    model_type), keyed by the slot's currently-active model_id.

    Without slot-level aggregation, a freshly-retrained base model has
    zero outcomes the moment it activates (its predictions haven't been
    written yet) — so every retrain night would reset the meta features'
    rolling-accuracy signal to zero. Aggregating across every model_id
    the symbol has had at this slot preserves the history.
    """
    out: dict[int, dict] = {}
    for slot, active_id in slot_to_active_id.items():
        timeframe, horizon, model_type = slot
        outcomes = await db.query_prediction_outcomes_for_slot(
            symbol=symbol, timeframe=timeframe,
            horizon_minutes=horizon, model_type=model_type, limit=limit,
        )
        if outcomes:
            correct = sum(1 for o in outcomes if o["is_correct"])
            acc = correct / len(outcomes)
            avg_conf = float(np.mean([o["confidence"] for o in outcomes]))
            cal_err = abs(avg_conf - acc)
        else:
            acc, cal_err = 0.0, 0.0
        out[active_id] = {"accuracy": acc, "calibration_error": cal_err,
                          "is_disabled": False}
    return out


def _ev_for_pair(sim_result, target_pct: float, stop_pct: float) -> float:
    if sim_result.hit_target:
        return target_pct
    if sim_result.hit_stop:
        return -stop_pct
    return float(sim_result.realized_return)


def _kelly_fraction(
    history: list[bool], target_pct: float, stop_pct: float,
    min_history: int = 200, cold_start: float = 0.05, cap: float = 0.25,
) -> float:
    if len(history) < min_history:
        return cold_start
    p = sum(1 for h in history if h) / len(history)
    if stop_pct == 0:
        return cold_start
    b = target_pct / stop_pct
    raw = (p * b - (1 - p)) / b if b else 0.0
    return float(max(0.0, min(raw, cap)))


class CandidateBuilder:
    def __init__(self, db: Database, *, symbol: str):
        self._db = db
        self._symbol = symbol

    async def fetch(self, *, since: datetime, until: datetime):
        bar_recs = await self._db.query_bars_between(
            self._symbol, "1m", start=since, end=until,
        )
        bars = pd.DataFrame(bar_recs)
        feature_rows = await self._db.query_feature_rows(
            self._symbol, "1m", limit=200000,
        )
        features = {fr.timestamp: fr.features for fr in feature_rows
                    if since <= fr.timestamp <= until}
        pred_rows = await self._db.query_predictions_in_range(
            symbol=self._symbol, timeframe="1m", start=since, end=until,
        )
        preds_by_ts: dict[datetime, list[dict]] = {}
        for p in pred_rows:
            preds_by_ts.setdefault(p["timestamp"], []).append(p)
        return bars, features, preds_by_ts


async def build_replay_rows(
    *,
    db: Database,
    symbol: str,
    direction: int,
    as_of: datetime,
    window_days: int,
    pair_grid: list[list[float]],
    max_hold_bars: int,
    regime_builder_factory: Callable[[], RegimeBuilder],
    cold_start_kelly: float = 0.05,
) -> list[MetaFeatureRow]:
    since = as_of - timedelta(days=window_days)
    cb = CandidateBuilder(db, symbol=symbol)
    bars, features, preds_by_ts = await cb.fetch(since=since, until=as_of)
    if bars.empty:
        return []

    active_models = await db.query_all_active_models()
    # Meta replay walks 1m bars and feeds the assembler features keyed off
    # 1m base predictions only. Including 1d (or other-timeframe) base
    # models in the slot list inflates the trained meta's feature count
    # to a shape that 1m-only inference can never reproduce — symptom was
    # `LightGBMError: data has 74 features but training had 194`. Keep the
    # slot list aligned with the per-bar inference path.
    base_models = [m for m in active_models
                   if m.symbol == symbol
                   and m.timeframe == "1m"
                   and m.model_type not in ("meta_entry", "meta_exec")]
    if not base_models:
        return []

    # Build the slot → active model_id map. A "slot" is the logical model
    # identity that survives retrains: same (timeframe, horizon, model_type)
    # → same slot, even with a fresh model_id every retrain night.
    slot_to_active_id: dict[SlotKey, int] = {
        (m.timeframe, m.horizon_minutes, m.model_type): m.id for m in base_models
    }
    # Resolve every prediction's model_id (which may be from a now-inactive
    # prior version of the same slot) to the slot's current active id, so
    # the assembler — which still keys by model_id — sees a coherent
    # picture. Predictions whose slot no longer has an active model are
    # dropped; predictions in slots with multiple historical model_ids per
    # timestamp are deduplicated, keeping the most recent.
    all_models = await db.query_models_for_symbol(symbol, include_inactive=True)
    mid_to_slot: dict[int, SlotKey] = {
        m.id: (m.timeframe, m.horizon_minutes, m.model_type) for m in all_models
    }
    for ts, preds in list(preds_by_ts.items()):
        rewritten: list[dict] = []
        seen_slots: set[SlotKey] = set()
        for p in preds:
            slot = mid_to_slot.get(p["model_id"])
            if slot is None or slot not in slot_to_active_id:
                continue
            if slot in seen_slots:
                continue
            seen_slots.add(slot)
            p_new = dict(p)
            p_new["model_id"] = slot_to_active_id[slot]
            rewritten.append(p_new)
        preds_by_ts[ts] = rewritten

    model_ids = [m.id for m in base_models]
    rolling = await _rolling_stats(
        db, symbol=symbol, slot_to_active_id=slot_to_active_id,
    )
    assembler = MetaFeatureAssembler(model_ids=model_ids)
    rb = regime_builder_factory()
    sim = PairSimulator()

    rows: list[MetaFeatureRow] = []
    pair_history: dict[int, list[bool]] = {i: [] for i in range(len(pair_grid))}

    for i in range(len(bars)):
        ts_raw = bars.iloc[i]["timestamp"]
        ts = pd.Timestamp(ts_raw).to_pydatetime()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts not in features or not preds_by_ts.get(ts):
            continue
        atr_pct = compute_atr_pct(bars, idx=i, period=14)
        if pd.isna(atr_pct) or atr_pct <= 0:
            continue

        per_pair = []
        for tm, sm in pair_grid:
            tp, sp = float(tm) * atr_pct, float(sm) * atr_pct
            res = sim.simulate(bars, entry_idx=i, direction=direction,
                               target_pct=tp, stop_pct=sp,
                               max_hold_bars=max_hold_bars)
            per_pair.append((res, tp, sp, _ev_for_pair(res, tp, sp)))

        entry_label = 1 if any(p[0].hit_target for p in per_pair) else 0
        opt_idx = max(range(len(per_pair)), key=lambda k: per_pair[k][3])
        opt_target_pct, opt_stop_pct = per_pair[opt_idx][1], per_pair[opt_idx][2]
        kelly = _kelly_fraction(
            pair_history[opt_idx], opt_target_pct, opt_stop_pct,
            cold_start=cold_start_kelly,
        )
        for k, (r, _, _, _) in enumerate(per_pair):
            pair_history[k].append(r.hit_target)

        symbol_state = {
            "realized_vol": features[ts].get("realized_vol", 0.02),
            "atr_price_ratio": atr_pct,
            "dist_sma_50": features[ts].get("dist_sma_50", 0.0),
            "dist_sma_200bar": features[ts].get("dist_sma_200", 0.0),
            "dist_sma_200day": features[ts].get("dist_sma_200d", 0.0),
        }
        regime = rb.regime_at(ts)
        # Normalize prediction dicts to the assembler's expected shape:
        # MetaFeatureAssembler reads p["direction"], but DB rows use
        # "predicted_direction" (matches the predictions table column).
        norm_preds = [
            {**p, "direction": p.get("direction", p.get("predicted_direction", 0))}
            for p in preds_by_ts[ts]
        ]
        vec = assembler.assemble(
            predictions=norm_preds,
            rolling_stats=rolling, regime=regime,
            symbol_state=symbol_state, timestamp=ts,
        )
        # Stash kelly into the feature_vector for the trainer to pick up
        vec["_kelly_label"] = kelly

        rows.append(MetaFeatureRow(
            symbol=symbol, timestamp=ts, direction=direction,
            feature_vector=vec, source="replay",
            entry_label=entry_label, optimal_pair_idx=opt_idx,
            realized_ev=per_pair[opt_idx][3],
        ))
    return rows
