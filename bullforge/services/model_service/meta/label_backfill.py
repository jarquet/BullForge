from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from shared.db import Database
from services.model_service.meta.replay import (
    PairSimulator,
    compute_atr_pct,
    _ev_for_pair,
)

logger = logging.getLogger(__name__)


class MetaLabelBackfiller:
    """Periodic loop: fill entry_label / optimal_pair_idx / realized_ev on
    live `meta_features` rows once enough bars have elapsed.

    Idempotent — only operates on rows where entry_label IS NULL.
    """

    def __init__(
        self,
        *,
        db: Database,
        max_hold_minutes: int,
        pair_grid: list[list[float]],
        safety_buffer_minutes: int = 5,
        interval_seconds: float = 900.0,  # 15 min default
    ):
        self._db = db
        self._max_hold_min = max_hold_minutes
        self._pair_grid = pair_grid
        self._buffer_min = safety_buffer_minutes
        self._interval_s = interval_seconds
        self._stop = asyncio.Event()
        self._sim = PairSimulator()

    def stop(self) -> None:
        self._stop.set()

    async def run_forever(self) -> None:
        while not self._stop.is_set():
            try:
                await self.run_once(now=datetime.now(timezone.utc))
            except Exception:
                logger.exception("MetaLabelBackfiller tick failed")
            try:
                await asyncio.wait_for(self._stop.wait(), self._interval_s)
                return
            except asyncio.TimeoutError:
                continue

    async def run_once(self, *, now: datetime) -> int:
        cutoff = now - timedelta(minutes=self._max_hold_min + self._buffer_min)
        rows = await self._db.query_unlabeled_meta_features(older_than=cutoff)
        labeled = 0
        for row in rows:
            ok = await self._label_one(row=row, now=now)
            if ok:
                labeled += 1
        return labeled

    async def _label_one(self, *, row, now: datetime) -> bool:
        bar_recs = await self._db.query_bars_between(
            row.symbol, "1m",
            start=row.timestamp,
            end=row.timestamp + timedelta(minutes=self._max_hold_min + 1),
        )
        if not bar_recs or len(bar_recs) < 2:
            return False
        bars = pd.DataFrame(bar_recs)
        # entry is the bar at `row.timestamp`; locate its index
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
        try:
            entry_idx = bars.index[bars["timestamp"] == row.timestamp][0]
        except IndexError:
            return False
        atr_pct = compute_atr_pct(bars, idx=entry_idx, period=14)
        if pd.isna(atr_pct) or atr_pct <= 0:
            return False

        per_pair = []
        for tm, sm in self._pair_grid:
            tp, sp = float(tm) * atr_pct, float(sm) * atr_pct
            res = self._sim.simulate(
                bars, entry_idx=entry_idx, direction=row.direction,
                target_pct=tp, stop_pct=sp, max_hold_bars=self._max_hold_min,
            )
            per_pair.append((res, tp, sp, _ev_for_pair(res, tp, sp)))

        entry_label = 1 if any(p[0].hit_target for p in per_pair) else 0
        opt_idx = max(range(len(per_pair)), key=lambda k: per_pair[k][3])
        await self._db.update_meta_feature_labels(
            symbol=row.symbol, timestamp=row.timestamp,
            direction=row.direction, entry_label=entry_label,
            optimal_pair_idx=opt_idx, realized_ev=per_pair[opt_idx][3],
        )
        return True
