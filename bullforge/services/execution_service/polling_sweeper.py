from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from shared.db import Database
from services.execution_service.alpaca_broker import AlpacaBroker
from services.execution_service.reconciler_core import ReconcilerCore

logger = logging.getLogger(__name__)


class PollingSweeper:
    def __init__(
        self,
        db: Database,
        broker: AlpacaBroker,
        core: ReconcilerCore,
        interval_s: int = 60,
        orphan_every: int = 5,
        orphan_window_hours: int = 48,
    ):
        self._db = db
        self._broker = broker
        self._core = core
        self._interval_s = interval_s
        self._orphan_every = orphan_every
        self._orphan_window = timedelta(hours=orphan_window_hours)
        self._tick_count = 0
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.sweep_now()
            except Exception:
                logger.exception("PollingSweeper tick failed")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._interval_s,
                )
            except asyncio.TimeoutError:
                pass

    async def sweep_now(self) -> None:
        self._tick_count += 1
        await self._reconcile_non_terminal()
        if self._tick_count % self._orphan_every == 0:
            await self._scan_orphans()

    async def _reconcile_non_terminal(self) -> None:
        orders = await self._db.query_non_terminal_orders(
            within=self._orphan_window,
        )
        for o in orders:
            try:
                snap = await self._broker.get_order(o.alpaca_order_id)
            except Exception as e:
                logger.warning(
                    "sweeper: get_order failed for %s: %s",
                    o.alpaca_order_id, e,
                )
                continue
            try:
                await self._core.apply_alpaca_order(snap, source="poll")
            except Exception:
                logger.exception(
                    "sweeper: apply_alpaca_order failed for %s",
                    o.alpaca_order_id,
                )

    async def _scan_orphans(self) -> None:
        after = datetime.now(timezone.utc) - self._orphan_window
        try:
            snapshots = await self._broker.list_open_orders(after=after)
        except Exception:
            logger.exception("sweeper: list_open_orders failed")
            return
        for snap in snapshots:
            alpaca_id = snap["id"]
            existing = await self._db.get_order_by_alpaca_id(alpaca_id)
            if existing is not None:
                continue
            try:
                await self._core.apply_alpaca_order(snap, source="poll")
            except Exception:
                logger.exception(
                    "sweeper: orphan ingest failed for %s", alpaca_id,
                )
