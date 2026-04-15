from __future__ import annotations

import asyncio
import logging
from datetime import timezone
from typing import Awaitable, Callable, Optional

from alpaca.trading.stream import TradingStream

from shared.config import Settings
from services.execution_service.reconciler_core import ReconcilerCore

logger = logging.getLogger(__name__)


def normalize_trade_update(event) -> dict:
    o = event.order
    if o is None:
        raise ValueError("trade update event missing order")

    def _enum(v):
        return v.value if hasattr(v, "value") else str(v)

    filled_at = getattr(o, "filled_at", None)
    if filled_at is not None and filled_at.tzinfo is None:
        filled_at = filled_at.replace(tzinfo=timezone.utc)
    submitted_at = getattr(o, "submitted_at", None)
    if submitted_at is not None and submitted_at.tzinfo is None:
        submitted_at = submitted_at.replace(tzinfo=timezone.utc)

    return {
        "id": str(o.id),
        "symbol": o.symbol,
        "side": _enum(o.side),
        "qty": float(o.qty),
        "order_type": _enum(o.order_type),
        "status": _enum(o.status),
        "submitted_at": submitted_at,
        "filled_qty": float(o.filled_qty) if getattr(o, "filled_qty", None) else None,
        "filled_avg_price": (
            float(o.filled_avg_price)
            if getattr(o, "filled_avg_price", None) else None
        ),
        "filled_at": filled_at,
    }


class TradeUpdatesStream:
    def __init__(
        self,
        settings: Settings,
        core: ReconcilerCore,
        on_reconnect: Callable[[], Awaitable[None]],
        max_backoff_s: int = 30,
    ):
        self._settings = settings
        self._core = core
        self._on_reconnect = on_reconnect
        self._max_backoff_s = max_backoff_s
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    def _backoff_delay(self, attempt: int) -> int:
        return min(2 ** attempt, self._max_backoff_s)

    async def _handle_event(self, event) -> None:
        try:
            snap = normalize_trade_update(event)
        except Exception:
            logger.exception("malformed trade update event")
            return
        try:
            await self._core.apply_alpaca_order(snap, source="stream")
        except Exception:
            logger.exception("stream: apply_alpaca_order failed")

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        attempt = 0
        while not self._stop_event.is_set():
            client = TradingStream(
                api_key=self._settings.alpaca_api_key,
                secret_key=self._settings.alpaca_secret_key,
                paper=True,
            )
            client.subscribe_trade_updates(self._handle_event)
            try:
                if attempt > 0:
                    try:
                        await self._on_reconnect()
                    except Exception:
                        logger.exception("on_reconnect callback failed")
                attempt = 0
                await asyncio.to_thread(client.run)
            except Exception:
                logger.exception("trade updates stream dropped")
            if self._stop_event.is_set():
                return
            delay = self._backoff_delay(attempt)
            attempt += 1
            logger.info("reconnecting trade stream in %ds", delay)
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=delay,
                )
            except asyncio.TimeoutError:
                pass
