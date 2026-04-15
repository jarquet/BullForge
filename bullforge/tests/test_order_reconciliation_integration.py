"""Integration tests for order reconciliation: late cancellation + orphan ingest.

Path B: uses an in-memory FakeDatabase (no Docker / real DB needed).
"""
from __future__ import annotations

import dataclasses
import pytest
from datetime import datetime, timezone
from typing import Dict, List, Optional
from unittest.mock import AsyncMock

from shared.db import UpsertResult, _normalize_alpaca_status
from shared.models import Order, Trade
from services.execution_service.reconciler_core import ReconcilerCore
from services.execution_service.polling_sweeper import PollingSweeper


# ---------------------------------------------------------------------------
# In-memory fake database
# ---------------------------------------------------------------------------

class FakeDatabase:
    """Minimal in-memory Database substitute used by ReconcilerCore + PollingSweeper."""

    def __init__(self):
        # keyed by alpaca_order_id
        self._orders: Dict[str, Order] = {}
        self._next_id = 1
        self._trades: List[Trade] = []
        self._next_trade_id = 1

    # -- orders ----------------------------------------------------------------

    async def insert_order(self, order: Order) -> int:
        oid = self._next_id
        self._next_id += 1
        stored = dataclasses.replace(order, id=oid)
        if stored.alpaca_order_id:
            self._orders[stored.alpaca_order_id] = stored
        return oid

    async def get_order_by_alpaca_id(self, alpaca_order_id: str) -> Optional[Order]:
        return self._orders.get(alpaca_order_id)

    async def query_non_terminal_orders(self, within) -> List[Order]:
        terminal = {"filled", "cancelled", "rejected", "expired"}
        return [
            o for o in self._orders.values()
            if o.status not in terminal and o.alpaca_order_id is not None
        ]

    async def upsert_order_from_alpaca(
        self, snapshot: dict, source: str,
    ) -> UpsertResult:
        alpaca_id = snapshot["id"]
        canonical_status = _normalize_alpaca_status(snapshot["status"])
        now_ts = datetime.now(timezone.utc)

        existing = self._orders.get(alpaca_id)
        if existing is None:
            # Insert as external
            oid = self._next_id
            self._next_id += 1
            order = Order(
                id=oid,
                symbol=snapshot["symbol"],
                side=snapshot["side"],
                quantity=float(snapshot["qty"]),
                order_type=snapshot.get("order_type", "market"),
                submitted_at=snapshot.get("submitted_at") or now_ts,
                signal_strength=0.0,
                alpaca_order_id=alpaca_id,
                status=canonical_status,
                filled_at=snapshot.get("filled_at"),
                filled_price=snapshot.get("filled_avg_price"),
                filled_quantity=snapshot.get("filled_qty"),
                origin="external",
                last_reconciled_at=now_ts,
                reconcile_source=source,
            )
            self._orders[alpaca_id] = order
            return UpsertResult(
                order=order, old_status=None,
                new_status=canonical_status, inserted=True,
            )

        old_status = existing.status
        updated = dataclasses.replace(
            existing,
            status=canonical_status,
            filled_at=snapshot.get("filled_at") or existing.filled_at,
            filled_price=snapshot.get("filled_avg_price") or existing.filled_price,
            filled_quantity=snapshot.get("filled_qty") or existing.filled_quantity,
            last_reconciled_at=now_ts,
            reconcile_source=source,
        )
        self._orders[alpaca_id] = updated
        return UpsertResult(
            order=updated, old_status=old_status,
            new_status=canonical_status, inserted=False,
        )

    # -- trades ----------------------------------------------------------------

    async def insert_trade(self, trade: Trade) -> int:
        tid = self._next_trade_id
        self._next_trade_id += 1
        stored = dataclasses.replace(trade, id=tid)
        self._trades.append(stored)
        return tid

    async def update_trade_exit(self, trade_id: int, **kwargs) -> None:
        for i, t in enumerate(self._trades):
            if t.id == trade_id:
                self._trades[i] = dataclasses.replace(t, **kwargs)
                return

    async def get_open_trade(self, symbol: str) -> Optional[Trade]:
        for t in reversed(self._trades):
            if t.symbol == symbol and getattr(t, "exit_time", None) is None:
                return t
        return None


# ---------------------------------------------------------------------------
# Fake broker
# ---------------------------------------------------------------------------

class FakeBroker:
    def __init__(self):
        self._state: Dict[str, dict] = {}

    def set_status(self, alpaca_id: str, status: str, **fields):
        snap = self._state.get(alpaca_id, {
            "id": alpaca_id, "symbol": "SPY", "side": "buy", "qty": 1.0,
            "order_type": "market",
            "submitted_at": datetime.now(timezone.utc),
        })
        snap["status"] = status
        snap.update(fields)
        self._state[alpaca_id] = snap

    async def get_order(self, alpaca_id: str) -> dict:
        return dict(self._state[alpaca_id])

    async def list_open_orders(self, after) -> List[dict]:
        terminal = {"filled", "cancelled", "rejected", "expired"}
        return [
            dict(s) for s in self._state.values()
            if s["status"] not in terminal
        ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_late_cancellation_is_reconciled():
    """An order stuck at status='submitted' is reconciled to 'cancelled' after sweep."""
    db = FakeDatabase()
    alpaca_id = "alp-late-cancel"

    stale = Order(
        symbol="SPY", side="buy", quantity=1.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
        alpaca_order_id=alpaca_id, status="submitted",
    )
    await db.insert_order(stale)

    broker = FakeBroker()
    broker.set_status(alpaca_id, "cancelled")

    redis_mock = AsyncMock()
    core = ReconcilerCore(db=db, redis_client=redis_mock)
    sweeper = PollingSweeper(
        db=db, broker=broker, core=core,
        interval_s=60, orphan_every=5, orphan_window_hours=48,
    )
    await sweeper.sweep_now()

    reconciled = await db.get_order_by_alpaca_id(alpaca_id)
    assert reconciled is not None
    assert reconciled.status == "cancelled"
    assert reconciled.reconcile_source == "poll"
    assert reconciled.last_reconciled_at is not None


@pytest.mark.asyncio
async def test_orphan_external_order_is_ingested():
    """An Alpaca-only order (unknown to DB) is ingested with origin='external'."""
    db = FakeDatabase()
    alpaca_id = "alp-external"

    broker = FakeBroker()
    broker.set_status(alpaca_id, "new")

    redis_mock = AsyncMock()
    core = ReconcilerCore(db=db, redis_client=redis_mock)
    sweeper = PollingSweeper(
        db=db, broker=broker, core=core,
        interval_s=60, orphan_every=1, orphan_window_hours=48,
    )
    await sweeper.sweep_now()

    ingested = await db.get_order_by_alpaca_id(alpaca_id)
    assert ingested is not None
    assert ingested.origin == "external"
    # Alpaca "new" normalizes to our canonical "submitted"
    assert ingested.status == "submitted"
