"""Regression tests for trade-pairing in the `trades` table.

Background: in live paper trading (2026-05-06..08) the `trades` table drifted
from the broker. Long-only symbols accumulated rows that never received an
exit, and short round-trips were paired one round-trip off (each new short's
entry linked to the previous round-trip's closing buy, giving negative hold
durations).

Root cause: order snapshots reach ReconcilerCore from three writers (the
submit path, the trade-updates stream, the polling sweeper) in no guaranteed
order. A stale "accepted" snapshot applied after "filled" regressed the order
row to "submitted"; the sweeper then re-polled it and the reconciler saw a
second submitted->filled transition and booked the same fill again. A double
entry left a phantom open row; a double exit opened a phantom row on the
other side that the next entry then "closed".

These tests replay those interleavings against a small in-memory ledger that
applies the same status rule as the SQL upsert (`merge_order_status`). The
SQL itself is covered by tests/integration/test_order_reconciliation_real_db.py.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from shared.db import UpsertResult, _normalize_alpaca_status, merge_order_status
from shared.models import Order, Trade
from services.execution_service.reconciler_core import ReconcilerCore

T0 = datetime(2026, 5, 6, 15, 30, tzinfo=timezone.utc)


class _Ledger:
    """In-memory stand-in for the `orders` and `trades` tables."""

    def __init__(self, *, guard_stale: bool = True):
        self.orders: dict[str, Order] = {}
        self.trades: list[Trade] = []
        self._guard_stale = guard_stale

    async def upsert_order_from_alpaca(self, snap: dict, source: str) -> UpsertResult:
        incoming = _normalize_alpaca_status(snap["status"])
        existing = self.orders.get(snap["id"])
        old = existing.status if existing else None
        status = merge_order_status(old, incoming) if self._guard_stale else incoming
        stale = status != incoming
        if existing is None:
            order = Order(
                id=len(self.orders) + 1, symbol=snap["symbol"], side=snap["side"],
                quantity=snap["qty"], order_type="market",
                submitted_at=snap["submitted_at"], signal_strength=0.0,
                alpaca_order_id=snap["id"], status=status,
                filled_at=snap.get("filled_at"),
                filled_price=snap.get("filled_avg_price"),
                filled_quantity=snap.get("filled_qty"),
            )
        elif stale:
            order = existing
        else:
            order = replace(
                existing, status=status,
                filled_at=snap.get("filled_at") or existing.filled_at,
                filled_price=snap.get("filled_avg_price") or existing.filled_price,
                filled_quantity=snap.get("filled_qty") or existing.filled_quantity,
            )
        self.orders[snap["id"]] = order
        return UpsertResult(
            order=order, old_status=old, new_status=order.status,
            inserted=existing is None,
        )

    async def get_open_trades(self, symbol: str) -> list[Trade]:
        rows = [t for t in self.trades if t.symbol == symbol and t.exit_time is None]
        return sorted(rows, key=lambda t: t.entry_time)

    async def insert_trade(self, trade: Trade) -> int:
        trade = replace(trade, id=len(self.trades) + 1)
        self.trades.append(trade)
        return trade.id

    async def update_trade_exit(
        self, trade_id, exit_time, exit_price, exit_order_id, pnl,
        hold_duration_seconds,
    ) -> None:
        for i, t in enumerate(self.trades):
            if t.id == trade_id:
                self.trades[i] = replace(
                    t, exit_time=exit_time, exit_price=exit_price,
                    exit_order_id=exit_order_id, pnl=pnl,
                    hold_duration_seconds=hold_duration_seconds,
                )

    def order_id(self, alpaca_id: str) -> int:
        return self.orders[alpaca_id].id


def _snap(alpaca_id, side, status, at, *, qty=5.0, price=400.0, symbol="MSFT"):
    filled = status == "filled"
    return {
        "id": alpaca_id, "symbol": symbol, "side": side, "qty": qty,
        "order_type": "market", "submitted_at": at, "status": status,
        "filled_qty": qty if filled else None,
        "filled_avg_price": price if filled else None,
        "filled_at": at if filled else None,
    }


async def _round_trip_with_stale_snapshots(core, *, entry_side, symbol):
    """Two round-trips where every order's late 'accepted' snapshot lands
    after its fill, and the sweeper re-polls the first exit only after the
    second entry has filled (the interleaving that produced negative holds).
    """
    exit_side = "buy" if entry_side == "sell" else "sell"
    t = [T0 + timedelta(minutes=i) for i in range(4)]
    e1, x1, e2, x2 = "e1", "x1", "e2", "x2"

    await core.apply_alpaca_order(_snap(e1, entry_side, "filled", t[0], symbol=symbol), "stream")
    await core.apply_alpaca_order(_snap(e1, entry_side, "accepted", t[0], symbol=symbol), "submit")
    await core.apply_alpaca_order(_snap(e1, entry_side, "filled", t[0], symbol=symbol), "poll")

    await core.apply_alpaca_order(_snap(x1, exit_side, "filled", t[1], price=401.0, symbol=symbol), "stream")
    await core.apply_alpaca_order(_snap(x1, exit_side, "accepted", t[1], symbol=symbol), "submit")

    await core.apply_alpaca_order(_snap(e2, entry_side, "filled", t[2], price=402.0, symbol=symbol), "stream")
    await core.apply_alpaca_order(_snap(x1, exit_side, "filled", t[1], price=401.0, symbol=symbol), "poll")
    await core.apply_alpaca_order(_snap(e2, entry_side, "accepted", t[2], symbol=symbol), "submit")

    await core.apply_alpaca_order(_snap(x2, exit_side, "filled", t[3], price=399.0, symbol=symbol), "stream")
    await core.apply_alpaca_order(_snap(x2, exit_side, "accepted", t[3], symbol=symbol), "submit")


def _assert_cleanly_paired(ledger: _Ledger):
    assert len(ledger.trades) == 2, ledger.trades
    t1, t2 = ledger.trades
    assert (t1.entry_order_id, t1.exit_order_id) == (ledger.order_id("e1"), ledger.order_id("x1"))
    assert (t2.entry_order_id, t2.exit_order_id) == (ledger.order_id("e2"), ledger.order_id("x2"))
    assert all(t.hold_duration_seconds is not None and t.hold_duration_seconds >= 0
               for t in ledger.trades)
    assert [t for t in ledger.trades if t.exit_time is None] == []


@pytest.mark.parametrize(
    "entry_side,symbol", [("sell", "MSFT"), ("buy", "NVDA")],
    ids=["short_round_trips", "long_round_trips"],
)
async def test_stale_snapshots_do_not_double_book_or_shift_pairing(entry_side, symbol):
    ledger = _Ledger()
    core = ReconcilerCore(db=ledger)
    await _round_trip_with_stale_snapshots(core, entry_side=entry_side, symbol=symbol)
    _assert_cleanly_paired(ledger)


async def test_without_status_guard_the_same_sequence_corrupts_pairing():
    """Documents the failure mode the DB-level rule prevents.

    With the ledger writing every snapshot's status verbatim (the pre-fix
    SQL), the reconciler-side stale check alone is not enough: the row has
    already regressed, so the sweeper's next poll is a "new" fill and the
    entry is booked twice.
    """
    ledger = _Ledger(guard_stale=False)
    core = ReconcilerCore(db=ledger)
    await _round_trip_with_stale_snapshots(core, entry_side="sell", symbol="MSFT")
    entries = [t.entry_order_id for t in ledger.trades]
    assert len(entries) != len(set(entries))  # same fill booked twice


def test_merge_order_status_never_regresses():
    assert merge_order_status(None, "submitted") == "submitted"
    assert merge_order_status("submitted", "filled") == "filled"
    assert merge_order_status("submitted", "partially_filled") == "partially_filled"
    assert merge_order_status("partially_filled", "filled") == "filled"
    assert merge_order_status("filled", "submitted") == "filled"
    assert merge_order_status("partially_filled", "submitted") == "partially_filled"
    assert merge_order_status("cancelled", "submitted") == "cancelled"
    # terminal -> terminal still applies (feeds the filled_reverted anomaly)
    assert merge_order_status("filled", "cancelled") == "cancelled"


async def test_fractional_lots_close_fully_despite_float_sum_error():
    """0.1 + 0.2 > 0.3 in floating point; the full close must still drain both."""
    ledger = _Ledger()
    core = ReconcilerCore(db=ledger)
    await core.apply_alpaca_order(
        _snap("b1", "buy", "filled", T0, qty=0.1, symbol="ETH/USD"), "stream")
    await core.apply_alpaca_order(
        _snap("b2", "buy", "filled", T0 + timedelta(minutes=1), qty=0.2, symbol="ETH/USD"), "stream")
    await core.apply_alpaca_order(
        _snap("s1", "sell", "filled", T0 + timedelta(minutes=2), qty=0.3, symbol="ETH/USD"), "stream")
    assert [t.exit_order_id for t in ledger.trades] == [ledger.order_id("s1")] * 2


async def test_sell_closes_long_even_when_an_older_short_row_is_open():
    """open = [short (oldest, stray), long]; a sell closes the long only.

    Pre-fix, the side check looked at open_trades[0] (the short), decided
    the sell was a new short entry, and the long was never closed.
    """
    ledger = _Ledger()
    ledger.trades.append(Trade(
        id=1, symbol="NVDA", side="short", entry_time=T0 - timedelta(days=1),
        entry_price=100.0, quantity=1.0, entry_order_id=999,
    ))
    ledger.trades.append(Trade(
        id=2, symbol="NVDA", side="long", entry_time=T0,
        entry_price=400.0, quantity=5.0, entry_order_id=998,
    ))
    core = ReconcilerCore(db=ledger)
    await core.apply_alpaca_order(
        _snap("s1", "sell", "filled", T0 + timedelta(minutes=5), price=410.0, symbol="NVDA"),
        "stream")

    by_id = {t.id: t for t in ledger.trades}
    assert by_id[2].exit_order_id == ledger.order_id("s1")
    assert by_id[2].pnl == pytest.approx(50.0)
    assert by_id[1].exit_time is None  # untouched
    assert len(ledger.trades) == 2      # no phantom short opened


async def test_buy_never_closes_a_long_row():
    """Adding to a long must open a new row, not drain the existing long."""
    ledger = _Ledger()
    ledger.trades.append(Trade(
        id=1, symbol="NVDA", side="long", entry_time=T0,
        entry_price=400.0, quantity=5.0, entry_order_id=998,
    ))
    core = ReconcilerCore(db=ledger)
    await core.apply_alpaca_order(
        _snap("b2", "buy", "filled", T0 + timedelta(minutes=1), symbol="NVDA"), "stream")
    assert [t.exit_time for t in ledger.trades] == [None, None]
    assert [t.side for t in ledger.trades] == ["long", "long"]
