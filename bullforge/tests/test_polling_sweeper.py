import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from shared.models import Order
from services.execution_service.polling_sweeper import PollingSweeper


def _order(alpaca_id, status="submitted"):
    return Order(
        symbol="SPY", side="buy", quantity=1.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
        alpaca_order_id=alpaca_id, status=status, id=1,
    )


@pytest.mark.asyncio
async def test_sweep_fetches_only_non_terminal_and_forwards():
    db = AsyncMock()
    db.query_non_terminal_orders = AsyncMock(return_value=[_order("a"), _order("b")])
    broker = AsyncMock()
    broker.get_order = AsyncMock(side_effect=lambda aid: {
        "id": aid, "symbol": "SPY", "side": "buy", "qty": 1.0,
        "order_type": "market",
        "submitted_at": datetime.now(timezone.utc),
        "status": "cancelled",
    })
    broker.list_open_orders = AsyncMock(return_value=[])
    core = AsyncMock()

    sweeper = PollingSweeper(
        db=db, broker=broker, core=core,
        interval_s=60, orphan_every=5, orphan_window_hours=48,
    )
    await sweeper.sweep_now()

    assert core.apply_alpaca_order.await_count == 2
    for call in core.apply_alpaca_order.await_args_list:
        assert call.kwargs.get("source") == "poll" or call.args[1] == "poll"


@pytest.mark.asyncio
async def test_sweep_continues_when_one_fetch_fails():
    db = AsyncMock()
    db.query_non_terminal_orders = AsyncMock(
        return_value=[_order("good"), _order("bad")]
    )
    broker = AsyncMock()

    async def get_order(aid):
        if aid == "bad":
            raise RuntimeError("boom")
        return {
            "id": aid, "symbol": "SPY", "side": "buy", "qty": 1.0,
            "order_type": "market",
            "submitted_at": datetime.now(timezone.utc),
            "status": "cancelled",
        }
    broker.get_order = AsyncMock(side_effect=get_order)
    broker.list_open_orders = AsyncMock(return_value=[])
    core = AsyncMock()

    sweeper = PollingSweeper(
        db=db, broker=broker, core=core,
        interval_s=60, orphan_every=5, orphan_window_hours=48,
    )
    await sweeper.sweep_now()

    assert core.apply_alpaca_order.await_count == 1


@pytest.mark.asyncio
async def test_orphan_scan_runs_every_n_ticks_only():
    db = AsyncMock()
    db.query_non_terminal_orders = AsyncMock(return_value=[])
    db.get_order_by_alpaca_id = AsyncMock(return_value=None)
    broker = AsyncMock()
    broker.list_open_orders = AsyncMock(return_value=[
        {"id": "orphan-1", "symbol": "SPY", "side": "buy", "qty": 1.0,
         "order_type": "market",
         "submitted_at": datetime.now(timezone.utc), "status": "new"},
    ])
    core = AsyncMock()

    sweeper = PollingSweeper(
        db=db, broker=broker, core=core,
        interval_s=60, orphan_every=3, orphan_window_hours=48,
    )
    for _ in range(2):
        await sweeper.sweep_now()
    assert broker.list_open_orders.await_count == 0

    await sweeper.sweep_now()
    assert broker.list_open_orders.await_count == 1
    assert core.apply_alpaca_order.await_count == 1


@pytest.mark.asyncio
async def test_orphan_scan_skips_known_alpaca_ids():
    db = AsyncMock()
    db.query_non_terminal_orders = AsyncMock(return_value=[])
    db.get_order_by_alpaca_id = AsyncMock(return_value=_order("known"))
    broker = AsyncMock()
    broker.list_open_orders = AsyncMock(return_value=[
        {"id": "known", "symbol": "SPY", "side": "buy", "qty": 1.0,
         "order_type": "market",
         "submitted_at": datetime.now(timezone.utc), "status": "new"},
    ])
    core = AsyncMock()

    sweeper = PollingSweeper(
        db=db, broker=broker, core=core,
        interval_s=60, orphan_every=1, orphan_window_hours=48,
    )
    await sweeper.sweep_now()
    core.apply_alpaca_order.assert_not_called()
