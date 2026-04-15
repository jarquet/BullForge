import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from shared.db import UpsertResult
from shared.models import Order, Trade
from services.execution_service.reconciler_core import ReconcilerCore


def _snapshot(status="submitted", alpaca_id="alp-1", symbol="SPY", side="buy"):
    return {
        "id": alpaca_id, "symbol": symbol, "side": side,
        "qty": 1.0, "order_type": "market",
        "submitted_at": datetime.now(timezone.utc),
        "status": status,
        "filled_qty": 1.0 if status == "filled" else None,
        "filled_avg_price": 100.0 if status == "filled" else None,
        "filled_at": datetime.now(timezone.utc) if status == "filled" else None,
    }


def _order(status="submitted", side="buy", origin="internal", order_id=1, filled_price=None):
    filled_at = None
    if status == "filled":
        filled_at = datetime.now(timezone.utc)
        if filled_price is None:
            filled_price = 100.0
    return Order(
        symbol="SPY", side=side, quantity=1.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
        alpaca_order_id="alp-1", status=status, origin=origin,
        id=order_id, filled_at=filled_at, filled_price=filled_price,
        filled_quantity=1.0 if status == "filled" else None,
    )


def _upsert_result(old_status, new_status, inserted=False, origin="internal"):
    return UpsertResult(
        order=_order(status=new_status, origin=origin),
        old_status=old_status, new_status=new_status, inserted=inserted,
    )


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.upsert_order_from_alpaca = AsyncMock()
    db.insert_trade = AsyncMock(return_value=1)
    db.update_trade_exit = AsyncMock()
    db.get_open_trade = AsyncMock(return_value=None)
    return db


@pytest.fixture
def mock_redis():
    r = AsyncMock()
    r.publish = AsyncMock()
    return r


@pytest.fixture
def core(mock_db, mock_redis):
    return ReconcilerCore(db=mock_db, redis_client=mock_redis)


@pytest.mark.asyncio
async def test_submitted_to_cancelled_publishes_and_does_not_insert_trade(
    core, mock_db, mock_redis,
):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="submitted", new_status="cancelled",
    )
    await core.apply_alpaca_order(_snapshot(status="cancelled"), source="poll")
    mock_db.insert_trade.assert_not_called()
    mock_db.update_trade_exit.assert_not_called()
    channels = [call.args[0] for call in mock_redis.publish.await_args_list]
    assert "execution:orders" in channels
    order_call = next(c for c in mock_redis.publish.await_args_list if c.args[0] == "execution:orders")
    assert json.loads(order_call.args[1])["status"] == "cancelled"


@pytest.mark.asyncio
async def test_no_status_change_does_not_publish(core, mock_db, mock_redis):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="filled", new_status="filled",
    )
    await core.apply_alpaca_order(_snapshot(status="filled"), source="poll")
    mock_redis.publish.assert_not_called()
    mock_db.insert_trade.assert_not_called()


@pytest.mark.asyncio
async def test_filled_to_cancelled_emits_anomaly(core, mock_db, mock_redis):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="filled", new_status="cancelled",
    )
    await core.apply_alpaca_order(_snapshot(status="cancelled"), source="stream")
    channels = [call.args[0] for call in mock_redis.publish.await_args_list]
    assert "execution:orders:anomaly" in channels
    mock_db.update_trade_exit.assert_not_called()


@pytest.mark.asyncio
async def test_fill_on_buy_with_no_open_trade_inserts_entry_trade(
    core, mock_db, mock_redis,
):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="submitted", new_status="filled",
    )
    mock_db.get_open_trade.return_value = None
    await core.apply_alpaca_order(_snapshot(status="filled"), source="stream")
    mock_db.insert_trade.assert_awaited_once()
    trade_arg = mock_db.insert_trade.await_args.args[0]
    assert isinstance(trade_arg, Trade)
    assert trade_arg.side == "long"
    assert trade_arg.entry_order_id == 1


@pytest.mark.asyncio
async def test_fill_on_sell_with_open_long_closes_trade(
    core, mock_db, mock_redis,
):
    open_trade = Trade(
        id=77, symbol="SPY", side="long",
        entry_time=datetime(2026, 4, 14, 13, 0, tzinfo=timezone.utc),
        entry_price=100.0, quantity=1.0, entry_order_id=10,
    )
    mock_db.get_open_trade.return_value = open_trade
    # Override with a sell order at 110.0 to test exit
    mock_db.upsert_order_from_alpaca.return_value = UpsertResult(
        order=_order(status="filled", side="sell", filled_price=110.0),
        old_status="submitted", new_status="filled", inserted=False,
    )
    snap = _snapshot(status="filled", side="sell")
    snap["filled_avg_price"] = 110.0
    await core.apply_alpaca_order(snap, source="stream")
    mock_db.update_trade_exit.assert_awaited_once()
    kwargs = mock_db.update_trade_exit.await_args.kwargs
    assert kwargs["trade_id"] == 77
    assert kwargs["pnl"] == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_duplicate_fill_event_does_not_insert_duplicate_trade(
    core, mock_db, mock_redis,
):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="filled", new_status="filled",
    )
    await core.apply_alpaca_order(_snapshot(status="filled"), source="stream")
    mock_db.insert_trade.assert_not_called()
    mock_db.update_trade_exit.assert_not_called()


# ---------------------------------------------------------------------------
# Counter event tests
# ---------------------------------------------------------------------------

def _monitoring_calls(mock_redis):
    """Return parsed counter payloads published to monitoring:reconciler."""
    return [
        json.loads(call.args[1])
        for call in mock_redis.publish.await_args_list
        if call.args[0] == "monitoring:reconciler"
    ]


@pytest.mark.asyncio
async def test_counter_events_total_on_status_change(core, mock_db, mock_redis):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="submitted", new_status="cancelled", inserted=False,
    )
    await core.apply_alpaca_order(_snapshot(status="cancelled"), source="poll")
    counters = _monitoring_calls(mock_redis)
    events = [c for c in counters if c["counter"] == "reconciler_events_total"]
    assert len(events) == 1
    assert events[0]["labels"]["source"] == "poll"
    assert events[0]["labels"]["transition"] == "submitted->cancelled"
    assert events[0]["value"] == 1


@pytest.mark.asyncio
async def test_counter_events_total_on_insert_uses_none_as_old(
    core, mock_db, mock_redis,
):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="submitted", new_status="submitted", inserted=True,
    )
    # inserted=True and old==new means status_changed is False — no counter
    await core.apply_alpaca_order(_snapshot(status="submitted"), source="poll")
    counters = _monitoring_calls(mock_redis)
    events = [c for c in counters if c["counter"] == "reconciler_events_total"]
    assert len(events) == 0  # no status change → no event counter


@pytest.mark.asyncio
async def test_counter_events_total_inserted_with_status_change(
    core, mock_db, mock_redis,
):
    """When inserted=True but old_status differs (edge case), label old as 'none'."""
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="submitted", new_status="cancelled", inserted=True,
    )
    await core.apply_alpaca_order(_snapshot(status="cancelled"), source="stream")
    counters = _monitoring_calls(mock_redis)
    events = [c for c in counters if c["counter"] == "reconciler_events_total"]
    assert len(events) == 1
    assert events[0]["labels"]["transition"] == "none->cancelled"


@pytest.mark.asyncio
async def test_no_counter_on_no_op_duplicate(core, mock_db, mock_redis):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="filled", new_status="filled",
    )
    await core.apply_alpaca_order(_snapshot(status="filled"), source="stream")
    counters = _monitoring_calls(mock_redis)
    events = [c for c in counters if c["counter"] == "reconciler_events_total"]
    assert len(events) == 0


@pytest.mark.asyncio
async def test_counter_orphans_ingested_only_when_source_is_poll(
    core, mock_db, mock_redis,
):
    # inserted=True, source="poll" → orphan counter emitted
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="submitted", new_status="cancelled", inserted=True,
    )
    await core.apply_alpaca_order(_snapshot(status="cancelled"), source="poll")
    counters = _monitoring_calls(mock_redis)
    orphans = [c for c in counters if c["counter"] == "reconciler_orphans_ingested_total"]
    assert len(orphans) == 1
    assert orphans[0]["labels"] == {}


@pytest.mark.asyncio
async def test_no_orphan_counter_when_source_is_not_poll(
    core, mock_db, mock_redis,
):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="submitted", new_status="cancelled", inserted=True,
    )
    await core.apply_alpaca_order(_snapshot(status="cancelled"), source="stream")
    counters = _monitoring_calls(mock_redis)
    orphans = [c for c in counters if c["counter"] == "reconciler_orphans_ingested_total"]
    assert len(orphans) == 0


@pytest.mark.asyncio
async def test_counter_anomalies_on_filled_reverted(core, mock_db, mock_redis):
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="filled", new_status="cancelled",
    )
    await core.apply_alpaca_order(_snapshot(status="cancelled"), source="stream")
    counters = _monitoring_calls(mock_redis)
    anomalies = [c for c in counters if c["counter"] == "reconciler_anomalies_total"]
    assert len(anomalies) == 1
    assert anomalies[0]["labels"] == {"kind": "filled_reverted"}
    assert anomalies[0]["value"] == 1


@pytest.mark.asyncio
async def test_anomaly_does_not_also_emit_events_total(core, mock_db, mock_redis):
    """The anomaly branch returns early; events_total should NOT be emitted."""
    mock_db.upsert_order_from_alpaca.return_value = _upsert_result(
        old_status="filled", new_status="cancelled",
    )
    await core.apply_alpaca_order(_snapshot(status="cancelled"), source="stream")
    counters = _monitoring_calls(mock_redis)
    events = [c for c in counters if c["counter"] == "reconciler_events_total"]
    assert len(events) == 0
