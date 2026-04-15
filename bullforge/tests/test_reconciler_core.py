import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from shared.db import UpsertResult
from shared.models import Order
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


def _order(status="submitted", side="buy", origin="internal", order_id=1):
    return Order(
        symbol="SPY", side=side, quantity=1.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
        alpaca_order_id="alp-1", status=status, origin=origin,
        id=order_id,
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
    mock_redis.publish.assert_awaited_once()
    channel, payload = mock_redis.publish.await_args.args
    assert channel == "execution:orders"
    assert json.loads(payload)["status"] == "cancelled"


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
