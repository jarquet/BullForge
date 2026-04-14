import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from shared.models import TradingSignal, Order
from services.execution_service.order_executor import OrderExecutor


def _signal(direction=1, strength=0.8, symbol="SPY"):
    return TradingSignal(
        symbol=symbol,
        timestamp=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        direction=direction,
        strength=strength,
        contributing_predictions=3,
    )


@pytest.fixture
def mock_broker():
    broker = AsyncMock()
    broker.submit_market_order = AsyncMock(return_value="alp-order-123")
    broker.get_order = AsyncMock(return_value={
        "id": "alp-order-123",
        "status": "filled",
        "filled_qty": 10.0,
        "filled_avg_price": 420.50,
        "filled_at": datetime(2026, 4, 12, 14, 0, 1, tzinfo=timezone.utc),
    })
    broker.close_position = AsyncMock(return_value="alp-close-456")
    return broker


@pytest.fixture
def mock_risk():
    risk = MagicMock()
    risk.check_entry = AsyncMock(return_value=(True, "Approved", 10))
    risk.check_exit = MagicMock(return_value=(False, "Signal matches position"))
    return risk


@pytest.fixture
def mock_tracker():
    tracker = AsyncMock()
    tracker.refresh = AsyncMock()
    tracker.account = {"equity": 100000.0, "buying_power": 200000.0}
    tracker.positions = {}
    tracker.get_position = MagicMock(return_value=None)
    return tracker


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.insert_order = AsyncMock(return_value=42)
    db.update_order_status = AsyncMock()
    db.insert_trade = AsyncMock(return_value=1)
    db.update_trade_exit = AsyncMock()
    db.get_open_trade = AsyncMock(return_value=None)
    return db


@pytest.fixture
def executor(mock_broker, mock_risk, mock_tracker, mock_db):
    return OrderExecutor(
        broker=mock_broker,
        risk_manager=mock_risk,
        position_tracker=mock_tracker,
        db=mock_db,
        fill_check_delay=0,
    )


@pytest.mark.asyncio
async def test_process_signal_enters_long(executor, mock_broker, mock_db):
    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is not None
    assert order.side == "buy"
    assert order.symbol == "SPY"
    assert order.status == "filled"
    assert order.filled_price == 420.50
    assert order.id == 42
    mock_broker.submit_market_order.assert_called_once_with("SPY", 10, "buy")
    mock_db.insert_order.assert_called_once()
    mock_db.update_order_status.assert_called_once()


@pytest.mark.asyncio
async def test_process_signal_exits_on_opposing(
    executor, mock_broker, mock_db, mock_risk, mock_tracker,
):
    mock_tracker.get_position = MagicMock(
        return_value={"symbol": "SPY", "qty": 10.0, "market_value": 4200.0},
    )
    mock_risk.check_exit = MagicMock(return_value=(True, "Opposing signal"))

    order = await executor.process_signal(
        _signal(direction=-1), current_price=420.0,
    )

    assert order is not None
    assert order.side == "sell"
    mock_broker.close_position.assert_called_once_with("SPY")


@pytest.mark.asyncio
async def test_process_signal_exits_on_flat(
    executor, mock_broker, mock_db, mock_risk, mock_tracker,
):
    mock_tracker.get_position = MagicMock(
        return_value={"symbol": "SPY", "qty": 10.0, "market_value": 4200.0},
    )
    mock_risk.check_exit = MagicMock(return_value=(True, "Flat signal"))

    order = await executor.process_signal(
        _signal(direction=0, strength=0.0), current_price=420.0,
    )

    assert order is not None
    assert order.side == "sell"


@pytest.mark.asyncio
async def test_process_signal_rejected_by_risk(
    executor, mock_broker, mock_risk,
):
    mock_risk.check_entry = AsyncMock(
        return_value=(False, "Max positions reached", 0),
    )

    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is None
    mock_broker.submit_market_order.assert_not_called()


@pytest.mark.asyncio
async def test_process_signal_no_action_flat_no_position(executor, mock_broker):
    order = await executor.process_signal(
        _signal(direction=0, strength=0.0), current_price=420.0,
    )

    assert order is None
    mock_broker.submit_market_order.assert_not_called()


@pytest.mark.asyncio
async def test_process_signal_no_action_same_direction(
    executor, mock_broker, mock_risk, mock_tracker,
):
    mock_tracker.get_position = MagicMock(
        return_value={"symbol": "SPY", "qty": 10.0, "market_value": 4200.0},
    )
    mock_risk.check_exit = MagicMock(
        return_value=(False, "Signal matches position"),
    )

    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is None
    mock_broker.submit_market_order.assert_not_called()


@pytest.mark.asyncio
async def test_broker_failure_returns_none(
    mock_broker, mock_risk, mock_tracker, mock_db,
):
    mock_broker.submit_market_order = AsyncMock(
        side_effect=Exception("connection refused"),
    )
    executor = OrderExecutor(
        broker=mock_broker,
        risk_manager=mock_risk,
        position_tracker=mock_tracker,
        db=mock_db,
        fill_check_delay=0,
    )

    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is None
    mock_db.insert_order.assert_not_called()


@pytest.mark.asyncio
async def test_fill_check_retries_until_filled(
    mock_broker, mock_risk, mock_tracker, mock_db,
):
    mock_broker.get_order = AsyncMock(side_effect=[
        {"id": "alp-order-123", "status": "submitted",
         "filled_qty": None, "filled_avg_price": None, "filled_at": None},
        {"id": "alp-order-123", "status": "filled",
         "filled_qty": 10.0, "filled_avg_price": 420.50,
         "filled_at": datetime(2026, 4, 12, 14, 0, 1, tzinfo=timezone.utc)},
    ])
    executor = OrderExecutor(
        broker=mock_broker,
        risk_manager=mock_risk,
        position_tracker=mock_tracker,
        db=mock_db,
        fill_check_delay=0,
        fill_check_retries=3,
    )

    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is not None
    assert order.status == "filled"
    assert mock_broker.get_order.call_count == 2
    mock_db.update_order_status.assert_called_once()


@pytest.mark.asyncio
async def test_fill_check_gives_up_after_retries(
    mock_broker, mock_risk, mock_tracker, mock_db,
):
    mock_broker.get_order = AsyncMock(return_value={
        "id": "alp-order-123", "status": "submitted",
        "filled_qty": None, "filled_avg_price": None, "filled_at": None,
    })
    executor = OrderExecutor(
        broker=mock_broker,
        risk_manager=mock_risk,
        position_tracker=mock_tracker,
        db=mock_db,
        fill_check_delay=0,
        fill_check_retries=2,
    )

    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is not None
    assert order.status == "submitted"
    assert mock_broker.get_order.call_count == 2
    mock_db.update_order_status.assert_not_called()


@pytest.mark.asyncio
async def test_fill_check_handles_cancelled(
    mock_broker, mock_risk, mock_tracker, mock_db,
):
    mock_broker.get_order = AsyncMock(return_value={
        "id": "alp-order-123", "status": "cancelled",
        "filled_qty": None, "filled_avg_price": None, "filled_at": None,
    })
    executor = OrderExecutor(
        broker=mock_broker,
        risk_manager=mock_risk,
        position_tracker=mock_tracker,
        db=mock_db,
        fill_check_delay=0,
    )

    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is not None
    assert order.status == "cancelled"
    mock_db.update_order_status.assert_called_once_with(42, status="cancelled")


@pytest.mark.asyncio
async def test_fill_check_retries_on_broker_error(
    mock_broker, mock_risk, mock_tracker, mock_db,
):
    mock_broker.get_order = AsyncMock(side_effect=[
        Exception("timeout"),
        {"id": "alp-order-123", "status": "filled",
         "filled_qty": 10.0, "filled_avg_price": 420.50,
         "filled_at": datetime(2026, 4, 12, 14, 0, 1, tzinfo=timezone.utc)},
    ])
    executor = OrderExecutor(
        broker=mock_broker,
        risk_manager=mock_risk,
        position_tracker=mock_tracker,
        db=mock_db,
        fill_check_delay=0,
        fill_check_retries=3,
    )

    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is not None
    assert order.status == "filled"
    assert mock_broker.get_order.call_count == 2


@pytest.mark.asyncio
async def test_execute_open_records_trade(executor, mock_broker, mock_db):
    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is not None
    mock_db.insert_trade.assert_called_once()
    trade_arg = mock_db.insert_trade.call_args[0][0]
    assert trade_arg.symbol == "SPY"
    assert trade_arg.side == "long"
    assert trade_arg.entry_price == 420.50
    assert trade_arg.quantity == 10.0
    assert trade_arg.entry_order_id == 42


@pytest.mark.asyncio
async def test_execute_open_no_trade_on_unfilled(
    mock_broker, mock_risk, mock_tracker, mock_db,
):
    mock_broker.get_order = AsyncMock(return_value={
        "id": "alp-order-123", "status": "submitted",
        "filled_qty": None, "filled_avg_price": None, "filled_at": None,
    })
    executor = OrderExecutor(
        broker=mock_broker,
        risk_manager=mock_risk,
        position_tracker=mock_tracker,
        db=mock_db,
        fill_check_delay=0,
        fill_check_retries=1,
    )

    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is not None
    assert order.status == "submitted"
    mock_db.insert_trade.assert_not_called()


@pytest.mark.asyncio
async def test_execute_close_updates_trade(
    executor, mock_broker, mock_db, mock_risk, mock_tracker,
):
    mock_tracker.get_position = MagicMock(
        return_value={"symbol": "SPY", "qty": 10.0, "market_value": 4200.0},
    )
    mock_risk.check_exit = MagicMock(return_value=(True, "Opposing signal"))
    mock_db.get_open_trade = AsyncMock(return_value=MagicMock(
        id=1,
        entry_price=420.0,
        entry_time=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        side="long",
        quantity=10.0,
    ))

    order = await executor.process_signal(
        _signal(direction=-1), current_price=425.0,
    )

    assert order is not None
    assert order.side == "sell"
    mock_db.update_trade_exit.assert_called_once()
    call_kwargs = mock_db.update_trade_exit.call_args
    assert call_kwargs[1]["trade_id"] == 1
    assert call_kwargs[1]["exit_price"] == 420.50


@pytest.mark.asyncio
async def test_execute_close_no_trade_update_when_no_open_trade(
    executor, mock_broker, mock_db, mock_risk, mock_tracker,
):
    mock_tracker.get_position = MagicMock(
        return_value={"symbol": "SPY", "qty": 10.0, "market_value": 4200.0},
    )
    mock_risk.check_exit = MagicMock(return_value=(True, "Opposing signal"))
    mock_db.get_open_trade = AsyncMock(return_value=None)

    order = await executor.process_signal(
        _signal(direction=-1), current_price=425.0,
    )

    assert order is not None
    mock_db.update_trade_exit.assert_not_called()
