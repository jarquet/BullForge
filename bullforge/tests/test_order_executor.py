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
        "symbol": "SPY",
        "side": "buy",
        "qty": 10.0,
        "order_type": "market",
        "submitted_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        "status": "accepted",
        "filled_qty": None,
        "filled_avg_price": None,
        "filled_at": None,
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
    db.get_order_by_alpaca_id = AsyncMock(return_value=None)
    return db


@pytest.fixture
def mock_core():
    core = AsyncMock()
    core.apply_alpaca_order = AsyncMock()
    return core


@pytest.fixture
def executor(mock_broker, mock_risk, mock_tracker, mock_db, mock_core):
    return OrderExecutor(
        broker=mock_broker,
        risk_manager=mock_risk,
        position_tracker=mock_tracker,
        db=mock_db,
        core=mock_core,
    )


@pytest.mark.asyncio
async def test_process_signal_submits_and_delegates_to_core(
    executor, mock_broker, mock_core, mock_db,
):
    order = await executor.process_signal(_signal(), current_price=420.0)
    assert order is not None
    assert order.symbol == "SPY"
    assert order.side == "buy"
    mock_broker.submit_market_order.assert_awaited_once_with("SPY", 10, "buy")
    mock_db.insert_order.assert_awaited_once()
    mock_core.apply_alpaca_order.assert_awaited_once()
    args, kwargs = mock_core.apply_alpaca_order.await_args
    assert (kwargs.get("source") or args[1]) == "submit"


@pytest.mark.asyncio
async def test_process_signal_rejected_by_risk_does_not_submit(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_risk.check_entry = AsyncMock(return_value=(False, "Too risky", 0))
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
    )
    result = await executor.process_signal(_signal(), current_price=420.0)
    assert result is None
    mock_broker.submit_market_order.assert_not_called()
    mock_core.apply_alpaca_order.assert_not_called()


@pytest.mark.asyncio
async def test_process_signal_exits_on_opposing(
    executor, mock_broker, mock_core, mock_risk, mock_tracker,
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
    mock_broker.close_position.assert_awaited_once_with("SPY")
    mock_core.apply_alpaca_order.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_signal_exits_on_flat(
    executor, mock_broker, mock_core, mock_risk, mock_tracker,
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
async def test_process_signal_no_action_flat_no_position(executor, mock_broker, mock_core):
    order = await executor.process_signal(
        _signal(direction=0, strength=0.0), current_price=420.0,
    )
    assert order is None
    mock_broker.submit_market_order.assert_not_called()
    mock_core.apply_alpaca_order.assert_not_called()


@pytest.mark.asyncio
async def test_process_signal_no_action_same_direction(
    executor, mock_broker, mock_core, mock_risk, mock_tracker,
):
    mock_tracker.get_position = MagicMock(
        return_value={"symbol": "SPY", "qty": 10.0, "market_value": 4200.0},
    )
    mock_risk.check_exit = MagicMock(return_value=(False, "Signal matches position"))

    order = await executor.process_signal(_signal(), current_price=420.0)
    assert order is None
    mock_broker.submit_market_order.assert_not_called()


@pytest.mark.asyncio
async def test_broker_failure_returns_none(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_broker.submit_market_order = AsyncMock(
        side_effect=Exception("connection refused"),
    )
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
    )
    order = await executor.process_signal(_signal(), current_price=420.0)
    assert order is None
    mock_db.insert_order.assert_not_called()
    mock_core.apply_alpaca_order.assert_not_called()
