import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from shared.models import BacktestScope, TradingSignal, Order
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
    db.insert_order_if_new = AsyncMock(return_value=42)
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
    mock_db.insert_order_if_new.assert_awaited_once()
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
    mock_db.insert_order_if_new.assert_not_called()
    mock_core.apply_alpaca_order.assert_not_called()


@pytest.mark.asyncio
async def test_submit_race_stream_wins_still_calls_apply_once(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    """When insert_order_if_new returns None the stream already has the row.
    The executor must still call core.apply_alpaca_order exactly once and
    return the existing order fetched from the DB."""
    existing_order = Order(
        id=99,
        symbol="SPY", side="buy", quantity=10.0, order_type="market",
        submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        signal_strength=0.8,
        alpaca_order_id="alp-order-123", status="submitted",
        origin="external",
    )
    mock_db.insert_order_if_new = AsyncMock(return_value=None)
    mock_db.get_order_by_alpaca_id = AsyncMock(return_value=existing_order)

    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
    )
    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is existing_order
    mock_core.apply_alpaca_order.assert_awaited_once()
    _, kwargs = mock_core.apply_alpaca_order.await_args
    assert kwargs.get("source") == "submit"
    mock_db.get_order_by_alpaca_id.assert_awaited_once_with("alp-order-123", scope=None)


@pytest.mark.asyncio
async def test_backtest_mode_skips_db_insert_when_broker_rejects(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_broker.get_order = AsyncMock(return_value={
        "id": "alp-order-123", "symbol": "BTC/USD", "side": "buy",
        "qty": 0.5, "order_type": "market",
        "submitted_at": datetime(2026, 4, 20, 14, 0, 0, tzinfo=timezone.utc),
        "status": "rejected",
        "filled_qty": None, "filled_avg_price": None, "filled_at": None,
    })
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
        scope=BacktestScope(backtest_run_id=1, fold_index=0),
    )

    order = await executor.process_signal(
        _signal(symbol="BTC/USD"), current_price=60_000.0,
    )

    assert order is None
    mock_broker.submit_market_order.assert_awaited_once()
    mock_db.insert_order_if_new.assert_not_called()
    mock_core.apply_alpaca_order.assert_not_called()


@pytest.mark.asyncio
async def test_backtest_mode_inserts_when_broker_fills(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_broker.get_order = AsyncMock(return_value={
        "id": "alp-order-123", "symbol": "SPY", "side": "buy",
        "qty": 10.0, "order_type": "market",
        "submitted_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        "status": "filled",
        "filled_qty": 10.0, "filled_avg_price": 420.05,
        "filled_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
    })
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
        scope=BacktestScope(backtest_run_id=1, fold_index=0),
    )

    order = await executor.process_signal(_signal(), current_price=420.0)

    assert order is not None
    mock_db.insert_order_if_new.assert_awaited_once()


@pytest.mark.asyncio
async def test_backtest_close_skips_db_insert_when_broker_rejects(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_tracker.get_position = MagicMock(
        return_value={"symbol": "SPY", "qty": 10.0, "market_value": 4200.0},
    )
    mock_risk.check_exit = MagicMock(return_value=(True, "Opposing signal"))
    mock_broker.get_order = AsyncMock(return_value={
        "id": "alp-close-456", "symbol": "SPY", "side": "sell",
        "qty": 10.0, "order_type": "market",
        "submitted_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        "status": "rejected",
        "filled_qty": None, "filled_avg_price": None, "filled_at": None,
    })
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
        scope=BacktestScope(backtest_run_id=1, fold_index=0),
    )

    order = await executor.process_signal(
        _signal(direction=-1), current_price=420.0,
    )

    assert order is None
    mock_broker.close_position.assert_awaited_once()
    mock_db.insert_order_if_new.assert_not_called()


@pytest.mark.asyncio
async def test_max_qty_cap_shrinks_order_to_cap(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_risk.check_entry = AsyncMock(return_value=(True, "Approved", 10.0))
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
    )
    await executor.process_signal(
        _signal(), current_price=420.0, max_qty_cap=2.5,
    )
    mock_broker.submit_market_order.assert_awaited_once_with("SPY", 2.5, "buy")


@pytest.mark.asyncio
async def test_max_qty_cap_skips_when_cap_below_one_dollar_notional(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_risk.check_entry = AsyncMock(return_value=(True, "Approved", 10.0))
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
    )
    result = await executor.process_signal(
        _signal(), current_price=420.0, max_qty_cap=0.001,
    )
    assert result is None
    mock_broker.submit_market_order.assert_not_called()


@pytest.mark.asyncio
async def test_max_qty_cap_floors_equity_short_to_whole_shares(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_risk.check_entry = AsyncMock(return_value=(True, "Approved", 10.0))
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
    )
    await executor.process_signal(
        _signal(direction=-1), current_price=420.0, max_qty_cap=2.5,
    )
    mock_broker.submit_market_order.assert_awaited_once_with("SPY", 2.0, "sell")


@pytest.mark.asyncio
async def test_max_qty_cap_skips_equity_short_capped_below_one_share(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_risk.check_entry = AsyncMock(return_value=(True, "Approved", 10.0))
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
    )
    result = await executor.process_signal(
        _signal(direction=-1), current_price=420.0, max_qty_cap=0.7,
    )
    assert result is None
    mock_broker.submit_market_order.assert_not_called()


@pytest.mark.asyncio
async def test_max_qty_cap_no_op_when_risk_qty_already_below_cap(
    mock_broker, mock_risk, mock_tracker, mock_db, mock_core,
):
    mock_risk.check_entry = AsyncMock(return_value=(True, "Approved", 1.0))
    executor = OrderExecutor(
        broker=mock_broker, risk_manager=mock_risk,
        position_tracker=mock_tracker, db=mock_db, core=mock_core,
    )
    await executor.process_signal(
        _signal(), current_price=420.0, max_qty_cap=5.0,
    )
    mock_broker.submit_market_order.assert_awaited_once_with("SPY", 1.0, "buy")
