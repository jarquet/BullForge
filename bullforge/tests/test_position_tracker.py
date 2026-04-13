import pytest
from unittest.mock import AsyncMock
from services.execution_service.position_tracker import PositionTracker


@pytest.fixture
def mock_broker():
    broker = AsyncMock()
    broker.get_account = AsyncMock(return_value={
        "equity": 100000.0,
        "buying_power": 200000.0,
        "cash": 100000.0,
        "portfolio_value": 100000.0,
    })
    broker.get_positions = AsyncMock(return_value=[
        {
            "symbol": "SPY",
            "qty": 10.0,
            "side": "long",
            "avg_entry_price": 420.0,
            "current_price": 425.0,
            "unrealized_pl": 50.0,
            "market_value": 4250.0,
        },
    ])
    return broker


@pytest.mark.asyncio
async def test_refresh_updates_state(mock_broker):
    tracker = PositionTracker(mock_broker)
    await tracker.refresh()

    assert tracker.account["equity"] == 100000.0
    assert len(tracker.positions) == 1
    assert "SPY" in tracker.positions


@pytest.mark.asyncio
async def test_get_position_found(mock_broker):
    tracker = PositionTracker(mock_broker)
    await tracker.refresh()

    pos = tracker.get_position("SPY")
    assert pos is not None
    assert pos["qty"] == 10.0


@pytest.mark.asyncio
async def test_get_position_not_found(mock_broker):
    tracker = PositionTracker(mock_broker)
    await tracker.refresh()

    pos = tracker.get_position("AAPL")
    assert pos is None


@pytest.mark.asyncio
async def test_has_position(mock_broker):
    tracker = PositionTracker(mock_broker)
    await tracker.refresh()

    assert tracker.has_position("SPY") is True
    assert tracker.has_position("AAPL") is False


@pytest.mark.asyncio
async def test_initial_state_empty():
    broker = AsyncMock()
    tracker = PositionTracker(broker)

    assert tracker.account == {}
    assert tracker.positions == {}
    assert tracker.get_position("SPY") is None
