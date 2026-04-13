import pytest
from datetime import datetime, timezone
from shared.models import TradingSignal
from services.execution_service.risk_manager import RiskManager


@pytest.fixture
def rm():
    return RiskManager(
        max_position_value=10000.0,
        max_total_positions=3,
        max_portfolio_exposure=0.5,
        min_signal_strength=0.5,
    )


def _signal(direction=1, strength=0.8):
    return TradingSignal(
        symbol="SPY",
        timestamp=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        direction=direction,
        strength=strength,
        contributing_predictions=3,
    )


def _account(equity=100000.0, buying_power=200000.0):
    return {"equity": equity, "buying_power": buying_power}


def _positions(*symbols):
    return [
        {"symbol": s, "qty": 10.0, "market_value": 4200.0} for s in symbols
    ]


def test_check_entry_approved(rm):
    approved, reason, qty = rm.check_entry(
        _signal(), _account(), [], current_price=420.0,
    )
    assert approved is True
    assert qty == 23  # int(10000 / 420)
    assert reason == "Approved"


def test_check_entry_weak_signal(rm):
    approved, reason, qty = rm.check_entry(
        _signal(strength=0.3), _account(), [], current_price=420.0,
    )
    assert approved is False
    assert "weak" in reason.lower()
    assert qty == 0


def test_check_entry_flat_signal(rm):
    approved, reason, qty = rm.check_entry(
        _signal(direction=0), _account(), [], current_price=420.0,
    )
    assert approved is False
    assert qty == 0


def test_check_entry_short_allowed(rm):
    approved, reason, qty = rm.check_entry(
        _signal(direction=-1), _account(), [], current_price=420.0,
    )
    assert approved is True
    assert qty == 23  # int(10000 / 420)
    assert reason == "Approved"


def test_check_entry_existing_position(rm):
    approved, reason, qty = rm.check_entry(
        _signal(), _account(), _positions("SPY"), current_price=420.0,
    )
    assert approved is False
    assert "position" in reason.lower()


def test_check_entry_max_positions(rm):
    approved, reason, qty = rm.check_entry(
        _signal(), _account(), _positions("QQQ", "AAPL", "MSFT"),
        current_price=420.0,
    )
    assert approved is False
    assert "max" in reason.lower()


def test_check_entry_max_exposure(rm):
    # Positions with large market values exceeding 50% of equity
    big_positions = [
        {"symbol": "QQQ", "qty": 100.0, "market_value": 30000.0},
        {"symbol": "AAPL", "qty": 100.0, "market_value": 25000.0},
    ]
    approved, reason, qty = rm.check_entry(
        _signal(), _account(), big_positions, current_price=420.0,
    )
    assert approved is False
    assert "exposure" in reason.lower()


def test_check_exit_opposing_signal(rm):
    position = {"symbol": "SPY", "qty": 10.0}
    should_exit, reason = rm.check_exit(_signal(direction=-1), position)
    assert should_exit is True
    assert "opposing" in reason.lower()


def test_check_exit_flat_signal(rm):
    position = {"symbol": "SPY", "qty": 10.0}
    should_exit, reason = rm.check_exit(_signal(direction=0, strength=0.0), position)
    assert should_exit is True
    assert "flat" in reason.lower()


def test_check_exit_same_direction(rm):
    position = {"symbol": "SPY", "qty": 10.0}
    should_exit, reason = rm.check_exit(_signal(direction=1), position)
    assert should_exit is False
