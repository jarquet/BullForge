from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from shared.models import TradingSignal
from services.execution_service.risk_manager import RiskManager


def _rc(values: dict | None = None):
    defaults = {
        "equity_window_open_buffer_minutes": 60,
        "equity_window_close_buffer_minutes": 60,
        "execution_min_signal_strength": 0.5,
        "execution_max_position_pct": 0.10,
        "execution_max_positions": 3,
        "execution_max_exposure": 0.5,
    }
    data = {**defaults, **(values or {})}
    rc = AsyncMock()

    async def get(key):
        return data[key]

    rc.get = get
    return rc


def _signal(direction=1, strength=0.8, symbol="SPY", hour_utc=14):
    return TradingSignal(
        symbol=symbol,
        timestamp=datetime(2026, 4, 13, hour_utc, 0, 0, tzinfo=timezone.utc),
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


@pytest.mark.asyncio
async def test_check_entry_approved():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, qty = await rm.check_entry(
        _signal(hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is True
    assert qty == 23
    assert reason == "Approved"


@pytest.mark.asyncio
async def test_check_entry_qty_scales_with_equity():
    rm = RiskManager(runtime_config=_rc())
    approved, _, qty = await rm.check_entry(
        _signal(hour_utc=16), _account(equity=200000.0), [], current_price=420.0,
    )
    assert approved is True
    assert qty == 47


@pytest.mark.asyncio
async def test_check_entry_weak_signal():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, qty = await rm.check_entry(
        _signal(strength=0.3, hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is False
    assert "weak" in reason.lower()
    assert qty == 0


@pytest.mark.asyncio
async def test_check_entry_flat_signal():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, qty = await rm.check_entry(
        _signal(direction=0, hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is False
    assert qty == 0


@pytest.mark.asyncio
async def test_check_entry_short_allowed():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, qty = await rm.check_entry(
        _signal(direction=-1, hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is True
    assert qty == 23


@pytest.mark.asyncio
async def test_check_entry_existing_position():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, qty = await rm.check_entry(
        _signal(hour_utc=16), _account(), _positions("SPY"), current_price=420.0,
    )
    assert approved is False
    assert "position" in reason.lower()


@pytest.mark.asyncio
async def test_check_entry_max_positions():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, qty = await rm.check_entry(
        _signal(hour_utc=16), _account(), _positions("QQQ", "AAPL", "MSFT"),
        current_price=420.0,
    )
    assert approved is False
    assert "max" in reason.lower()


@pytest.mark.asyncio
async def test_check_entry_max_exposure():
    rm = RiskManager(runtime_config=_rc())
    big_positions = [
        {"symbol": "QQQ", "qty": 100.0, "market_value": 30000.0},
        {"symbol": "AAPL", "qty": 100.0, "market_value": 25000.0},
    ]
    approved, reason, _ = await rm.check_entry(
        _signal(hour_utc=16), _account(), big_positions, current_price=420.0,
    )
    assert approved is False
    assert "exposure" in reason.lower()


def test_check_exit_opposing_signal():
    rm = RiskManager(runtime_config=_rc())
    position = {"symbol": "SPY", "qty": 10.0}
    should_exit, reason = rm.check_exit(_signal(direction=-1), position)
    assert should_exit is True
    assert "opposing" in reason.lower()


def test_check_exit_flat_signal():
    rm = RiskManager(runtime_config=_rc())
    position = {"symbol": "SPY", "qty": 10.0}
    should_exit, reason = rm.check_exit(_signal(direction=0, strength=0.0), position)
    assert should_exit is True
    assert "flat" in reason.lower()


def test_check_exit_same_direction():
    rm = RiskManager(runtime_config=_rc())
    position = {"symbol": "SPY", "qty": 10.0}
    should_exit, _ = rm.check_exit(_signal(direction=1), position)
    assert should_exit is False


@pytest.mark.asyncio
async def test_overrides_win_over_runtime_config():
    """overrides={'min_signal_strength': 0.9} rejects a 0.8 strength signal."""
    rm = RiskManager(
        runtime_config=_rc(),
        overrides={"min_signal_strength": 0.9},
    )
    approved, reason, _ = await rm.check_entry(
        _signal(strength=0.8, hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is False
    assert "weak" in reason.lower()


@pytest.mark.asyncio
async def test_override_none_falls_back_to_runtime_config():
    rm = RiskManager(
        runtime_config=_rc({"execution_min_signal_strength": 0.1}),
        overrides={"min_signal_strength": None},
    )
    approved, _, qty = await rm.check_entry(
        _signal(strength=0.2, hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is True
    assert qty > 0


@pytest.mark.asyncio
async def test_max_position_pct_override():
    rm = RiskManager(
        runtime_config=_rc(),
        overrides={"max_position_pct": 0.5},
    )
    approved, _, qty = await rm.check_entry(
        _signal(hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is True
    assert qty == 119


@pytest.mark.asyncio
async def test_window_rejects_equity_before_buffer():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, qty = await rm.check_entry(
        _signal(hour_utc=13), _account(), [], current_price=420.0,
    )
    assert approved is False
    assert "window" in reason.lower()
    assert qty == 0


@pytest.mark.asyncio
async def test_window_rejects_equity_after_close_buffer():
    rm = RiskManager(runtime_config=_rc())
    approved, reason, _ = await rm.check_entry(
        _signal(hour_utc=19), _account(), [], current_price=420.0,
    )
    assert approved is False
    assert "window" in reason.lower()


@pytest.mark.asyncio
async def test_window_rejects_equity_weekend():
    rm = RiskManager(runtime_config=_rc())
    sig = TradingSignal(
        symbol="SPY",
        timestamp=datetime(2026, 4, 11, 16, 0, tzinfo=timezone.utc),
        direction=1, strength=0.8, contributing_predictions=3,
    )
    approved, reason, _ = await rm.check_entry(
        sig, _account(), [], current_price=420.0,
    )
    assert approved is False
    assert "window" in reason.lower()


@pytest.mark.asyncio
async def test_window_allows_equity_midday():
    rm = RiskManager(runtime_config=_rc())
    approved, _, qty = await rm.check_entry(
        _signal(hour_utc=16), _account(), [], current_price=420.0,
    )
    assert approved is True
    assert qty == 23


@pytest.mark.asyncio
async def test_window_allows_crypto_weekend():
    rm = RiskManager(runtime_config=_rc())
    sig = TradingSignal(
        symbol="BTC/USD",
        timestamp=datetime(2026, 4, 12, 3, 0, tzinfo=timezone.utc),
        direction=1, strength=0.8, contributing_predictions=3,
    )
    approved, _, _ = await rm.check_entry(
        sig, _account(), [], current_price=67.0,
    )
    assert approved is True


@pytest.mark.asyncio
async def test_window_allows_crypto_weekday_midnight():
    rm = RiskManager(runtime_config=_rc())
    sig = TradingSignal(
        symbol="ETH/USD",
        timestamp=datetime(2026, 4, 15, 0, 0, tzinfo=timezone.utc),
        direction=1, strength=0.8, contributing_predictions=3,
    )
    approved, _, _ = await rm.check_entry(
        sig, _account(), [], current_price=3500.0,
    )
    assert approved is True


@pytest.mark.asyncio
async def test_window_gate_precedes_other_checks():
    rm = RiskManager(runtime_config=_rc())
    sig = TradingSignal(
        symbol="SPY",
        timestamp=datetime(2026, 4, 12, 16, 0, tzinfo=timezone.utc),
        direction=0, strength=0.1, contributing_predictions=1,
    )
    approved, reason, _ = await rm.check_entry(
        sig, _account(), [], current_price=420.0,
    )
    assert approved is False
    assert "window" in reason.lower()
