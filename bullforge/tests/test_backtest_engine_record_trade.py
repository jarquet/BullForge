from datetime import datetime, timezone

import pytest

from services.backtest_service.engine import BacktestEngine
from services.backtest_service.fill_model import FillModel
from services.backtest_service.simulated_broker import SimulatedBroker
from shared.models import Order, TradingSignal


UTC = timezone.utc


def _engine() -> BacktestEngine:
    return BacktestEngine(
        db=None, runtime_config=None, model_store=None, results_db=None,
    )


def _bar(o, h, l, c, v):
    return {"open": o, "high": h, "low": l, "close": c, "volume": v}


def _signal(direction: int) -> TradingSignal:
    return TradingSignal(
        symbol="BTC/USD",
        timestamp=datetime(2026, 4, 20, 4, 0, tzinfo=UTC),
        direction=direction, strength=0.8, contributing_predictions=3,
    )


def _order(alpaca_id: str, side: str, qty: float) -> Order:
    return Order(
        symbol="BTC/USD", side=side, quantity=qty, order_type="market",
        submitted_at=datetime(2026, 4, 20, 4, 0, tzinfo=UTC),
        signal_strength=0.8, alpaca_order_id=alpaca_id, status="submitted",
    )


@pytest.mark.asyncio
async def test_record_trade_skips_rejected_entry():
    # Volume cap set so any entry qty > 0.001 is rejected.
    fm = FillModel(slippage_bps=0.0, max_volume_pct=1.0, max_volume_pct_crypto=0.001)
    broker = SimulatedBroker(starting_cash=100_000.0, fill_model=fm)
    broker.set_clock(datetime(2026, 4, 20, 4, 0, tzinfo=UTC))
    broker.set_next_bar("BTC/USD", _bar(50_000.0, 50_100.0, 49_900.0, 50_050.0, 1.0))

    alpaca_id = await broker.submit_market_order("BTC/USD", 0.5, "buy")
    snap = await broker.get_order(alpaca_id)
    assert snap["status"] == "rejected"
    assert snap["filled_avg_price"] is None

    open_trade: dict = {}
    completed: list = []
    ts = datetime(2026, 4, 20, 4, 1, tzinfo=UTC)

    await _engine()._record_trade(
        "BTC/USD", _order(alpaca_id, "buy", 0.5), _signal(1), ts,
        open_trade, completed, broker,
    )

    assert open_trade == {}
    assert completed == []


@pytest.mark.asyncio
async def test_record_trade_skips_rejected_exit_and_preserves_open_trade():
    # Fill the entry so we have an open trade, then reject the exit.
    fm = FillModel(slippage_bps=0.0, max_volume_pct=1.0)
    broker = SimulatedBroker(starting_cash=100_000.0, fill_model=fm)
    broker.set_clock(datetime(2026, 4, 20, 4, 0, tzinfo=UTC))
    broker.set_next_bar("BTC/USD", _bar(50_000.0, 50_100.0, 49_900.0, 50_050.0, 1.0))

    entry_id = await broker.submit_market_order("BTC/USD", 0.1, "buy")
    assert (await broker.get_order(entry_id))["status"] == "filled"

    open_trade: dict = {
        "BTC/USD": {
            "symbol": "BTC/USD", "side": "long",
            "entry_time": datetime(2026, 4, 20, 4, 0, tzinfo=UTC),
            "entry_price": 50_000.0, "quantity": 0.1,
            "exit_time": None, "exit_price": None, "pnl": None,
            "signal_strength": 0.8,
        }
    }
    completed: list = []

    # Now tighten the volume cap so the close is rejected.
    broker._fill_model = FillModel(slippage_bps=0.0, max_volume_pct=1.0, max_volume_pct_crypto=0.001)
    broker.set_next_bar("BTC/USD", _bar(51_000.0, 51_100.0, 50_900.0, 51_050.0, 0.01))
    close_id = await broker.close_position("BTC/USD")
    assert (await broker.get_order(close_id))["status"] == "rejected"

    ts_exit = datetime(2026, 4, 20, 4, 2, tzinfo=UTC)
    await _engine()._record_trade(
        "BTC/USD", _order(close_id, "sell", 0.1), _signal(0), ts_exit,
        open_trade, completed, broker,
    )

    assert "BTC/USD" in open_trade
    assert open_trade["BTC/USD"]["exit_price"] is None
    assert completed == []
