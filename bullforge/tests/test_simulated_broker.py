import pytest
from datetime import datetime, timezone

from services.backtest_service.fill_model import FillModel
from services.backtest_service.simulated_broker import SimulatedBroker


UTC = timezone.utc


def _bar(o, h, l, c, v):
    return {"open": o, "high": h, "low": l, "close": c, "volume": v}


@pytest.fixture
def broker():
    fm = FillModel(slippage_bps=0.0, max_volume_pct=1.0)
    b = SimulatedBroker(starting_cash=100_000.0, fill_model=fm)
    b.set_clock(datetime(2025, 1, 2, 15, 0, tzinfo=UTC))
    b.set_next_bar("SPY", _bar(100.0, 101.0, 99.5, 100.5, 1_000_000))
    return b


@pytest.mark.asyncio
async def test_initial_account():
    fm = FillModel(slippage_bps=0.0, max_volume_pct=1.0)
    b = SimulatedBroker(starting_cash=100_000.0, fill_model=fm)
    acct = await b.get_account()
    assert acct["cash"] == 100_000.0
    assert acct["equity"] == 100_000.0
    assert acct["buying_power"] == 100_000.0
    assert acct["portfolio_value"] == 100_000.0


@pytest.mark.asyncio
async def test_buy_creates_position_and_deducts_cash(broker):
    order_id = await broker.submit_market_order("SPY", 100, "buy")
    assert isinstance(order_id, str) and len(order_id) > 0

    snap = await broker.get_order(order_id)
    assert snap["status"] == "filled"
    assert snap["filled_qty"] == 100
    assert snap["filled_avg_price"] == pytest.approx(100.0)

    pos = await broker.get_position("SPY")
    assert pos is not None
    assert pos["qty"] == 100
    assert pos["avg_entry_price"] == pytest.approx(100.0)
    assert pos["side"] == "long"

    acct = await broker.get_account()
    assert acct["cash"] == pytest.approx(100_000.0 - 100 * 100.0)


@pytest.mark.asyncio
async def test_close_position_realizes_pnl(broker):
    await broker.submit_market_order("SPY", 100, "buy")

    broker.set_next_bar("SPY", _bar(105.0, 106.0, 104.5, 105.5, 1_000_000))
    close_id = await broker.close_position("SPY")
    assert close_id is not None

    pos = await broker.get_position("SPY")
    assert pos is None

    acct = await broker.get_account()
    assert acct["cash"] == pytest.approx(100_000.0 + 500.0)


@pytest.mark.asyncio
async def test_rejected_order_when_no_volume(broker):
    fm = FillModel(slippage_bps=0.0, max_volume_pct=0.001)
    b = SimulatedBroker(starting_cash=100_000.0, fill_model=fm)
    b.set_clock(datetime(2025, 1, 2, 15, 0, tzinfo=UTC))
    b.set_next_bar("SPY", _bar(100.0, 101.0, 99.5, 100.5, 1_000))

    order_id = await b.submit_market_order("SPY", 500, "buy")
    snap = await b.get_order(order_id)
    assert snap["status"] == "rejected"
    pos = await b.get_position("SPY")
    assert pos is None


@pytest.mark.asyncio
async def test_mark_to_market_updates_unrealized_pnl(broker):
    await broker.submit_market_order("SPY", 100, "buy")
    broker.mark_to_market("SPY", price=110.0)
    pos = await broker.get_position("SPY")
    assert pos["current_price"] == 110.0
    assert pos["unrealized_pl"] == pytest.approx(1000.0)
    assert pos["market_value"] == pytest.approx(110.0 * 100)


@pytest.mark.asyncio
async def test_reversal_resets_avg_entry_price(broker):
    await broker.submit_market_order("SPY", 100, "buy")  # long 100 @ 100
    # New bar at higher price
    broker.set_next_bar("SPY", _bar(110.0, 111.0, 109.5, 110.5, 1_000_000))
    await broker.submit_market_order("SPY", 150, "sell")  # crosses flat to short 50

    pos = await broker.get_position("SPY")
    assert pos is not None
    assert pos["qty"] == -50
    assert pos["side"] == "short"
    assert pos["avg_entry_price"] == pytest.approx(110.0)  # residual opens at 110, not 100
