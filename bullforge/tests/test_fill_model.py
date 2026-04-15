import pytest

from services.backtest_service.fill_model import FillModel, FillRequest


def _bar(o, h, l, c, v):
    return {"open": o, "high": h, "low": l, "close": c, "volume": v}


def test_fill_at_next_bar_open_with_buy_slippage():
    fm = FillModel(slippage_bps=10.0, max_volume_pct=1.0)
    next_bar = _bar(100.0, 101.0, 99.5, 100.5, 10_000)
    fill = fm.fill(FillRequest(side="buy", qty=100), next_bar)
    assert fill.filled is True
    assert fill.price == pytest.approx(100.0 * 1.001)
    assert fill.qty == 100


def test_fill_at_next_bar_open_with_sell_slippage():
    fm = FillModel(slippage_bps=10.0, max_volume_pct=1.0)
    next_bar = _bar(100.0, 101.0, 99.5, 100.5, 10_000)
    fill = fm.fill(FillRequest(side="sell", qty=100), next_bar)
    assert fill.price == pytest.approx(100.0 * 0.999)


def test_rejects_when_qty_exceeds_volume_cap():
    fm = FillModel(slippage_bps=0.0, max_volume_pct=0.10)
    next_bar = _bar(100.0, 101.0, 99.5, 100.5, 1_000)
    fill = fm.fill(FillRequest(side="buy", qty=500), next_bar)
    assert fill.filled is False
    assert fill.qty == 0
    assert "volume" in fill.reason.lower()


def test_partial_volume_cap_disabled_when_pct_is_one():
    fm = FillModel(slippage_bps=0.0, max_volume_pct=1.0)
    next_bar = _bar(100.0, 101.0, 99.5, 100.5, 50)
    fill = fm.fill(FillRequest(side="buy", qty=50), next_bar)
    assert fill.filled is True
    assert fill.qty == 50
