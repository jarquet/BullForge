import pytest

from services.backtest_service.fill_model import FillModel, FillRequest


def _bar(o, h, l, c, v):
    return {"open": o, "high": h, "low": l, "close": c, "volume": v}


def test_fill_at_next_bar_open_with_buy_slippage():
    fm = FillModel(slippage_bps=10.0, max_volume_pct=1.0)
    next_bar = _bar(100.0, 101.0, 99.5, 100.5, 10_000)
    fill = fm.fill(FillRequest(side="buy", qty=100), next_bar, symbol="SPY")
    assert fill.filled is True
    assert fill.price == pytest.approx(100.0 * 1.001)
    assert fill.qty == 100


def test_fill_at_next_bar_open_with_sell_slippage():
    fm = FillModel(slippage_bps=10.0, max_volume_pct=1.0)
    next_bar = _bar(100.0, 101.0, 99.5, 100.5, 10_000)
    fill = fm.fill(FillRequest(side="sell", qty=100), next_bar, symbol="SPY")
    assert fill.price == pytest.approx(100.0 * 0.999)


def test_rejects_when_qty_exceeds_volume_cap():
    fm = FillModel(slippage_bps=0.0, max_volume_pct=0.10)
    next_bar = _bar(100.0, 101.0, 99.5, 100.5, 1_000)
    fill = fm.fill(FillRequest(side="buy", qty=500), next_bar, symbol="SPY")
    assert fill.filled is False
    assert fill.qty == 0
    assert "volume" in fill.reason.lower()


def test_partial_volume_cap_disabled_when_pct_is_one():
    fm = FillModel(slippage_bps=0.0, max_volume_pct=1.0)
    next_bar = _bar(100.0, 101.0, 99.5, 100.5, 50)
    fill = fm.fill(FillRequest(side="buy", qty=50), next_bar, symbol="SPY")
    assert fill.filled is True
    assert fill.qty == 50


# ----- asset-class-aware cap (Task 2) -----------------------------------


def test_equity_and_crypto_get_different_caps():
    """Same FillModel + same bar volume; an order qty between the equity cap
    (0.10 * volume) and the crypto cap (0.25 * volume) must be rejected for
    SPY but accepted for BTC/USD."""
    fm = FillModel(
        slippage_bps=0.0,
        max_volume_pct=0.10,
        max_volume_pct_crypto=0.25,
    )
    next_bar = _bar(100.0, 101.0, 99.5, 100.5, 1_000)

    # qty=200 → 20% of bar volume.
    # Equity cap (0.10 * 1000 = 100): 200 > 100 → rejected.
    # Crypto cap (0.25 * 1000 = 250): 200 ≤ 250 → filled.
    spy_fill = fm.fill(FillRequest(side="buy", qty=200), next_bar, symbol="SPY")
    btc_fill = fm.fill(FillRequest(side="buy", qty=200), next_bar, symbol="BTC/USD")

    assert spy_fill.filled is False
    assert "volume" in spy_fill.reason.lower()
    assert btc_fill.filled is True
    assert btc_fill.qty == 200


def test_crypto_cap_can_still_reject_when_qty_exceeds_crypto_cap():
    fm = FillModel(
        slippage_bps=0.0,
        max_volume_pct=0.10,
        max_volume_pct_crypto=0.25,
    )
    next_bar = _bar(50_000.0, 50_100.0, 49_900.0, 50_050.0, 1.0)
    # 0.5 BTC > 0.25 cap → rejected even on the looser crypto cap.
    fill = fm.fill(FillRequest(side="buy", qty=0.5), next_bar, symbol="BTC/USD")
    assert fill.filled is False
    assert "volume" in fill.reason.lower()


def test_legacy_constructor_without_crypto_cap_still_works():
    """Backwards-compat: callers that don't pass max_volume_pct_crypto must
    still construct successfully; crypto orders fall back to a sensible
    default cap (matching the BacktestConfig default)."""
    fm = FillModel(slippage_bps=0.0, max_volume_pct=0.10)
    # SPY uses the equity cap as before.
    spy_bar = _bar(100.0, 101.0, 99.5, 100.5, 1_000)
    spy_fill = fm.fill(FillRequest(side="buy", qty=50), spy_bar, symbol="SPY")
    assert spy_fill.filled is True

    # BTC/USD uses the default crypto cap (0.25). 0.2 BTC of a 1.0 BTC bar
    # is within 0.25 → filled. (Would have been rejected at 0.10.)
    btc_bar = _bar(50_000.0, 50_100.0, 49_900.0, 50_050.0, 1.0)
    btc_fill = fm.fill(
        FillRequest(side="buy", qty=0.2), btc_bar, symbol="BTC/USD",
    )
    assert btc_fill.filled is True


def test_crypto_cap_validation_matches_equity_cap():
    """The (0, 1] validation extends to the new field."""
    with pytest.raises(ValueError, match="max_volume_pct_crypto"):
        FillModel(
            slippage_bps=0.0, max_volume_pct=0.10, max_volume_pct_crypto=0.0,
        )
    with pytest.raises(ValueError, match="max_volume_pct_crypto"):
        FillModel(
            slippage_bps=0.0, max_volume_pct=0.10, max_volume_pct_crypto=1.5,
        )
