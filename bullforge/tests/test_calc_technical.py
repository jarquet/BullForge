import pytest
import pandas as pd
import numpy as np
from services.feature_engine.calculators.technical import compute_technical_features


def _make_bars_df(n: int = 100) -> pd.DataFrame:
    timestamps = pd.date_range(
        start="2026-04-10 12:00:00", periods=n, freq="1min", tz="UTC"
    )
    np.random.seed(42)
    close = 420.0 + np.cumsum(np.random.randn(n) * 0.1)
    return pd.DataFrame({
        "symbol": "SPY",
        "timestamp": timestamps,
        "open": close - np.random.rand(n) * 0.2,
        "high": close + np.random.rand(n) * 0.3,
        "low": close - np.random.rand(n) * 0.3,
        "close": close,
        "volume": np.random.randint(1000, 50000, size=n),
    })


def test_technical_features_returns_expected_columns():
    df = _make_bars_df()
    result = compute_technical_features(df)
    expected_cols = [
        "sma_10", "sma_20", "sma_50",
        "ema_10", "ema_20",
        "rsi_14",
        "macd", "macd_signal", "macd_histogram",
        "bb_upper", "bb_middle", "bb_lower", "bb_width",
        "atr_14",
    ]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"


def test_sma_is_rolling_mean():
    df = _make_bars_df(50)
    result = compute_technical_features(df)
    expected = df["close"].rolling(window=10).mean()
    pd.testing.assert_series_equal(
        result["sma_10"], expected, check_names=False
    )


def test_ema_uses_ewm():
    df = _make_bars_df(50)
    result = compute_technical_features(df)
    expected = df["close"].ewm(span=10, adjust=False).mean()
    pd.testing.assert_series_equal(
        result["ema_10"], expected, check_names=False
    )


def test_rsi_bounded_0_100():
    df = _make_bars_df()
    result = compute_technical_features(df)
    rsi = result["rsi_14"].dropna()
    assert (rsi >= 0).all()
    assert (rsi <= 100).all()


def test_bollinger_bands_relation():
    """Upper > middle > lower for all non-NaN rows."""
    df = _make_bars_df()
    result = compute_technical_features(df)
    valid = result.dropna(subset=["bb_upper", "bb_middle", "bb_lower"])
    assert (valid["bb_upper"] >= valid["bb_middle"]).all()
    assert (valid["bb_middle"] >= valid["bb_lower"]).all()


def test_bollinger_width_positive():
    df = _make_bars_df()
    result = compute_technical_features(df)
    valid = result["bb_width"].dropna()
    assert (valid >= 0).all()


def test_atr_positive():
    df = _make_bars_df()
    result = compute_technical_features(df)
    valid = result["atr_14"].dropna()
    assert (valid >= 0).all()


def test_macd_histogram_is_macd_minus_signal():
    df = _make_bars_df()
    result = compute_technical_features(df)
    valid = result.dropna(subset=["macd", "macd_signal", "macd_histogram"])
    expected = valid["macd"] - valid["macd_signal"]
    pd.testing.assert_series_equal(
        valid["macd_histogram"], expected, check_names=False
    )


def test_technical_features_preserves_length():
    df = _make_bars_df(80)
    result = compute_technical_features(df)
    assert len(result) == len(df)


def test_rsi_is_neutral_on_flat_market():
    """Crypto minute bars (BTC/USD) on no-trade minutes are emitted by Alpaca
    with OHLC == prior close. After a stretch of such bars, both avg_gain and
    avg_loss in the RSI EWM are exactly 0.0, rs = 0/0 = NaN, and RSI inherits
    the NaN — which then tanks every row at engine.py's notna().all(axis=1)
    gate. Convention is RSI=50 (neutral) for a flat market.

    Warmup rows (before EWM has min_periods worth of data) must still be NaN
    so engine.py's warmup drop remains correct.
    """
    n = 60
    timestamps = pd.date_range(
        "2026-05-02 03:14:00", periods=n, freq="1min", tz="UTC"
    )
    close = np.full(n, 90000.0)
    df = pd.DataFrame({
        "symbol": "BTC/USD",
        "timestamp": timestamps,
        "open": close, "high": close, "low": close, "close": close,
        "volume": np.zeros(n),
    })

    result = compute_technical_features(df)
    rsi = result["rsi_14"]

    # Post-warmup (index >= 14) must be 50.0 exactly.
    post_warmup = rsi.iloc[14:]
    assert not post_warmup.isna().any(), (
        f"rsi_14 has NaN post-warmup: {post_warmup[post_warmup.isna()].index.tolist()}"
    )
    assert (post_warmup == 50.0).all(), (
        f"rsi_14 post-warmup values: {post_warmup.unique()} (want 50.0)"
    )
