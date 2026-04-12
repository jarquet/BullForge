import pytest
import pandas as pd
import numpy as np
from services.feature_engine.calculators.price import compute_price_features


def _make_bars_df(n: int = 60) -> pd.DataFrame:
    timestamps = pd.date_range(
        start="2026-04-10 14:00:00", periods=n, freq="1min", tz="UTC"
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


def test_price_features_returns_expected_columns():
    df = _make_bars_df()
    result = compute_price_features(df)
    expected_cols = [
        "return_1", "return_5", "return_10",
        "log_return_1",
        "volatility_10", "volatility_20",
        "high_low_spread", "close_open_spread",
    ]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"


def test_price_features_return_1_is_pct_change():
    df = _make_bars_df(30)
    result = compute_price_features(df)
    expected = df["close"].pct_change(1)
    pd.testing.assert_series_equal(
        result["return_1"], expected, check_names=False
    )


def test_price_features_log_return():
    df = _make_bars_df(30)
    result = compute_price_features(df)
    expected = np.log(df["close"] / df["close"].shift(1))
    pd.testing.assert_series_equal(
        result["log_return_1"], expected, check_names=False
    )


def test_price_features_volatility_is_rolling_std():
    df = _make_bars_df(30)
    result = compute_price_features(df)
    returns = df["close"].pct_change(1)
    expected = returns.rolling(window=10).std()
    pd.testing.assert_series_equal(
        result["volatility_10"], expected, check_names=False
    )


def test_price_features_spread():
    df = _make_bars_df(30)
    result = compute_price_features(df)
    expected_hl = (df["high"] - df["low"]) / df["close"]
    pd.testing.assert_series_equal(
        result["high_low_spread"], expected_hl, check_names=False
    )


def test_price_features_preserves_length():
    df = _make_bars_df(50)
    result = compute_price_features(df)
    assert len(result) == len(df)
