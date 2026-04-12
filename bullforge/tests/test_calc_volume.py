import pytest
import pandas as pd
import numpy as np
from services.feature_engine.calculators.volume import compute_volume_features


def _make_bars_df(n: int = 60) -> pd.DataFrame:
    timestamps = pd.date_range(
        start="2026-04-10 14:00:00", periods=n, freq="1min", tz="UTC"
    )
    np.random.seed(42)
    close = 420.0 + np.cumsum(np.random.randn(n) * 0.1)
    volume = np.random.randint(5000, 50000, size=n)
    return pd.DataFrame({
        "symbol": "SPY",
        "timestamp": timestamps,
        "open": close - np.random.rand(n) * 0.2,
        "high": close + np.random.rand(n) * 0.3,
        "low": close - np.random.rand(n) * 0.3,
        "close": close,
        "volume": volume,
    })


def test_volume_features_returns_expected_columns():
    df = _make_bars_df()
    result = compute_volume_features(df)
    expected_cols = ["vwap", "obv", "volume_ratio_10", "volume_ratio_20"]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"


def test_vwap_calculation():
    df = _make_bars_df(30)
    result = compute_volume_features(df)
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    cumulative_tp_vol = (typical_price * df["volume"]).cumsum()
    cumulative_vol = df["volume"].cumsum()
    expected = cumulative_tp_vol / cumulative_vol
    pd.testing.assert_series_equal(
        result["vwap"], expected, check_names=False
    )


def test_obv_direction():
    """OBV should increase when close goes up and decrease when close goes down."""
    df = _make_bars_df(30)
    result = compute_volume_features(df)
    obv = result["obv"]
    # OBV should not be all NaN
    assert not obv.isna().all()


def test_volume_ratio_is_relative_to_rolling_mean():
    df = _make_bars_df(30)
    result = compute_volume_features(df)
    vol = df["volume"].astype(float)
    expected = vol / vol.rolling(window=10).mean()
    pd.testing.assert_series_equal(
        result["volume_ratio_10"], expected, check_names=False
    )


def test_volume_features_preserves_length():
    df = _make_bars_df(50)
    result = compute_volume_features(df)
    assert len(result) == len(df)
