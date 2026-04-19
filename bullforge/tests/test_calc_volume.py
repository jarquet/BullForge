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


def test_volume_ratio_is_neutral_when_rolling_window_is_all_zero():
    """Crypto minute bars have long stretches (~77% for BTC/USD) where volume=0.
    When the rolling window is entirely zero-volume, volume / rolling.mean()
    yields NaN (0/0) or inf (v>0/0) — either tanks the row at engine.py's
    notna().all(axis=1) gate. Guard the divisor so the ratio becomes 1.0
    (neutral) for those bars instead.

    Warmup rows (before the rolling window is full) must still be NaN so
    engine.py's warmup drop remains correct.
    """
    n = 40
    timestamps = pd.date_range(
        "2026-04-10 00:00:00", periods=n, freq="1min", tz="UTC"
    )
    close = 60000.0 + np.arange(n) * 10.0
    # Indices 10..29 are zero-volume; surroundings have volume.
    volume = np.array([100.0] * 10 + [0.0] * 20 + [100.0] * 10)
    df = pd.DataFrame({
        "symbol": "BTC/USD",
        "timestamp": timestamps,
        "open": close, "high": close, "low": close,
        "close": close, "volume": volume,
    })

    result = compute_volume_features(df)

    # Indices 19..29: rolling(10) window is entirely zeros → divisor=0.
    for idx in range(19, 30):
        val = result["volume_ratio_10"].iloc[idx]
        assert not np.isnan(val), f"volume_ratio_10[{idx}] is NaN"
        assert not np.isinf(val), f"volume_ratio_10[{idx}] is inf"
        assert val == 1.0, f"volume_ratio_10[{idx}] = {val}, want 1.0"

    # Indices 29..29: rolling(20) window covers indices 10..29 → all zeros.
    val_20 = result["volume_ratio_20"].iloc[29]
    assert val_20 == 1.0, f"volume_ratio_20[29] = {val_20}, want 1.0"

    # Warmup for rolling(10) — indices 0..8 — must remain NaN.
    for idx in range(0, 9):
        assert np.isnan(result["volume_ratio_10"].iloc[idx]), (
            f"volume_ratio_10[{idx}] should be NaN (warmup)"
        )
