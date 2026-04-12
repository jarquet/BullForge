import pytest
import pandas as pd
import numpy as np
from services.feature_engine.labels import compute_forward_return_labels


def _make_bars_df(n: int = 100) -> pd.DataFrame:
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


def test_forward_return_labels_expected_columns():
    df = _make_bars_df()
    result = compute_forward_return_labels(df, horizons=[5, 10, 30])
    expected_cols = [
        "fwd_return_5", "fwd_direction_5",
        "fwd_return_10", "fwd_direction_10",
        "fwd_return_30", "fwd_direction_30",
    ]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"


def test_forward_return_is_future_pct_change():
    df = _make_bars_df(50)
    result = compute_forward_return_labels(df, horizons=[5])
    close = df["close"]
    expected = close.shift(-5) / close - 1.0
    pd.testing.assert_series_equal(
        result["fwd_return_5"], expected, check_names=False
    )


def test_forward_direction_sign():
    df = _make_bars_df(50)
    result = compute_forward_return_labels(df, horizons=[5], threshold=0.0)
    # direction should match sign of return (with 0 threshold, no flat zone)
    valid = result.dropna(subset=["fwd_return_5"])
    for idx in valid.index:
        ret = valid.loc[idx, "fwd_return_5"]
        direction = valid.loc[idx, "fwd_direction_5"]
        if ret > 0:
            assert direction == 1
        elif ret < 0:
            assert direction == -1
        else:
            assert direction == 0


def test_forward_direction_threshold():
    """Returns within threshold should be labeled as 0 (flat)."""
    df = _make_bars_df(50)
    result = compute_forward_return_labels(df, horizons=[5], threshold=0.01)
    valid = result.dropna(subset=["fwd_return_5"])
    for idx in valid.index:
        ret = valid.loc[idx, "fwd_return_5"]
        direction = valid.loc[idx, "fwd_direction_5"]
        if abs(ret) <= 0.01:
            assert direction == 0


def test_forward_return_nans_at_tail():
    """Last `horizon` rows should be NaN (no future data available)."""
    df = _make_bars_df(50)
    result = compute_forward_return_labels(df, horizons=[10])
    tail = result["fwd_return_10"].iloc[-10:]
    assert tail.isna().all()


def test_multiple_horizons():
    df = _make_bars_df()
    result = compute_forward_return_labels(df, horizons=[5, 10, 30])
    # Different horizons should produce different NaN tails
    assert result["fwd_return_5"].iloc[-5:].isna().all()
    assert result["fwd_return_10"].iloc[-10:].isna().all()
    assert result["fwd_return_30"].iloc[-30:].isna().all()
    # But earlier rows should have values for all
    assert result["fwd_return_5"].iloc[0:50].notna().all()


def test_preserves_length():
    df = _make_bars_df(80)
    result = compute_forward_return_labels(df, horizons=[5, 10])
    assert len(result) == len(df)
