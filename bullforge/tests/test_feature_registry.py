import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from services.feature_engine.registry import FeatureRegistry
from services.feature_engine.main import build_registry


def _make_bars_df(n: int = 50) -> pd.DataFrame:
    """Create a realistic bars DataFrame for testing."""
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


def simple_calculator(df: pd.DataFrame) -> pd.DataFrame:
    """A trivial calculator for testing the registry."""
    return pd.DataFrame({
        "close_doubled": df["close"] * 2,
    }, index=df.index)


def another_calculator(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "volume_log": np.log1p(df["volume"]),
    }, index=df.index)


def test_register_and_list():
    registry = FeatureRegistry()
    registry.register("simple", simple_calculator)
    assert "simple" in registry.list_calculators()


def test_register_duplicate_raises():
    registry = FeatureRegistry()
    registry.register("simple", simple_calculator)
    with pytest.raises(ValueError, match="already registered"):
        registry.register("simple", simple_calculator)


def test_compute_all_runs_registered_calculators():
    registry = FeatureRegistry()
    registry.register("simple", simple_calculator)
    registry.register("another", another_calculator)

    df = _make_bars_df(30)
    result = registry.compute_all(df)

    assert "close_doubled" in result.columns
    assert "volume_log" in result.columns
    assert len(result) == 30


def test_compute_all_preserves_index():
    registry = FeatureRegistry()
    registry.register("simple", simple_calculator)

    df = _make_bars_df(20)
    result = registry.compute_all(df)

    assert result.index.equals(df.index)


def test_compute_all_empty_registry():
    registry = FeatureRegistry()
    df = _make_bars_df(10)
    result = registry.compute_all(df)
    assert result.empty or len(result.columns) == 0


def test_compute_all_btc_zero_volume_window_produces_features():
    """Reproducer for the BTC/USD 1m feature_engine stall (2026-05-02).

    BTC/USD has long zero-volume minute-bar stretches on the live Alpaca feed
    (~77% of bars overall, and even higher during quiet weekend hours). When
    the entire 500-bar window pulled by feature_engine is zero-volume + flat
    price (Alpaca emits OHLC == prior close on no-trade bars), the production
    registry must still produce at least one fully-non-NaN feature row so the
    engine's `features_df.notna().all(axis=1)` gate doesn't drop everything.

    Before the fix: `vwap` was NaN for every row (cumsum(volume)=0 → 0/0=NaN),
    every row was dropped, and feature_engine logged `Computed 0 feature rows`
    every minute.
    """
    n = 200  # >> warmup periods (max is bb/sma_20=20, atr_14=14, etc.)
    timestamps = pd.date_range(
        "2026-05-02 00:54:00", periods=n, freq="1min", tz="UTC"
    )
    base = 90000.0
    close = np.full(n, base)
    df = pd.DataFrame({
        "symbol": "BTC/USD",
        "timestamp": timestamps,
        "open": close, "high": close, "low": close, "close": close,
        "volume": np.zeros(n),
    })

    registry = build_registry()
    features = registry.compute_all(df)

    # The engine drops rows where any feature is NaN. After the fix, the post-
    # warmup tail must yield at least one all-non-NaN row.
    valid_rows = features.notna().all(axis=1).sum()
    assert valid_rows >= 1, (
        f"BTC zero-volume flat window produced 0 valid feature rows; "
        f"per-column NaN counts (last row): "
        f"{features.iloc[-1][features.iloc[-1].isna()].index.tolist()}"
    )
