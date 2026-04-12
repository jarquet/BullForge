import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from services.feature_engine.registry import FeatureRegistry


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
