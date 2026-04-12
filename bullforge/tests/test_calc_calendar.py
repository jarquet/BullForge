import pytest
import pandas as pd
import numpy as np
from services.feature_engine.calculators.calendar import compute_calendar_features


def _make_bars_df_multiday() -> pd.DataFrame:
    """Create bars spanning multiple days and times for calendar feature testing."""
    # Mix of weekdays and hours
    timestamps = pd.to_datetime([
        "2026-04-06 14:00:00+00:00",  # Monday
        "2026-04-06 15:30:00+00:00",  # Monday
        "2026-04-07 12:00:00+00:00",  # Tuesday
        "2026-04-08 16:00:00+00:00",  # Wednesday
        "2026-04-09 11:30:00+00:00",  # Thursday
        "2026-04-10 13:45:00+00:00",  # Friday
    ])
    n = len(timestamps)
    np.random.seed(42)
    close = 420.0 + np.cumsum(np.random.randn(n) * 0.1)
    return pd.DataFrame({
        "symbol": "SPY",
        "timestamp": timestamps,
        "open": close - 0.1,
        "high": close + 0.2,
        "low": close - 0.2,
        "close": close,
        "volume": np.random.randint(1000, 50000, size=n),
    })


def test_calendar_features_returns_expected_columns():
    df = _make_bars_df_multiday()
    result = compute_calendar_features(df)
    expected_cols = [
        "day_of_week",
        "hour_of_day",
        "minute_of_hour",
        "is_market_open_first_30",
        "is_market_close_last_30",
    ]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"


def test_day_of_week_encoding():
    df = _make_bars_df_multiday()
    result = compute_calendar_features(df)
    # Monday=0, Tuesday=1, ..., Friday=4
    assert result["day_of_week"].iloc[0] == 0  # Monday
    assert result["day_of_week"].iloc[2] == 1  # Tuesday
    assert result["day_of_week"].iloc[5] == 4  # Friday


def test_hour_of_day():
    df = _make_bars_df_multiday()
    result = compute_calendar_features(df)
    assert result["hour_of_day"].iloc[0] == 14
    assert result["hour_of_day"].iloc[1] == 15
    assert result["hour_of_day"].iloc[2] == 12


def test_market_open_first_30():
    """Bars between 14:30-15:00 UTC (9:30-10:00 ET) should be flagged."""
    timestamps = pd.to_datetime([
        "2026-04-10 14:30:00+00:00",  # market open
        "2026-04-10 14:45:00+00:00",  # first 30 min
        "2026-04-10 15:00:00+00:00",  # just outside
        "2026-04-10 16:00:00+00:00",  # mid-day
    ])
    df = pd.DataFrame({
        "symbol": "SPY",
        "timestamp": timestamps,
        "open": [420.0] * 4,
        "high": [421.0] * 4,
        "low": [419.0] * 4,
        "close": [420.5] * 4,
        "volume": [10000] * 4,
    })
    result = compute_calendar_features(df)
    assert result["is_market_open_first_30"].iloc[0] == 1
    assert result["is_market_open_first_30"].iloc[1] == 1
    assert result["is_market_open_first_30"].iloc[2] == 0
    assert result["is_market_open_first_30"].iloc[3] == 0


def test_market_close_last_30():
    """Bars between 20:30-21:00 UTC (3:30-4:00 PM ET) should be flagged."""
    timestamps = pd.to_datetime([
        "2026-04-10 19:00:00+00:00",  # mid-day
        "2026-04-10 20:30:00+00:00",  # last 30 min
        "2026-04-10 20:45:00+00:00",  # last 30 min
        "2026-04-10 20:59:00+00:00",  # last 30 min
    ])
    df = pd.DataFrame({
        "symbol": "SPY",
        "timestamp": timestamps,
        "open": [420.0] * 4,
        "high": [421.0] * 4,
        "low": [419.0] * 4,
        "close": [420.5] * 4,
        "volume": [10000] * 4,
    })
    result = compute_calendar_features(df)
    assert result["is_market_close_last_30"].iloc[0] == 0
    assert result["is_market_close_last_30"].iloc[1] == 1
    assert result["is_market_close_last_30"].iloc[2] == 1
    assert result["is_market_close_last_30"].iloc[3] == 1


def test_calendar_features_preserves_length():
    df = _make_bars_df_multiday()
    result = compute_calendar_features(df)
    assert len(result) == len(df)
