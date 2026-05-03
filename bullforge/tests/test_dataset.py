import json
import pytest
import pytest_asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock
from services.model_service.dataset import DatasetBuilder, time_based_split


def _make_training_rows(n: int = 100) -> list[dict]:
    base = datetime(2026, 4, 10, 14, 0, 0, tzinfo=timezone.utc)
    np.random.seed(42)
    rows = []
    for i in range(n):
        features = {
            "sma_20": float(420 + np.random.randn()),
            "rsi_14": float(50 + np.random.randn() * 10),
            "return_1": float(np.random.randn() * 0.001),
            "volume_ratio_10": float(1.0 + np.random.randn() * 0.2),
        }
        direction = int(np.random.choice([-1, 0, 1]))
        fwd_ret = float(np.random.randn() * 0.002)
        rows.append({
            "timestamp": base + timedelta(minutes=i),
            "features": json.dumps(features),
            "direction": direction,
            "forward_return": fwd_ret,
        })
    return rows


@pytest.mark.asyncio
async def test_dataset_builder_builds_dataframe():
    mock_db = AsyncMock()
    mock_db.query_training_data = AsyncMock(return_value=_make_training_rows(100))

    builder = DatasetBuilder(mock_db, min_samples=50)
    result = await builder.build("SPY", "1m", 5, as_of=datetime(2026, 4, 13, tzinfo=timezone.utc), limit=1000)

    assert result is not None
    X, y = result
    assert isinstance(X, pd.DataFrame)
    assert isinstance(y, pd.Series)
    assert len(X) == 100
    assert len(y) == 100
    assert "sma_20" in X.columns
    assert "rsi_14" in X.columns
    assert "return_1" in X.columns
    assert "volume_ratio_10" in X.columns
    assert set(y.unique()).issubset({-1, 0, 1})


@pytest.mark.asyncio
async def test_dataset_builder_returns_none_insufficient_data():
    mock_db = AsyncMock()
    mock_db.query_training_data = AsyncMock(return_value=_make_training_rows(5))

    builder = DatasetBuilder(mock_db, min_samples=50)
    result = await builder.build("SPY", "1m", 5, as_of=datetime(2026, 4, 13, tzinfo=timezone.utc))

    assert result is None


@pytest.mark.asyncio
async def test_dataset_builder_returns_none_empty():
    mock_db = AsyncMock()
    mock_db.query_training_data = AsyncMock(return_value=[])

    builder = DatasetBuilder(mock_db, min_samples=500)
    result = await builder.build("SPY", "1m", 5, as_of=datetime(2026, 4, 13, tzinfo=timezone.utc))

    assert result is None


@pytest.mark.asyncio
async def test_dataset_builder_handles_json_string_features():
    mock_db = AsyncMock()
    rows = [
        {
            "timestamp": datetime(2026, 4, 10, 14, i, 0, tzinfo=timezone.utc),
            "features": json.dumps({"sma_20": 420.0 + i, "rsi_14": 50.0}),
            "direction": 1,
            "forward_return": 0.001,
        }
        for i in range(20)
    ]
    mock_db.query_training_data = AsyncMock(return_value=rows)

    builder = DatasetBuilder(mock_db, min_samples=10)
    result = await builder.build("SPY", "1m", 5, as_of=datetime(2026, 4, 13, tzinfo=timezone.utc))

    assert result is not None
    X, y = result
    assert len(X) == 20
    assert X["sma_20"].iloc[0] == 420.0


def _make_rows_at_timestamps(timestamps, *, direction: int = 1) -> list[dict]:
    return [
        {
            "timestamp": ts,
            "features": json.dumps({"sma_20": 420.0, "rsi_14": 50.0}),
            "direction": direction,
            "forward_return": 0.001,
        }
        for ts in timestamps
    ]


@pytest.mark.asyncio
async def test_dataset_builder_filters_exclusion_zone():
    """When buffer minutes are set, rows whose target timestamp lands in the
    equity exclusion zone are dropped before threshold + NaN cleanup."""
    # Monday 2026-04-13. EDT = UTC-4. NYSE 09:30 ET = 13:30 UTC.
    base = datetime(2026, 4, 13, 13, 30, 0, tzinfo=timezone.utc)
    # 120 rows, +0..+119 minutes → 13:30 to 15:29 UTC.
    # Horizon = 10m, so target = ts + 10m. Open buffer = 60m → window opens
    # at 14:30 UTC. Excluded if target < 14:30 UTC ⟺ ts < 14:20 UTC ⟺
    # ts is in the first 50 minutes (rows 0..49). Rows 50..119 (70 rows)
    # have target >= 14:30 UTC and survive. Max target is 15:39 UTC, well
    # before the close-buffer end at 19:00 UTC, so close-buffer is irrelevant.
    timestamps = [base + timedelta(minutes=i) for i in range(120)]
    rows = _make_rows_at_timestamps(timestamps)

    mock_db = AsyncMock()
    mock_db.query_training_data = AsyncMock(return_value=rows)

    builder = DatasetBuilder(mock_db, min_samples=50)
    result = await builder.build(
        "SPY", "1m", 10,
        as_of=datetime(2026, 4, 14, tzinfo=timezone.utc),
        open_buffer_min=60, close_buffer_min=60,
    )

    assert result is not None
    X, y = result
    assert len(X) == 70
    assert len(y) == 70


@pytest.mark.asyncio
async def test_dataset_builder_no_filter_when_buffers_none():
    """No filter applied when buffer kwargs are absent (backward compat for
    existing tests / mocked Trainer paths that don't provide a runtime
    config)."""
    rows = _make_training_rows(100)
    mock_db = AsyncMock()
    mock_db.query_training_data = AsyncMock(return_value=rows)

    builder = DatasetBuilder(mock_db, min_samples=50)
    result = await builder.build(
        "SPY", "1m", 5,
        as_of=datetime(2026, 4, 13, tzinfo=timezone.utc),
    )

    assert result is not None
    X, _ = result
    assert len(X) == 100


@pytest.mark.asyncio
async def test_dataset_builder_crypto_no_filter_even_with_buffers():
    """Crypto symbols bypass the equity exclusion zone — every row survives
    even on a Saturday with buffers set."""
    # Saturday 2026-04-11 — equity would be 100% excluded by the weekend rule.
    base = datetime(2026, 4, 11, 3, 0, 0, tzinfo=timezone.utc)
    timestamps = [base + timedelta(minutes=i) for i in range(60)]
    rows = _make_rows_at_timestamps(timestamps)

    mock_db = AsyncMock()
    mock_db.query_training_data = AsyncMock(return_value=rows)

    builder = DatasetBuilder(mock_db, min_samples=50)
    result = await builder.build(
        "BTC/USD", "1m", 10,
        as_of=datetime(2026, 4, 14, tzinfo=timezone.utc),
        open_buffer_min=60, close_buffer_min=60,
    )

    assert result is not None
    X, _ = result
    assert len(X) == 60


@pytest.mark.asyncio
async def test_dataset_builder_filter_returns_none_when_below_threshold():
    """If filtering drops below min_samples, build returns None."""
    base = datetime(2026, 4, 13, 13, 30, 0, tzinfo=timezone.utc)
    # 50 rows in the open buffer (rows 0..49 from the prior arithmetic).
    timestamps = [base + timedelta(minutes=i) for i in range(50)]
    rows = _make_rows_at_timestamps(timestamps)

    mock_db = AsyncMock()
    mock_db.query_training_data = AsyncMock(return_value=rows)

    builder = DatasetBuilder(mock_db, min_samples=50)
    result = await builder.build(
        "SPY", "1m", 10,
        as_of=datetime(2026, 4, 14, tzinfo=timezone.utc),
        open_buffer_min=60, close_buffer_min=60,
    )

    # All 50 filtered out → 0 < threshold → None.
    assert result is None


def test_time_based_split():
    np.random.seed(42)
    X = pd.DataFrame({
        "a": np.random.randn(100),
        "b": np.random.randn(100),
    })
    y = pd.Series(np.random.choice([-1, 0, 1], 100))

    X_train, X_test, y_train, y_test = time_based_split(X, y, test_ratio=0.2)

    assert len(X_train) == 80
    assert len(X_test) == 20
    assert len(y_train) == 80
    assert len(y_test) == 20
    # Train indices come before test indices (time-based)
    assert X_train.index[-1] < X_test.index[0]


def test_time_based_split_preserves_data():
    X = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    y = pd.Series([0, 1, 0, 1, 0])

    X_train, X_test, y_train, y_test = time_based_split(X, y, test_ratio=0.4)

    assert len(X_train) == 3
    assert len(X_test) == 2
    assert list(X_train["a"]) == [1, 2, 3]
    assert list(X_test["a"]) == [4, 5]
