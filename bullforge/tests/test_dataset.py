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

    builder = DatasetBuilder(mock_db)
    result = await builder.build("SPY", "1m", 5, limit=1000)

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
    result = await builder.build("SPY", "1m", 5)

    assert result is None


@pytest.mark.asyncio
async def test_dataset_builder_returns_none_empty():
    mock_db = AsyncMock()
    mock_db.query_training_data = AsyncMock(return_value=[])

    builder = DatasetBuilder(mock_db)
    result = await builder.build("SPY", "1m", 5)

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
    result = await builder.build("SPY", "1m", 5)

    assert result is not None
    X, y = result
    assert len(X) == 20
    assert X["sma_20"].iloc[0] == 420.0


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
