"""
Data-integrity guardrail tests.

These tests guard against look-ahead bias (data leakage / future-data contamination),
the #1 killer of trading ML systems. They MUST never be skipped or weakened.

Invariants tested here (Task 2):
2. No forward-label leakage into training data (test_training_data_respects_horizon_cutoff)
4. Training/test split is strictly chronological (test_time_based_split_is_chronological)

Task 3 adds invariants 1, 3, and 5.
"""
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
import pytest_asyncio

from shared.config import Settings
from shared.db import Database
from services.model_service.dataset import time_based_split


@pytest_asyncio.fixture
async def real_test_db():
    settings = Settings()
    database = Database(settings)
    await database.connect()
    # Clean relevant tables before test
    async with database.pool.acquire() as conn:
        await conn.execute("DELETE FROM features WHERE symbol = 'TEST'")
        await conn.execute("DELETE FROM labels WHERE symbol = 'TEST'")
    yield database
    async with database.pool.acquire() as conn:
        await conn.execute("DELETE FROM features WHERE symbol = 'TEST'")
        await conn.execute("DELETE FROM labels WHERE symbol = 'TEST'")
    await database.disconnect()


def test_time_based_split_is_chronological():
    # Invariant 4: chronological split
    n = 100
    ts = [datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=i) for i in range(n)]
    X = pd.DataFrame({"feat": range(n), "ts": ts})
    y = pd.Series(range(n))
    X_train, X_test, y_train, y_test = time_based_split(X, y, test_ratio=0.2)
    assert X_train["ts"].max() < X_test["ts"].min()
    assert len(X_train) + len(X_test) == n


@pytest.mark.asyncio
async def test_training_data_respects_horizon_cutoff(real_test_db):
    """
    Invariant 2: forward_return label must come from bars at (T, T+H],
    and training cutoff `as_of` must satisfy T+H <= as_of for every row.
    """
    db = real_test_db
    symbol, tf, horizon = "TEST", "1m", 5

    base = datetime(2026, 1, 1, 14, 0, tzinfo=timezone.utc)
    for i in range(20):
        ts = base + timedelta(minutes=i)
        async with db.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO features (symbol, timestamp, timeframe, features) VALUES ($1,$2,$3,$4)",
                symbol, ts, tf, '{"x": 1.0}',
            )
            await conn.execute(
                "INSERT INTO labels (symbol, timestamp, timeframe, horizon_minutes, forward_return, direction) VALUES ($1,$2,$3,$4,$5,$6)",
                symbol, ts, tf, horizon, 0.001, 1,
            )

    as_of = base + timedelta(minutes=10)
    rows = await db.query_training_data(symbol, tf, horizon, as_of=as_of, limit=1000)
    for row in rows:
        assert row["timestamp"] + timedelta(minutes=horizon) <= as_of, (
            f"Row at {row['timestamp']} violates as_of cutoff: "
            f"{row['timestamp']} + {horizon}m > {as_of}"
        )
    expected_included_cutoff = as_of - timedelta(minutes=horizon)
    included_timestamps = {r["timestamp"] for r in rows}
    assert expected_included_cutoff in included_timestamps


@pytest.mark.asyncio
async def test_query_training_data_requires_as_of(real_test_db):
    """Caller MUST pass as_of — no default allowed."""
    db = real_test_db
    with pytest.raises(TypeError):
        await db.query_training_data("TEST", "1m", 5)  # no as_of
