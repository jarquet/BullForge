import pytest
import pytest_asyncio
from datetime import datetime, timedelta, timezone

from shared.config import Settings
from shared.db import Database
from shared.models import ModelMetadata

pytestmark = pytest.mark.services  # needs TimescaleDB


@pytest_asyncio.fixture
async def db():
    settings = Settings()
    database = Database(settings)
    await database.connect()
    async with database.pool.acquire() as conn:
        await conn.execute("DELETE FROM model_status")
        await conn.execute("DELETE FROM model_registry")
        await conn.execute("DELETE FROM bars_1m")
    yield database
    async with database.pool.acquire() as conn:
        await conn.execute("DELETE FROM model_status")
        await conn.execute("DELETE FROM model_registry")
        await conn.execute("DELETE FROM bars_1m")
    await database.disconnect()


@pytest.mark.asyncio
async def test_query_latest_bar_timestamps(db):
    base = datetime(2026, 4, 13, 14, 0, tzinfo=timezone.utc)
    async with db.pool.acquire() as conn:
        for symbol, offset in [("SPY", 0), ("QQQ", 2), ("AAPL", 5)]:
            await conn.execute(
                "INSERT INTO bars_1m "
                "(symbol, timestamp, open, high, low, close, volume) "
                "VALUES ($1, $2, 100, 100, 100, 100, 1)",
                symbol, base + timedelta(minutes=offset),
            )
    result = await db.query_latest_bar_timestamps()
    d = {r["symbol"]: r["last_bar"] for r in result}
    assert d["SPY"] == base
    assert d["AAPL"] == base + timedelta(minutes=5)


@pytest.mark.asyncio
async def test_query_symbol_bar_stats(db):
    base = datetime(2026, 4, 13, 14, 0, tzinfo=timezone.utc)
    async with db.pool.acquire() as conn:
        for i in range(3):
            await conn.execute(
                "INSERT INTO bars_1m "
                "(symbol, timestamp, open, high, low, close, volume) "
                "VALUES ($1, $2, 100, 100, 100, 100, 1)",
                "SPY", base + timedelta(minutes=i),
            )
    stats = await db.query_symbol_bar_stats("SPY")
    assert stats["symbol"] == "SPY"
    assert stats["bar_count"] == 3
    assert stats["last_bar"] == base + timedelta(minutes=2)

    empty = await db.query_symbol_bar_stats("NOPE")
    assert empty["bar_count"] == 0
    assert empty["last_bar"] is None


@pytest.mark.asyncio
async def test_query_model_details_joins_status(db):
    meta = ModelMetadata(
        symbol="SPY", timeframe="1m", horizon_minutes=5,
        model_type="gradient_boost", artifact_path="/tmp/x",
        metrics={"accuracy": 0.6}, feature_columns=["a"],
        trained_at=datetime.now(timezone.utc),
        train_samples=100, test_samples=20, is_active=True,
    )
    model_id = await db.insert_model_metadata(meta)
    await db.upsert_model_status(model_id, "active", "ok")

    details = await db.query_model_details()
    row = next(r for r in details if r["model_id"] == model_id)
    assert row["symbol"] == "SPY"
    assert row["model_type"] == "gradient_boost"
    assert row["horizon_minutes"] == 5
    assert row["status"] == "active"
    assert row["metrics"]["accuracy"] == 0.6


@pytest.mark.asyncio
async def test_query_model_details_defaults_status_when_missing(db):
    meta = ModelMetadata(
        symbol="QQQ", timeframe="1m", horizon_minutes=15,
        model_type="lstm", artifact_path="/tmp/y",
        metrics={"accuracy": 0.55}, feature_columns=["b"],
        trained_at=datetime.now(timezone.utc),
        train_samples=50, test_samples=10, is_active=True,
    )
    model_id = await db.insert_model_metadata(meta)
    details = await db.query_model_details()
    row = next(r for r in details if r["model_id"] == model_id)
    assert row["status"] == "active"
    assert row["reason"] == ""
