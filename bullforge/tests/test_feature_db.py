import pytest
import pytest_asyncio
from datetime import datetime, timezone
from shared.config import Settings
from shared.db import Database
from shared.models import FeatureRow, Label

pytestmark = pytest.mark.services  # needs TimescaleDB

# A symbol no real data uses, so these tests neither see nor delete market
# data loaded into the same database (e.g. SPY for the backtest smoke test).
SYM = "TEST_FEATURE_DB"


@pytest_asyncio.fixture
async def db():
    settings = Settings()
    database = Database(settings)
    await database.connect()
    async def _clean():
        async with database.pool.acquire() as conn:
            await conn.execute("DELETE FROM features WHERE symbol = $1", SYM)
            await conn.execute("DELETE FROM labels WHERE symbol = $1", SYM)

    await _clean()
    yield database
    await _clean()
    await database.disconnect()


@pytest.mark.asyncio
async def test_insert_and_query_feature_row(db):
    row = FeatureRow(
        symbol=SYM,
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        features={"sma_20": 420.5, "rsi_14": 55.3},
    )
    await db.insert_feature_row(row)

    rows = await db.query_feature_rows(SYM, "1m", limit=1)
    assert len(rows) == 1
    assert rows[0].symbol == SYM
    assert rows[0].features["sma_20"] == 420.5
    assert rows[0].features["rsi_14"] == 55.3


@pytest.mark.asyncio
async def test_insert_feature_row_upsert(db):
    ts = datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc)
    row1 = FeatureRow(symbol=SYM, timestamp=ts, timeframe="1m",
                      features={"sma_20": 420.5})
    row2 = FeatureRow(symbol=SYM, timestamp=ts, timeframe="1m",
                      features={"sma_20": 421.0, "rsi_14": 60.0})

    await db.insert_feature_row(row1)
    await db.insert_feature_row(row2)

    rows = await db.query_feature_rows(SYM, "1m", limit=10)
    assert len(rows) == 1
    assert rows[0].features["sma_20"] == 421.0
    assert rows[0].features["rsi_14"] == 60.0


@pytest.mark.asyncio
async def test_insert_feature_rows_batch(db):
    rows = [
        FeatureRow(
            symbol=SYM,
            timestamp=datetime(2026, 4, 10, 15, i, 0, tzinfo=timezone.utc),
            timeframe="1m",
            features={"sma_20": 420.0 + i},
        )
        for i in range(5)
    ]
    await db.insert_feature_rows(rows)

    result = await db.query_feature_rows(SYM, "1m", limit=10)
    assert len(result) == 5


@pytest.mark.asyncio
async def test_insert_and_query_label(db):
    label = Label(
        symbol=SYM,
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        timeframe="1m",
        horizon_minutes=30,
        forward_return=0.0025,
        direction=1,
    )
    await db.insert_label(label)

    labels = await db.query_labels(SYM, "1m", horizon_minutes=30, limit=1)
    assert len(labels) == 1
    assert labels[0].forward_return == 0.0025
    assert labels[0].direction == 1


@pytest.mark.asyncio
async def test_insert_labels_batch(db):
    labels = [
        Label(
            symbol=SYM,
            timestamp=datetime(2026, 4, 10, 15, i, 0, tzinfo=timezone.utc),
            timeframe="1m",
            horizon_minutes=30,
            forward_return=0.001 * i,
            direction=1 if i % 2 == 0 else -1,
        )
        for i in range(5)
    ]
    await db.insert_labels(labels)

    result = await db.query_labels(SYM, "1m", horizon_minutes=30, limit=10)
    assert len(result) == 5
