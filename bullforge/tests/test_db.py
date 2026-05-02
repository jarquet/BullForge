import pytest
import pytest_asyncio
import asyncpg
from shared.config import Settings
from shared.db import Database


@pytest_asyncio.fixture
async def db():
    settings = Settings()
    database = Database(settings)
    await database.connect()
    yield database
    # Clean up test data
    async with database.pool.acquire() as conn:
        await conn.execute("DELETE FROM bars_1m")
        await conn.execute("DELETE FROM bars_daily")
        await conn.execute("DELETE FROM calendar_events")
    await database.disconnect()


@pytest.mark.asyncio
async def test_connect_and_disconnect():
    settings = Settings()
    database = Database(settings)
    await database.connect()
    assert database.pool is not None
    await database.disconnect()


@pytest.mark.asyncio
async def test_run_migrations(db):
    """After connect, migrations should have created tables."""
    async with db.pool.acquire() as conn:
        # Verify bars_1m exists and is a hypertable
        result = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.hypertables "
            "WHERE hypertable_name = 'bars_1m'"
        )
        assert result == 1

        # Verify bars_daily exists and is a hypertable
        result = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.hypertables "
            "WHERE hypertable_name = 'bars_daily'"
        )
        assert result == 1

        # Verify calendar_events exists
        result = await conn.fetchval(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_name = 'calendar_events'"
        )
        assert result == 1


@pytest.mark.asyncio
async def test_insert_and_query_bar(db):
    from datetime import datetime, timezone
    from shared.models import Bar

    bar = Bar(
        symbol="SPY",
        timestamp=datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc),
        open=420.10,
        high=420.50,
        low=419.80,
        close=420.30,
        volume=15000,
        timeframe="1m",
    )
    await db.insert_bar(bar)

    rows = await db.query_bars("SPY", "1m", limit=1)
    assert len(rows) == 1
    assert rows[0].symbol == "SPY"
    assert rows[0].close == 420.30


@pytest.mark.asyncio
async def test_query_bars_accepts_datetime_since(db):
    """asyncpg refuses to coerce str → TIMESTAMPTZ when binding parameters,
    so query_bars must accept a datetime for `since`. Regression: the meta
    retrain phase was crashing here because we passed an ISO string."""
    from datetime import datetime, timezone, timedelta
    from shared.models import Bar

    ts_old = datetime(2026, 4, 10, 14, 0, 0, tzinfo=timezone.utc)
    ts_new = datetime(2026, 4, 10, 16, 0, 0, tzinfo=timezone.utc)
    await db.insert_bar(Bar(symbol="SPY", timestamp=ts_old, open=1, high=1,
                            low=1, close=1, volume=1, timeframe="1m"))
    await db.insert_bar(Bar(symbol="SPY", timestamp=ts_new, open=2, high=2,
                            low=2, close=2, volume=2, timeframe="1m"))

    cutoff = ts_old + timedelta(hours=1)
    rows = await db.query_bars("SPY", "1m", limit=10, since=cutoff)
    assert len(rows) == 1
    assert rows[0].close == 2

    # Legacy ISO-string form still works (parsed internally).
    rows_str = await db.query_bars(
        "SPY", "1m", limit=10, since=cutoff.isoformat(),
    )
    assert len(rows_str) == 1
    assert rows_str[0].close == 2


@pytest.mark.asyncio
async def test_insert_bar_upsert_on_duplicate(db):
    """Inserting a bar with the same symbol+timestamp should update, not error."""
    from datetime import datetime, timezone
    from shared.models import Bar

    ts = datetime(2026, 4, 10, 15, 30, 0, tzinfo=timezone.utc)
    bar1 = Bar(symbol="SPY", timestamp=ts, open=420.0, high=420.5,
               low=419.8, close=420.3, volume=15000, timeframe="1m")
    bar2 = Bar(symbol="SPY", timestamp=ts, open=420.0, high=421.0,
               low=419.5, close=420.8, volume=18000, timeframe="1m")

    await db.insert_bar(bar1)
    await db.insert_bar(bar2)

    rows = await db.query_bars("SPY", "1m", limit=10)
    assert len(rows) == 1
    assert rows[0].close == 420.8
    assert rows[0].volume == 18000


@pytest.mark.asyncio
async def test_get_bar_range_empty(db):
    earliest, latest = await db.get_bar_range("XYZ", "1m")
    assert earliest is None
    assert latest is None


@pytest.mark.asyncio
async def test_get_bar_range_populated(db):
    from datetime import datetime, timezone, timedelta
    from shared.models import Bar
    base = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    bars = [
        Bar(
            symbol="RNG", timestamp=base + timedelta(minutes=i),
            open=1.0, high=1.0, low=1.0, close=1.0, volume=1,
            timeframe="1m",
        )
        for i in range(3)
    ]
    await db.insert_bars(bars)
    earliest, latest = await db.get_bar_range("RNG", "1m")
    assert earliest == base
    assert latest == base + timedelta(minutes=2)


@pytest.mark.asyncio
async def test_get_bar_range_timeframe_isolated(db):
    from datetime import datetime, timezone, timedelta
    from shared.models import Bar
    base = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    minute_bar = Bar(
        symbol="ISO", timestamp=base, open=1.0, high=1.0,
        low=1.0, close=1.0, volume=1, timeframe="1m",
    )
    daily_bar = Bar(
        symbol="ISO", timestamp=base + timedelta(days=10),
        open=1.0, high=1.0, low=1.0, close=1.0, volume=1,
        timeframe="1d",
    )
    await db.insert_bars([minute_bar])
    await db.insert_bars([daily_bar])
    e1, l1 = await db.get_bar_range("ISO", "1m")
    e2, l2 = await db.get_bar_range("ISO", "1d")
    assert e1 == l1 == base
    assert e2 == l2 == base + timedelta(days=10)
