import pytest
import pytest_asyncio
import asyncpg
from datetime import datetime, timezone
from unittest.mock import AsyncMock
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


# ── Hermetic UTC enforcement sweep ──────────────────────────────────
#
# All public Database helpers that take a datetime arg must reject naive
# datetimes with a ValueError (project invariant: every datetime is UTC).
# These tests mock the asyncpg pool so they run without a live DB; the
# assertion is that _require_utc raises BEFORE pool.acquire() is touched.

NAIVE_DT = datetime(2026, 5, 2, 12, 0, 0)
UTC_DT = datetime(2026, 5, 2, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def mock_db():
    """Database with an AsyncMock pool — _require_utc must reject naive
    datetimes before any pool method is called."""
    settings = Settings()
    db = Database(settings)
    db.pool = AsyncMock()
    return db


# (helper_name, kwargs) pairs — one entry per (helper, datetime-arg).
# Helpers with two datetime args appear twice (e.g. start naive / end naive).
NAIVE_DT_CASES = [
    # Required-datetime helpers
    ("query_bars_between", dict(symbol="SPY", timeframe="1m", start=NAIVE_DT, end=UTC_DT)),
    ("query_bars_between", dict(symbol="SPY", timeframe="1m", start=UTC_DT, end=NAIVE_DT)),
    ("query_latest_close", dict(symbol="SPY", at=NAIVE_DT)),
    ("query_training_data", dict(symbol="SPY", timeframe="1m", horizon_minutes=15, as_of=NAIVE_DT)),
    ("count_predictions_in_window", dict(start=NAIVE_DT, end=UTC_DT)),
    ("count_predictions_in_window", dict(start=UTC_DT, end=NAIVE_DT)),
    ("query_predictions_in_range", dict(symbol="SPY", timeframe="1m", start=NAIVE_DT, end=UTC_DT)),
    ("query_predictions_in_range", dict(symbol="SPY", timeframe="1m", start=UTC_DT, end=NAIVE_DT)),
    ("update_trade_exit", dict(trade_id=1, exit_time=NAIVE_DT, exit_price=1.0,
                               exit_order_id=1, pnl=0.0, hold_duration_seconds=60)),
    ("query_trades_closed_before", dict(symbol="SPY", as_of=NAIVE_DT)),
    ("update_meta_feature_labels", dict(symbol="SPY", timestamp=NAIVE_DT, direction=1,
                                        entry_label=1, optimal_pair_idx=0, realized_ev=0.0)),
    ("query_unlabeled_meta_features", dict(older_than=NAIVE_DT)),

    # Optional-datetime helpers — pass naive in the optional slot.
    ("query_bars", dict(symbol="SPY", timeframe="1m", since=NAIVE_DT)),
    ("update_order_status", dict(order_id=1, status="filled", filled_at=NAIVE_DT)),
    ("query_closed_trades", dict(symbol="SPY", since=NAIVE_DT)),
    ("query_equity_curve", dict(starting_equity=1000.0, from_dt=NAIVE_DT)),
    ("query_equity_curve", dict(starting_equity=1000.0, to_dt=NAIVE_DT)),
    ("query_distributions", dict(from_dt=NAIVE_DT)),
    ("query_distributions", dict(to_dt=NAIVE_DT)),
    ("query_streaks", dict(from_dt=NAIVE_DT)),
    ("query_streaks", dict(to_dt=NAIVE_DT)),
    ("query_heatmap_time_of_day", dict(from_dt=NAIVE_DT)),
    ("query_heatmap_time_of_day", dict(to_dt=NAIVE_DT)),
    ("query_exposure_timeline", dict(equity=1000.0, from_dt=NAIVE_DT)),
    ("query_exposure_timeline", dict(equity=1000.0, to_dt=NAIVE_DT)),
    ("query_benchmark", dict(starting_equity=1000.0, from_dt=NAIVE_DT)),
    ("query_benchmark", dict(starting_equity=1000.0, to_dt=NAIVE_DT)),
    ("query_symbol_drilldown", dict(symbol="SPY", from_dt=NAIVE_DT)),
    ("query_symbol_drilldown", dict(symbol="SPY", to_dt=NAIVE_DT)),
    ("query_accuracy_history", dict(model_id=1, from_dt=NAIVE_DT)),
    ("query_accuracy_history", dict(model_id=1, to_dt=NAIVE_DT)),
    ("query_calibration_bins", dict(model_id=1, from_dt=NAIVE_DT)),
    ("query_calibration_bins", dict(model_id=1, to_dt=NAIVE_DT)),
    ("query_simulated_pnl", dict(model_id=1, from_dt=NAIVE_DT)),
    ("query_simulated_pnl", dict(model_id=1, to_dt=NAIVE_DT)),
    ("query_regime_breakdown", dict(model_id=1, from_dt=NAIVE_DT)),
    ("query_regime_breakdown", dict(model_id=1, to_dt=NAIVE_DT)),
    ("query_metric_snapshots", dict(category="cat", scope="scp", window="w", from_dt=NAIVE_DT)),
    ("query_metric_snapshots", dict(category="cat", scope="scp", window="w", to_dt=NAIVE_DT)),
]


@pytest.mark.parametrize("helper_name,call_kwargs", NAIVE_DT_CASES)
async def test_helper_rejects_naive_datetime(mock_db, helper_name, call_kwargs):
    """Each public DB helper that takes a datetime arg must raise ValueError
    on a naive datetime, before touching the pool. Hermetic — no DB."""
    helper = getattr(mock_db, helper_name)
    with pytest.raises(ValueError):
        await helper(**call_kwargs)
    # Pool must NOT have been touched — validation happens up-front.
    mock_db.pool.acquire.assert_not_called()


def test_require_utc_accepts_utc_datetime():
    """Happy path: _require_utc returns the datetime unchanged for a UTC dt."""
    from shared.db import _require_utc
    out = _require_utc(UTC_DT, "x")
    assert out is UTC_DT


def test_require_utc_rejects_naive_datetime():
    from shared.db import _require_utc
    with pytest.raises(ValueError, match="x must be UTC"):
        _require_utc(NAIVE_DT, "x")


def test_require_utc_rejects_non_utc_tzinfo():
    """A tz-aware datetime in a non-UTC zone is also rejected."""
    from datetime import timedelta as _td
    from shared.db import _require_utc
    other_tz = timezone(_td(hours=-5))
    with pytest.raises(ValueError, match="must be UTC"):
        _require_utc(datetime(2026, 5, 2, 12, 0, tzinfo=other_tz), "x")


def test_require_utc_accepts_zoneinfo_utc():
    """ZoneInfo("UTC") is semantically UTC even though it isn't `timezone.utc`."""
    from zoneinfo import ZoneInfo
    from shared.db import _require_utc
    dt = datetime(2026, 5, 2, 12, 0, tzinfo=ZoneInfo("UTC"))
    out = _require_utc(dt, "x")
    assert out is dt


def test_require_utc_accepts_zero_offset_timezone():
    """Any tzinfo whose UTC offset is zero is accepted (timezone(timedelta(0)))."""
    from datetime import timedelta as _td
    from shared.db import _require_utc
    dt = datetime(2026, 5, 2, 12, 0, tzinfo=timezone(_td(0)))
    out = _require_utc(dt, "x")
    assert out is dt
