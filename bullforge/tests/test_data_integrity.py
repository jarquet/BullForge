"""
Data-integrity guardrail tests.

These tests guard against look-ahead bias (data leakage / future-data contamination),
the #1 killer of trading ML systems. They MUST never be skipped or weakened.

Invariants tested here (Task 2):
2. No forward-label leakage into training data (test_training_data_respects_horizon_cutoff)
4. Training/test split is strictly chronological (test_time_based_split_is_chronological)

Invariants tested here (Task 3):
1. Feature row at T uses only bars with timestamp < T (test_features_exclude_current_bar)
3. Trade-history features use only trades with exit_time < T (test_trade_features_exclude_open_and_future_trades)
5. feature:updates published only after DB persist (test_feature_update_published_only_after_persist)
"""
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
import pytest_asyncio

from shared.config import Settings
from shared.db import Database
from shared.models import Bar
from services.feature_engine.engine import FeatureEngine
from services.feature_engine.registry import FeatureRegistry
from services.feature_engine.calculators.price import compute_price_features
from services.feature_engine.calculators.technical import compute_technical_features
from services.model_service.dataset import time_based_split


@pytest_asyncio.fixture
async def real_test_db():
    settings = Settings()
    database = Database(settings)
    await database.connect()
    # Clean relevant tables before test (trades before orders due to FK; trades before features/labels)
    async with database.pool.acquire() as conn:
        await conn.execute("DELETE FROM trades WHERE symbol = 'TEST'")
        await conn.execute("DELETE FROM orders WHERE symbol = 'TEST'")
        await conn.execute("DELETE FROM features WHERE symbol = 'TEST'")
        await conn.execute("DELETE FROM labels WHERE symbol = 'TEST'")
    yield database
    async with database.pool.acquire() as conn:
        await conn.execute("DELETE FROM trades WHERE symbol = 'TEST'")
        await conn.execute("DELETE FROM orders WHERE symbol = 'TEST'")
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
@pytest.mark.services
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
@pytest.mark.services
async def test_query_training_data_requires_as_of(real_test_db):
    """Caller MUST pass as_of — no default allowed."""
    db = real_test_db
    with pytest.raises(TypeError):
        await db.query_training_data("TEST", "1m", 5)  # no as_of


# ---------------------------------------------------------------------------
# Task 3 invariants
# ---------------------------------------------------------------------------


def _build_registry_with_basic_calculators() -> FeatureRegistry:
    """Mirrors production registration so feature output reflects real calculators."""
    registry = FeatureRegistry()
    # Register the same calculators the production feature_engine uses (main.py).
    # At minimum price and technical so SMA-based features are present — these
    # are the ones most susceptible to current-bar leakage.
    registry.register("price", compute_price_features)
    registry.register("technical", compute_technical_features)
    return registry


def test_features_exclude_current_bar():
    """
    Invariant 1: Feature row at timestamp T must use ONLY bars with timestamp < T.
    The bar AT T must not contribute to any feature AT T.
    """
    base = datetime(2026, 1, 1, 14, 0, tzinfo=timezone.utc)
    bars = []
    for i in range(100):
        ts = base + timedelta(minutes=i)
        close = 99999.99 if i == 50 else 100.0
        bars.append(Bar(
            symbol="TEST", timestamp=ts, open=close, high=close, low=close,
            close=close, volume=1000, timeframe="1m",
        ))
    target_ts = base + timedelta(minutes=50)

    settings = Settings()
    registry = _build_registry_with_basic_calculators()
    engine = FeatureEngine(
        settings=settings, db=None, registry=registry,
        label_horizons={"1m": [5], "1d": [5]}, redis_client=None,
    )
    features_at_target = engine.compute_for_bar(bars, target_ts, timeframe="1m")

    for feat_name, feat_value in features_at_target.items():
        if feat_value is None or (isinstance(feat_value, float) and pd.isna(feat_value)):
            continue
        assert abs(feat_value - 99999.99) > 1e-6, (
            f"Feature {feat_name}={feat_value} leaked from current-bar close=99999.99"
        )
        assert abs(feat_value - 99999.99 / 2) > 1e-6


@pytest.mark.asyncio
@pytest.mark.services
async def test_trade_features_exclude_open_and_future_trades(real_test_db):
    """
    Invariant 3: Features at timestamp T that derive from trade history must
    use ONLY trades with exit_time < T. Open trades and trades closing at/after T
    MUST NOT contribute.
    """
    from shared.models import Order, Trade
    db = real_test_db
    T = datetime(2026, 1, 1, 14, 0, tzinfo=timezone.utc)

    def _make_order(submitted_at: datetime) -> Order:
        return Order(
            symbol="TEST", side="buy", quantity=10, order_type="market",
            submitted_at=submitted_at, signal_strength=0.8,
        )

    # Insert the orders that trades will reference (FK: trades.entry_order_id -> orders.id)
    order_a_entry_id = await db.insert_order(_make_order(T - timedelta(hours=2, minutes=1)))
    order_a_exit_id = await db.insert_order(_make_order(T - timedelta(hours=1, minutes=1)))
    order_b_entry_id = await db.insert_order(_make_order(T - timedelta(minutes=31)))
    order_c_entry_id = await db.insert_order(_make_order(T - timedelta(minutes=11)))
    order_c_exit_id = await db.insert_order(_make_order(T + timedelta(minutes=4)))

    # Trade A: closed before T — MUST contribute
    trade_a_id = await db.insert_trade(Trade(
        symbol="TEST", side="long",
        entry_time=T - timedelta(hours=2), entry_price=420.0, quantity=10,
        entry_order_id=order_a_entry_id,
    ))
    await db.update_trade_exit(
        trade_id=trade_a_id,
        exit_time=T - timedelta(hours=1), exit_price=421.0,
        exit_order_id=order_a_exit_id, pnl=10.0, hold_duration_seconds=3600,
    )

    # Trade B: open — MUST NOT contribute
    await db.insert_trade(Trade(
        symbol="TEST", side="long",
        entry_time=T - timedelta(minutes=30), entry_price=421.0, quantity=10,
        entry_order_id=order_b_entry_id,
    ))

    # Trade C: closed after T — MUST NOT contribute
    trade_c_id = await db.insert_trade(Trade(
        symbol="TEST", side="long",
        entry_time=T - timedelta(minutes=10), entry_price=421.5, quantity=10,
        entry_order_id=order_c_entry_id,
    ))
    await db.update_trade_exit(
        trade_id=trade_c_id,
        exit_time=T + timedelta(minutes=5), exit_price=422.0,
        exit_order_id=order_c_exit_id, pnl=5.0, hold_duration_seconds=900,
    )

    trades = await db.query_trades_closed_before("TEST", T)
    assert len(trades) == 1
    assert trades[0]["id"] == trade_a_id


@pytest.mark.asyncio
async def test_feature_update_published_only_after_persist():
    """
    Invariant 5: feature:updates is published ONLY after the feature rows for
    the closed bars have been persisted to the DB.
    """
    from unittest.mock import AsyncMock

    call_order: list[str] = []

    db = AsyncMock()

    async def fake_query_bars(symbol, timeframe, limit):
        base = datetime(2026, 1, 1, 14, 0, tzinfo=timezone.utc)
        return [
            Bar(
                symbol="TEST",
                timestamp=base + timedelta(minutes=i),
                open=100, high=100, low=100, close=100, volume=1000,
                timeframe="1m",
            )
            for i in range(100)
        ]

    async def fake_insert(rows):
        call_order.append("insert")

    db.query_bars = fake_query_bars
    db.insert_feature_rows = fake_insert

    redis_client = AsyncMock()

    async def fake_publish(channel, msg):
        call_order.append(f"publish:{channel}")

    redis_client.publish = fake_publish

    settings = Settings()
    registry = _build_registry_with_basic_calculators()
    engine = FeatureEngine(
        settings=settings, db=db, registry=registry,
        label_horizons={"1m": [5], "1d": [5]}, redis_client=redis_client,
    )
    await engine.compute_features_for_symbol("TEST", "1m", limit=100)

    insert_idx = call_order.index("insert")
    publish_idx = next(
        i for i, s in enumerate(call_order) if s.startswith("publish:feature:updates")
    )
    assert insert_idx < publish_idx, (
        f"feature:updates published before insert_feature_rows: order was {call_order}"
    )
