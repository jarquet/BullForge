import pytest
from datetime import datetime, timezone

from shared.config import Settings
from shared.db import Database
from shared.models import Order

pytestmark = pytest.mark.services  # needs TimescaleDB


@pytest.mark.asyncio
async def test_sandbox_order_writes_to_backtest_schema():
    settings = Settings()
    live = Database(settings)
    sandbox = Database(settings, sandbox=True)
    await live.connect()
    await sandbox.connect()
    try:
        order = Order(
            symbol="TEST_SANDBOX",
            side="buy",
            quantity=1.0,
            order_type="market",
            submitted_at=datetime.now(timezone.utc),
            signal_strength=0.5,
            alpaca_order_id="sandbox-test-1",
        )
        await sandbox.insert_order_if_new(order)

        async with live.pool.acquire() as conn:
            live_count = await conn.fetchval(
                "SELECT count(*) FROM public.orders WHERE alpaca_order_id = $1",
                "sandbox-test-1",
            )
            sandbox_count = await conn.fetchval(
                "SELECT count(*) FROM backtest.orders WHERE alpaca_order_id = $1",
                "sandbox-test-1",
            )
        assert live_count == 0, "sandbox write leaked into public.orders"
        assert sandbox_count == 1, "sandbox write missing from backtest.orders"
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM backtest.orders WHERE alpaca_order_id = $1",
                "sandbox-test-1",
            )
        await sandbox.disconnect()
        await live.disconnect()


@pytest.mark.asyncio
async def test_sandbox_reads_bars_from_public():
    settings = Settings()
    sandbox = Database(settings, sandbox=True)
    await sandbox.connect()
    try:
        async with sandbox.pool.acquire() as conn:
            result = await conn.fetchval("SELECT to_regclass('bars_1m')")
        assert result == "bars_1m", "bars_1m should resolve via search_path fallback"
    finally:
        await sandbox.disconnect()


@pytest.mark.asyncio
async def test_sandbox_model_registry_writes_to_backtest_schema():
    settings = Settings()
    live = Database(settings)
    sandbox = Database(settings, sandbox=True)
    await live.connect()
    await sandbox.connect()
    try:
        marker = "TEST_SANDBOX_MODEL"
        async with sandbox.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO model_registry
                  (symbol, timeframe, horizon_minutes, model_type, artifact_path,
                   metrics, feature_columns, trained_at, train_samples, test_samples,
                   is_active)
                VALUES ($1, '1m', 5, 'gradient_boost', '/tmp/x',
                        '{}'::jsonb, '["f1"]'::jsonb, NOW(), 100, 20, true)
                """,
                marker,
            )
        async with live.pool.acquire() as conn:
            live_count = await conn.fetchval(
                "SELECT count(*) FROM public.model_registry WHERE symbol = $1",
                marker,
            )
            sandbox_count = await conn.fetchval(
                "SELECT count(*) FROM backtest.model_registry WHERE symbol = $1",
                marker,
            )
        assert live_count == 0, "model_registry write leaked into public"
        assert sandbox_count == 1
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM backtest.model_registry WHERE symbol = $1", marker,
            )
        await sandbox.disconnect()
        await live.disconnect()


@pytest.mark.asyncio
async def test_sandbox_predictions_writes_to_backtest_schema():
    settings = Settings()
    live = Database(settings)
    sandbox = Database(settings, sandbox=True)
    await live.connect()
    await sandbox.connect()
    try:
        marker = "TEST_SANDBOX_PRED"
        async with sandbox.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO predictions
                  (symbol, timestamp, timeframe, horizon_minutes,
                   model_id, predicted_direction, confidence)
                VALUES ($1, NOW(), '1m', 5, 999999, 1, 0.75)
                """,
                marker,
            )
        async with live.pool.acquire() as conn:
            live_count = await conn.fetchval(
                "SELECT count(*) FROM public.predictions WHERE symbol = $1",
                marker,
            )
            sandbox_count = await conn.fetchval(
                "SELECT count(*) FROM backtest.predictions WHERE symbol = $1",
                marker,
            )
        assert live_count == 0
        assert sandbox_count == 1
    finally:
        async with live.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM backtest.predictions WHERE symbol = $1", marker,
            )
        await sandbox.disconnect()
        await live.disconnect()
