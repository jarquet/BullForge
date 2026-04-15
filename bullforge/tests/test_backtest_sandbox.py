import pytest
from datetime import datetime, timezone

from shared.config import Settings
from shared.db import Database
from shared.models import Order


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
