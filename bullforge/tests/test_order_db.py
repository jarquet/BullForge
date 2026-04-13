import json
import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock
from shared.models import Order


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    conn = AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx
    return pool, conn


def _make_order():
    return Order(
        symbol="SPY",
        side="buy",
        quantity=10.0,
        order_type="market",
        submitted_at=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        signal_strength=0.8,
        alpaca_order_id="alp-123",
        status="submitted",
    )


@pytest.mark.asyncio
async def test_insert_order(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchval = AsyncMock(return_value=42)
    db = Database(settings)
    db.pool = pool

    order = _make_order()
    row_id = await db.insert_order(order)

    assert row_id == 42
    conn.fetchval.assert_called_once()
    call_args = conn.fetchval.call_args
    assert "INSERT INTO orders" in call_args[0][0]
    assert call_args[0][1] == "SPY"


@pytest.mark.asyncio
async def test_update_order_status(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.execute = AsyncMock()
    db = Database(settings)
    db.pool = pool

    filled_at = datetime(2026, 4, 12, 14, 0, 1, tzinfo=timezone.utc)
    await db.update_order_status(
        order_id=42,
        status="filled",
        filled_at=filled_at,
        filled_price=420.50,
        filled_quantity=10.0,
    )

    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert "UPDATE orders" in call_args[0][0]
    assert call_args[0][1] == 42
    assert call_args[0][2] == "filled"


@pytest.mark.asyncio
async def test_query_orders_by_symbol(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1,
            "symbol": "SPY",
            "side": "buy",
            "quantity": 10.0,
            "order_type": "market",
            "submitted_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            "signal_strength": 0.8,
            "alpaca_order_id": "alp-123",
            "status": "filled",
            "filled_at": datetime(2026, 4, 12, 14, 0, 1, tzinfo=timezone.utc),
            "filled_price": 420.50,
            "filled_quantity": 10.0,
            "limit_price": None,
        }
    ])
    db = Database(settings)
    db.pool = pool

    orders = await db.query_orders(symbol="SPY", limit=10)

    assert len(orders) == 1
    assert orders[0]["symbol"] == "SPY"
    assert orders[0]["status"] == "filled"


@pytest.mark.asyncio
async def test_query_orders_by_status(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[])
    db = Database(settings)
    db.pool = pool

    orders = await db.query_orders(status="submitted", limit=10)

    assert len(orders) == 0
    conn.fetch.assert_called_once()
    call_args = conn.fetch.call_args
    assert "status = " in call_args[0][0]


@pytest.mark.asyncio
async def test_query_orders_no_filters(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[])
    db = Database(settings)
    db.pool = pool

    orders = await db.query_orders(limit=50)

    assert len(orders) == 0
    conn.fetch.assert_called_once()
