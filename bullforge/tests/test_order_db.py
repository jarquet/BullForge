import pytest
from datetime import datetime, timedelta, timezone
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
            "origin": "internal",
            "last_reconciled_at": None,
            "reconcile_source": None,
        }
    ])
    db = Database(settings)
    db.pool = pool

    orders = await db.query_orders(symbol="SPY", limit=10)

    assert len(orders) == 1
    assert isinstance(orders[0], Order)
    assert orders[0].symbol == "SPY"
    assert orders[0].status == "filled"
    assert orders[0].id == 1


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


@pytest.mark.asyncio
async def test_query_non_terminal_orders_uses_correct_sql(settings, mock_pool):
    from shared.db import Database
    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[])
    db = Database(settings)
    db.pool = pool

    await db.query_non_terminal_orders(older_than=timedelta(hours=48))

    conn.fetch.assert_awaited_once()
    sql, cutoff = conn.fetch.await_args.args
    assert "NOT IN ('filled','cancelled','rejected','expired')" in sql
    assert "submitted_at >= $1" in sql
    assert "alpaca_order_id IS NOT NULL" in sql
    assert cutoff.tzinfo is timezone.utc


@pytest.mark.asyncio
async def test_query_non_terminal_orders_parses_rows(settings, mock_pool):
    from shared.db import Database
    pool, conn = mock_pool
    now = datetime.now(timezone.utc)
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1, "symbol": "SPY", "side": "buy", "quantity": 1.0,
            "order_type": "market", "submitted_at": now,
            "signal_strength": 0.5, "alpaca_order_id": "alp-x",
            "status": "submitted", "filled_at": None,
            "filled_price": None, "filled_quantity": None,
            "limit_price": None, "origin": "internal",
            "last_reconciled_at": None, "reconcile_source": None,
        }
    ])
    db = Database(settings)
    db.pool = pool

    rows = await db.query_non_terminal_orders(older_than=timedelta(hours=48))
    assert len(rows) == 1
    assert rows[0].alpaca_order_id == "alp-x"
    assert rows[0].origin == "internal"


@pytest.mark.asyncio
async def test_get_order_by_alpaca_id_returns_none_when_missing(
    settings, mock_pool,
):
    from shared.db import Database
    pool, conn = mock_pool
    conn.fetchrow = AsyncMock(return_value=None)
    db = Database(settings)
    db.pool = pool

    result = await db.get_order_by_alpaca_id("nope")
    assert result is None


@pytest.mark.asyncio
async def test_get_order_by_alpaca_id_returns_order(settings, mock_pool):
    from shared.db import Database
    pool, conn = mock_pool
    now = datetime.now(timezone.utc)
    conn.fetchrow = AsyncMock(return_value={
        "id": 7, "symbol": "SPY", "side": "buy", "quantity": 1.0,
        "order_type": "market", "submitted_at": now,
        "signal_strength": 0.5, "alpaca_order_id": "alp-y",
        "status": "filled", "filled_at": now, "filled_price": 420.0,
        "filled_quantity": 1.0, "limit_price": None,
        "origin": "external", "last_reconciled_at": now,
        "reconcile_source": "stream",
    })
    db = Database(settings)
    db.pool = pool

    result = await db.get_order_by_alpaca_id("alp-y")
    assert result is not None
    assert result.id == 7
    assert result.origin == "external"
    assert result.reconcile_source == "stream"


@pytest.mark.asyncio
async def test_insert_order_writes_origin_and_reconcile_fields(
    settings, mock_pool,
):
    from shared.db import Database
    pool, conn = mock_pool
    conn.fetchval = AsyncMock(return_value=99)
    db = Database(settings)
    db.pool = pool

    order = Order(
        symbol="SPY", side="buy", quantity=1.0, order_type="market",
        submitted_at=datetime.now(timezone.utc), signal_strength=0.5,
        alpaca_order_id="alp-z", origin="external",
        reconcile_source="submit",
    )
    oid = await db.insert_order(order)
    assert oid == 99
    sql, *args = conn.fetchval.await_args.args
    assert "origin" in sql and "last_reconciled_at" in sql and "reconcile_source" in sql
    assert "external" in args
    assert "submit" in args
