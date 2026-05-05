import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from shared.models import Trade, ModelStatus


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    conn = AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx
    return pool, conn


def _make_trade():
    return Trade(
        symbol="SPY",
        side="long",
        entry_time=datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        entry_price=420.0,
        quantity=10.0,
        entry_order_id=1,
    )


@pytest.mark.asyncio
async def test_insert_trade(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchval = AsyncMock(return_value=1)
    db = Database(settings)
    db.pool = pool

    trade = _make_trade()
    row_id = await db.insert_trade(trade)

    assert row_id == 1
    conn.fetchval.assert_called_once()
    call_args = conn.fetchval.call_args
    assert "INSERT INTO trades" in call_args[0][0]
    assert call_args[0][1] == "SPY"


@pytest.mark.asyncio
async def test_update_trade_exit(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.execute = AsyncMock()
    db = Database(settings)
    db.pool = pool

    exit_time = datetime(2026, 4, 12, 15, 0, 0, tzinfo=timezone.utc)
    await db.update_trade_exit(
        trade_id=1,
        exit_time=exit_time,
        exit_price=425.0,
        exit_order_id=2,
        pnl=50.0,
        hold_duration_seconds=3600,
    )

    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert "UPDATE trades" in call_args[0][0]
    assert call_args[0][1] == 1


@pytest.mark.asyncio
async def test_get_open_trade(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchrow = AsyncMock(return_value={
        "id": 1,
        "symbol": "SPY",
        "side": "long",
        "entry_time": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        "entry_price": 420.0,
        "quantity": 10.0,
        "entry_order_id": 1,
        "exit_time": None,
        "exit_price": None,
        "pnl": None,
        "hold_duration_seconds": None,
        "exit_order_id": None,
    })
    db = Database(settings)
    db.pool = pool

    trade = await db.get_open_trade("SPY")

    assert trade is not None
    assert isinstance(trade, Trade)
    assert trade.symbol == "SPY"
    assert trade.exit_time is None


@pytest.mark.asyncio
async def test_get_open_trade_not_found(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchrow = AsyncMock(return_value=None)
    db = Database(settings)
    db.pool = pool

    trade = await db.get_open_trade("AAPL")

    assert trade is None


@pytest.mark.asyncio
async def test_query_closed_trades(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1,
            "symbol": "SPY",
            "side": "long",
            "entry_time": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
            "entry_price": 420.0,
            "quantity": 10.0,
            "entry_order_id": 1,
            "exit_time": datetime(2026, 4, 12, 15, 0, 0, tzinfo=timezone.utc),
            "exit_price": 425.0,
            "pnl": 50.0,
            "hold_duration_seconds": 3600,
            "exit_order_id": 2,
        },
    ])
    db = Database(settings)
    db.pool = pool

    trades = await db.query_closed_trades(limit=50)

    assert len(trades) == 1
    assert isinstance(trades[0], Trade)
    assert trades[0].pnl == 50.0


@pytest.mark.asyncio
async def test_query_closed_trades_with_since(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[])
    db = Database(settings)
    db.pool = pool

    since = datetime(2026, 4, 12, 0, 0, 0, tzinfo=timezone.utc)
    trades = await db.query_closed_trades(since=since, limit=50)

    assert len(trades) == 0
    call_args = conn.fetch.call_args
    assert "exit_time >=" in call_args[0][0]


@pytest.mark.asyncio
async def test_upsert_model_status(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.execute = AsyncMock()
    db = Database(settings)
    db.pool = pool

    await db.upsert_model_status(5, "warned", "Accuracy degrading")

    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert "INSERT INTO model_status" in call_args[0][0]
    assert "ON CONFLICT" in call_args[0][0]
    assert call_args[0][1] == 5
    assert call_args[0][2] == "warned"


@pytest.mark.asyncio
async def test_get_all_model_statuses(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[
        {
            "model_id": 5,
            "status": "active",
            "reason": "OK",
            "updated_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        },
    ])
    db = Database(settings)
    db.pool = pool

    statuses = await db.get_all_model_statuses()

    assert len(statuses) == 1
    assert isinstance(statuses[0], ModelStatus)
    assert statuses[0].model_id == 5


@pytest.mark.asyncio
async def test_query_prediction_outcomes(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[
        {
            "predicted_direction": 1,
            "confidence": 0.75,
            "price_move": 0.5,
            "is_correct": True,
        },
        {
            "predicted_direction": -1,
            "confidence": 0.60,
            "price_move": 0.3,
            "is_correct": False,
        },
    ])
    db = Database(settings)
    db.pool = pool

    outcomes = await db.query_prediction_outcomes(model_id=5, limit=50)

    assert len(outcomes) == 2
    assert outcomes[0]["is_correct"] is True
    assert outcomes[1]["is_correct"] is False


@pytest.mark.asyncio
async def test_insert_halt_event(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchval = AsyncMock(return_value=1)
    db = Database(settings)
    db.pool = pool

    halt_id = await db.insert_halt_event(4, "Drawdown exceeded threshold")

    assert halt_id == 1
    conn.fetchval.assert_called_once()
    call_args = conn.fetchval.call_args
    assert "INSERT INTO halt_events" in call_args[0][0]


@pytest.mark.asyncio
async def test_get_active_halts(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1,
            "tier": 4,
            "reason": "Drawdown exceeded",
            "direction": None,
            "activated_at": datetime(2026, 4, 12, 14, 0, 0, tzinfo=timezone.utc),
        },
    ])
    db = Database(settings)
    db.pool = pool

    halts = await db.get_active_halts()

    assert len(halts) == 1
    assert halts[0]["tier"] == 4


@pytest.mark.asyncio
async def test_resolve_halt_event(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.execute = AsyncMock()
    db = Database(settings)
    db.pool = pool

    await db.resolve_halt_event(1)

    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert "UPDATE halt_events" in call_args[0][0]
    assert "resolved_at" in call_args[0][0]


@pytest.mark.asyncio
async def test_insert_metric_snapshot(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.execute = AsyncMock()
    db = Database(settings)
    db.pool = pool

    await db.insert_metric_snapshot(
        category="trading",
        scope="",
        window="last_20",
        metrics={"win_rate": 0.65, "profit_factor": 1.8},
    )

    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert "INSERT INTO metric_snapshots" in call_args[0][0]


@pytest.mark.asyncio
async def test_get_open_trades_returns_all_open_rows_fifo(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    rows = [
        {
            "id": 1, "symbol": "BTC/USD", "side": "long",
            "entry_time": datetime(2026, 5, 5, 0, 57, tzinfo=timezone.utc),
            "entry_price": 60000.0, "quantity": 0.001, "entry_order_id": 10,
            "exit_time": None, "exit_price": None, "pnl": None,
            "hold_duration_seconds": None, "exit_order_id": None,
        },
        {
            "id": 2, "symbol": "BTC/USD", "side": "long",
            "entry_time": datetime(2026, 5, 5, 0, 58, tzinfo=timezone.utc),
            "entry_price": 60050.0, "quantity": 0.001, "entry_order_id": 11,
            "exit_time": None, "exit_price": None, "pnl": None,
            "hold_duration_seconds": None, "exit_order_id": None,
        },
    ]
    conn.fetch = AsyncMock(return_value=rows)
    db = Database(settings)
    db.pool = pool

    trades = await db.get_open_trades("BTC/USD")

    assert len(trades) == 2
    assert all(isinstance(t, Trade) for t in trades)
    assert [t.id for t in trades] == [1, 2]  # FIFO oldest-first
    assert all(t.exit_time is None for t in trades)
    sql = conn.fetch.call_args[0][0]
    assert "ORDER BY entry_time ASC" in sql
    assert "BTC/USD" == conn.fetch.call_args[0][1]


@pytest.mark.asyncio
async def test_get_open_trades_empty_when_none_open(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[])
    db = Database(settings)
    db.pool = pool

    trades = await db.get_open_trades("BTC/USD")

    assert trades == []
