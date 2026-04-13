import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    conn = AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx
    return pool, conn


@pytest.mark.asyncio
async def test_query_latest_metric_snapshot(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchrow = AsyncMock(return_value={
        "id": 1,
        "timestamp": datetime(2026, 4, 13, 14, 0, 0, tzinfo=timezone.utc),
        "category": "trading",
        "scope": "",
        "window": "all",
        "metrics": json.dumps({
            "total_trades": 50,
            "win_rate": 0.62,
            "total_pnl": 1250.0,
        }),
    })
    db = Database(settings)
    db.pool = pool

    result = await db.query_latest_metric_snapshot("trading", "", "all")

    assert result is not None
    assert result["category"] == "trading"
    assert result["metrics"]["total_trades"] == 50
    assert result["metrics"]["win_rate"] == 0.62
    conn.fetchrow.assert_called_once()
    call_args = conn.fetchrow.call_args
    assert "metric_snapshots" in call_args[0][0]
    assert call_args[0][1] == "trading"
    assert call_args[0][2] == ""
    assert call_args[0][3] == "all"


@pytest.mark.asyncio
async def test_query_latest_metric_snapshot_none(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetchrow = AsyncMock(return_value=None)
    db = Database(settings)
    db.pool = pool

    result = await db.query_latest_metric_snapshot("trading", "", "all")

    assert result is None


@pytest.mark.asyncio
async def test_query_metric_snapshots(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 2,
            "timestamp": datetime(2026, 4, 13, 14, 0, 0, tzinfo=timezone.utc),
            "category": "trading",
            "scope": "",
            "window": "all",
            "metrics": json.dumps({"total_trades": 50, "win_rate": 0.62}),
        },
        {
            "id": 1,
            "timestamp": datetime(2026, 4, 13, 13, 0, 0, tzinfo=timezone.utc),
            "category": "trading",
            "scope": "",
            "window": "all",
            "metrics": json.dumps({"total_trades": 48, "win_rate": 0.60}),
        },
    ])
    db = Database(settings)
    db.pool = pool

    result = await db.query_metric_snapshots("trading", "", "all", limit=10)

    assert len(result) == 2
    assert result[0]["metrics"]["total_trades"] == 50
    assert result[1]["metrics"]["total_trades"] == 48
    conn.fetch.assert_called_once()
    call_args = conn.fetch.call_args
    assert "metric_snapshots" in call_args[0][0]


@pytest.mark.asyncio
async def test_query_halt_history(settings, mock_pool):
    from shared.db import Database

    pool, conn = mock_pool
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 2,
            "tier": 4,
            "reason": "Manual halt",
            "direction": None,
            "activated_at": datetime(2026, 4, 13, 14, 0, 0, tzinfo=timezone.utc),
            "resolved_at": None,
        },
        {
            "id": 1,
            "tier": 1,
            "reason": "Model degraded",
            "direction": None,
            "activated_at": datetime(2026, 4, 13, 12, 0, 0, tzinfo=timezone.utc),
            "resolved_at": datetime(2026, 4, 13, 13, 0, 0, tzinfo=timezone.utc),
        },
    ])
    db = Database(settings)
    db.pool = pool

    result = await db.query_halt_history(limit=50)

    assert len(result) == 2
    assert result[0]["tier"] == 4
    assert result[0]["resolved_at"] is None
    assert result[1]["resolved_at"] is not None
    conn.fetch.assert_called_once()
    call_args = conn.fetch.call_args
    assert "halt_events" in call_args[0][0]
