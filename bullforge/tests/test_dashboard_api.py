import json
import pytest
import pytest_asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from aiohttp import web

from shared.models import ModelStatus, Trade, Order


@pytest.fixture
def mock_db():
    db = AsyncMock()

    # Status endpoint
    db.query_latest_metric_snapshot = AsyncMock(return_value={
        "id": 1,
        "timestamp": datetime(2026, 4, 13, 14, 0, 0, tzinfo=timezone.utc),
        "category": "trading",
        "scope": "",
        "window": "all",
        "metrics": {
            "total_trades": 50,
            "win_rate": 0.62,
            "total_pnl": 1250.0,
            "profit_factor": 1.8,
            "sharpe_ratio": 1.2,
        },
    })
    db.get_active_halts = AsyncMock(return_value=[])
    db.get_all_model_statuses = AsyncMock(return_value=[
        ModelStatus(
            model_id=1, status="active", reason="Within range",
            updated_at=datetime(2026, 4, 13, 14, 0, 0, tzinfo=timezone.utc),
        ),
    ])

    # Trading metrics
    db.query_latest_metric_snapshot = AsyncMock(return_value={
        "id": 1,
        "timestamp": datetime(2026, 4, 13, 14, 0, 0, tzinfo=timezone.utc),
        "category": "trading",
        "scope": "",
        "window": "all",
        "metrics": {
            "total_trades": 50,
            "win_rate": 0.62,
            "total_pnl": 1250.0,
            "profit_factor": 1.8,
            "sharpe_ratio": 1.2,
        },
    })

    # Metric history
    db.query_metric_snapshots = AsyncMock(return_value=[
        {
            "id": 2,
            "timestamp": datetime(2026, 4, 13, 14, 0, 0, tzinfo=timezone.utc),
            "category": "trading",
            "scope": "",
            "window": "all",
            "metrics": {"total_trades": 50, "win_rate": 0.62},
        },
    ])

    # Trades
    db.query_closed_trades = AsyncMock(return_value=[
        Trade(
            id=1, symbol="SPY", side="long",
            entry_time=datetime(2026, 4, 13, 13, 0, 0, tzinfo=timezone.utc),
            exit_time=datetime(2026, 4, 13, 14, 0, 0, tzinfo=timezone.utc),
            entry_price=420.0, exit_price=425.0,
            quantity=10.0, pnl=50.0, hold_duration_seconds=3600,
            entry_order_id=1, exit_order_id=2,
        ),
    ])

    # Orders
    db.query_orders = AsyncMock(return_value=[
        Order(
            id=1, symbol="SPY", side="buy", quantity=10.0,
            order_type="market",
            submitted_at=datetime(2026, 4, 13, 13, 0, 0, tzinfo=timezone.utc),
            signal_strength=0.8, alpaca_order_id="alp-123",
            status="filled",
            filled_at=datetime(2026, 4, 13, 13, 0, 1, tzinfo=timezone.utc),
            filled_price=420.0, filled_quantity=10.0,
        ),
    ])

    # Halt history
    db.query_halt_history = AsyncMock(return_value=[
        {
            "id": 1, "tier": 1, "reason": "Model degraded",
            "direction": None,
            "activated_at": datetime(2026, 4, 13, 12, 0, 0, tzinfo=timezone.utc),
            "resolved_at": datetime(2026, 4, 13, 13, 0, 0, tzinfo=timezone.utc),
        },
    ])

    return db


@pytest_asyncio.fixture
async def client(aiohttp_client, mock_db):
    from services.dashboard_service.api import create_api_app

    app = create_api_app(mock_db)
    return await aiohttp_client(app)


@pytest.mark.asyncio
async def test_api_status(client, mock_db):
    resp = await client.get("/api/status")
    assert resp.status == 200
    data = await resp.json()
    assert "models" in data
    assert "halts" in data
    assert "account" in data


@pytest.mark.asyncio
async def test_api_metrics_trading(client, mock_db):
    resp = await client.get("/api/metrics/trading?window=all")
    assert resp.status == 200
    data = await resp.json()
    assert "metrics" in data
    mock_db.query_latest_metric_snapshot.assert_called()


@pytest.mark.asyncio
async def test_api_metrics_trading_default_window(client, mock_db):
    resp = await client.get("/api/metrics/trading")
    assert resp.status == 200
    data = await resp.json()
    assert "metrics" in data


@pytest.mark.asyncio
async def test_api_metrics_account(client, mock_db):
    resp = await client.get("/api/metrics/account")
    assert resp.status == 200
    data = await resp.json()
    assert "metrics" in data


@pytest.mark.asyncio
async def test_api_metrics_history(client, mock_db):
    resp = await client.get("/api/metrics/history?category=trading&scope=&window=all&limit=10")
    assert resp.status == 200
    data = await resp.json()
    assert isinstance(data, list)
    assert len(data) == 1
    mock_db.query_metric_snapshots.assert_called()


@pytest.mark.asyncio
async def test_api_models(client, mock_db):
    resp = await client.get("/api/models")
    assert resp.status == 200
    data = await resp.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["model_id"] == 1
    assert data[0]["status"] == "active"


@pytest.mark.asyncio
async def test_api_trades(client, mock_db):
    resp = await client.get("/api/trades?limit=10")
    assert resp.status == 200
    data = await resp.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["symbol"] == "SPY"
    assert data[0]["pnl"] == 50.0


@pytest.mark.asyncio
async def test_api_trades_default_limit(client, mock_db):
    resp = await client.get("/api/trades")
    assert resp.status == 200
    data = await resp.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_orders(client, mock_db):
    resp = await client.get("/api/orders?limit=10")
    assert resp.status == 200
    data = await resp.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["symbol"] == "SPY"
    assert data[0]["status"] == "filled"


@pytest.mark.asyncio
async def test_api_halts(client, mock_db):
    resp = await client.get("/api/halts")
    assert resp.status == 200
    data = await resp.json()
    assert "active" in data
    assert "history" in data
    assert isinstance(data["history"], list)
