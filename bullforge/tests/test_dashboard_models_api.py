from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import web

from services.dashboard_service.models_api import register_models_routes


@pytest.fixture
def mock_db_models():
    db = AsyncMock()
    db.query_accuracy_history = AsyncMock(return_value=[
        {
            "date": datetime(2026, 4, 14, 0, 0, 0, tzinfo=timezone.utc),
            "total": 10, "correct": 7, "accuracy": 0.7,
        },
        {
            "date": datetime(2026, 4, 15, 0, 0, 0, tzinfo=timezone.utc),
            "total": 12, "correct": 9, "accuracy": 0.75,
        },
    ])
    db.query_calibration_bins = AsyncMock(return_value=[
        {"bin_low": 0.0, "bin_high": 0.1, "count": 0, "correct": 0,
         "avg_confidence": 0.0, "hit_rate": 0.0},
        {"bin_low": 0.5, "bin_high": 0.6, "count": 5, "correct": 3,
         "avg_confidence": 0.55, "hit_rate": 0.6},
    ])
    db.query_simulated_pnl = AsyncMock(return_value={
        "signal_count": 42, "cumulative_pnl_pct": 0.08,
        "avg_pnl_pct": 0.002, "win_rate": 0.65,
        "daily_series": [
            {"date": datetime(2026, 4, 15, 0, 0, 0, tzinfo=timezone.utc),
             "pnl_pct": 0.04, "count": 10},
        ],
    })
    db.query_regime_breakdown = AsyncMock(return_value=[
        {"regime": "bull", "count": 30, "correct": 22,
         "accuracy": 0.73, "cumulative_pnl_pct": 0.06},
        {"regime": "bear", "count": 12, "correct": 5,
         "accuracy": 0.42, "cumulative_pnl_pct": -0.01},
    ])
    db.query_signal_history = AsyncMock(return_value=[
        {
            "timestamp": datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc),
            "symbol": "AAPL", "timeframe": "1m", "horizon_minutes": 5,
            "predicted_direction": 1, "confidence": 0.78,
            "entry_price": 180.5, "exit_price": 181.2,
            "is_correct": True, "pnl_pct": 0.00388,
        },
    ])
    return db


@pytest.fixture
def models_app(mock_db_models):
    app = web.Application()
    app["db"] = mock_db_models
    register_models_routes(app)
    return app


@pytest.mark.asyncio
async def test_history_endpoint(aiohttp_client, models_app, mock_db_models):
    client = await aiohttp_client(models_app)
    resp = await client.get(
        "/api/models/42/history?from=2026-04-14T00:00:00Z&to=2026-04-16T00:00:00Z",
    )
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    assert len(body) == 2
    assert body[0]["accuracy"] == 0.7
    # Verify DB was called with int model_id and parsed datetimes.
    call = mock_db_models.query_accuracy_history.call_args
    assert call.args[0] == 42 or call.kwargs.get("model_id") == 42
    assert call.kwargs.get("from_dt") is not None
    assert call.kwargs.get("to_dt") is not None


@pytest.mark.asyncio
async def test_calibration_endpoint(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/calibration")
    assert resp.status == 200
    body = await resp.json()
    assert "bins" in body
    assert len(body["bins"]) == 2
    # Aggregate calibration error computed over bins with count > 0.
    assert "calibration_error" in body


@pytest.mark.asyncio
async def test_simulated_pnl_endpoint(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/simulated_pnl")
    assert resp.status == 200
    body = await resp.json()
    assert body["signal_count"] == 42
    assert "daily_series" in body


@pytest.mark.asyncio
async def test_regime_breakdown_endpoint(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/regime_breakdown")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    regimes = [r["regime"] for r in body]
    assert "bull" in regimes


@pytest.mark.asyncio
async def test_signal_history_endpoint(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/signal_history?limit=50")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    assert body[0]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_invalid_model_id_returns_400(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/not-a-number/history")
    assert resp.status == 400


@pytest.mark.asyncio
async def test_history_rejects_malformed_from(aiohttp_client, models_app):
    client = await aiohttp_client(models_app)
    resp = await client.get("/api/models/42/history?from=garbage")
    assert resp.status == 400
