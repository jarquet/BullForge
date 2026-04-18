from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from aiohttp import web


@pytest.mark.asyncio
async def test_models_endpoint_returns_leaderboard_fields(aiohttp_client):
    from services.dashboard_service.api import create_api_app
    db = AsyncMock()
    db.query_model_details.return_value = [
        {
            "model_id": 42, "symbol": "AAPL", "timeframe": "1m",
            "horizon_minutes": 5, "model_type": "gradient_boost",
            "trained_at": datetime(2026, 4, 15, tzinfo=timezone.utc),
            "metrics": {"accuracy": 0.62}, "is_active": True,
            "status": "active", "reason": "Within range",
            "status_updated_at": datetime(2026, 4, 15, tzinfo=timezone.utc),
        },
    ]
    db.query_models_with_leaderboard_stats = AsyncMock(return_value={
        42: {
            "live_accuracy_7d": 0.58,
            "signals_7d": 210,
            "simulated_pnl_7d_pct": 0.012,
            "accuracy_sparkline_30d": [0.60, 0.62, 0.58, 0.61, 0.59],
        }
    })

    app = create_api_app(db)
    client = await aiohttp_client(app)
    resp = await client.get("/api/models")
    assert resp.status == 200
    body = await resp.json()
    assert len(body) == 1
    row = body[0]
    assert row["live_accuracy_7d"] == pytest.approx(0.58)
    assert row["backtested_accuracy"] == pytest.approx(0.62)
    assert row["fidelity_delta"] == pytest.approx(0.62 - 0.58)
    assert row["signals_7d"] == 210
    assert row["simulated_pnl_7d_pct"] == pytest.approx(0.012)
    assert row["accuracy_sparkline_30d"] == [0.60, 0.62, 0.58, 0.61, 0.59]
