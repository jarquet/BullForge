import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from aiohttp import web

from services.dashboard_service.backtests_api import register_backtests_routes


@pytest.fixture
def app():
    app = web.Application()
    app["results_db"] = AsyncMock()
    app["jobs_db"] = AsyncMock()
    register_backtests_routes(app)
    return app


@pytest.mark.asyncio
async def test_list_returns_runs_and_pending_jobs(aiohttp_client, app):
    app["results_db"].list_runs = AsyncMock(return_value=[
        {"id": 1, "name": "r", "symbols": ["SPY"],
         "range_start": None, "range_end": None,
         "status": "completed",
         "started_at": None, "finished_at": None,
         "metrics": {"total_pnl": 1.0, "sharpe": 0.5, "max_dd": -0.1,
                     "n_trades": 2, "hit_rate": 0.5}},
    ])
    app["jobs_db"].list_pending = AsyncMock(return_value=[
        {"id": 9, "name": "q", "config_json": {"symbols": ["TSLA"]},
         "status": "queued", "created_at": None,
         "started_at": None, "finished_at": None, "run_id": None,
         "error": None},
    ])
    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests")
    assert resp.status == 200
    body = await resp.json()
    kinds = {row["kind"] for row in body["rows"]}
    assert {"run", "job"} == kinds


@pytest.mark.asyncio
async def test_validate_new_job_rejects_embargo_gt_horizon(aiohttp_client, app):
    client = await aiohttp_client(app)
    resp = await client.post("/api/backtests", json={
        "name": "n", "symbols": ["SPY"], "timeframe": "1m",
        "horizons": [5, 10], "range_start": "2025-01-01T00:00:00+00:00",
        "range_end":   "2025-01-02T00:00:00+00:00",
        "train_days": 1, "test_days": 1, "embargo_min": 30, "mode": "rolling",
    })
    assert resp.status == 400
    body = await resp.json()
    assert "embargo_min" in body["errors"]
