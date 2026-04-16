import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from datetime import datetime, timezone
from aiohttp import web

from services.dashboard_service.backtests_api import register_backtests_routes


@pytest.fixture
def app():
    app = web.Application()
    app["results_db"] = AsyncMock()
    app["jobs_db"] = AsyncMock()
    rc = AsyncMock()
    rc.get = AsyncMock(side_effect=lambda key: {
        "label_horizons_1m": [1, 2, 3, 5, 8, 10, 20, 40, 60, 80, 100, 120],
        "label_horizons_1d": [1, 2, 3, 5, 12, 15],
    }[key])
    app["runtime_config"] = rc
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


@pytest.mark.asyncio
async def test_detail_happy_path(aiohttp_client, app):
    app["results_db"].get_run = AsyncMock(return_value={
        "id": 1, "name": "r", "symbols": ["SPY"], "timeframe": "1m",
        "horizons": [5], "range_start": None, "range_end": None,
        "train_days": 1, "test_days": 1, "embargo_min": 5,
        "mode": "rolling", "status": "completed",
        "started_at": None, "finished_at": None,
        "config": {}, "metrics": {"total_pnl": 1.0, "n_trades": 2,
                                   "hit_rate": 0.5, "profit_factor": 1.0,
                                   "max_dd": -0.1, "sharpe": 0.0,
                                   "sortino": 0.0, "n_folds": 1},
    })
    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests/1")
    assert resp.status == 200
    data = await resp.json()
    assert data["id"] == 1
    assert data["metrics"]["n_folds"] == 1


@pytest.mark.asyncio
async def test_detail_404(aiohttp_client, app):
    app["results_db"].get_run = AsyncMock(return_value=None)
    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests/999")
    assert resp.status == 404


@pytest.mark.asyncio
async def test_folds_route(aiohttp_client, app):
    app["results_db"].list_folds = AsyncMock(return_value=[
        {"id": 1, "fold_index": 0,
         "train_start": None, "train_end": None,
         "test_start": None, "test_end": None,
         "metrics": {"n_trades": 1, "total_pnl": 1.0,
                     "max_dd": -0.1, "sharpe": 0.0},
         "metrics_json": None},
    ])
    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests/1/folds")
    assert resp.status == 200
    data = await resp.json()
    assert len(data["folds"]) == 1


@pytest.mark.asyncio
async def test_equity_route(aiohttp_client, app):
    app["results_db"].get_equity_curve = AsyncMock(return_value=[
        {"t": datetime(2025, 1, 1, tzinfo=timezone.utc), "equity": 100.0, "cash": 100.0},
    ])
    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests/1/equity?fold_id=7")
    assert resp.status == 200
    data = await resp.json()
    assert data["points"][0]["equity"] == 100.0
    app["results_db"].get_equity_curve.assert_awaited_with(1, fold_id=7)


@pytest.mark.asyncio
async def test_create_composes_full_name_with_custom(aiohttp_client, app):
    app["jobs_db"].insert_job = AsyncMock(return_value=42)
    client = await aiohttp_client(app)
    resp = await client.post("/api/backtests", json={
        "name": "my-tag", "symbols": ["SPY", "QQQ"], "timeframe": "1m",
        "horizons": [5, 40], "range_start": "2025-01-02T00:00:00+00:00",
        "range_end":   "2025-06-30T00:00:00+00:00",
        "train_days": 60, "test_days": 5, "embargo_min": 40, "mode": "rolling",
    })
    assert resp.status == 201
    body = await resp.json()
    assert body["custom_name"] == "my-tag"
    assert body["name"] == "my-tag__SPY-QQQ_1m_2025-01-02_2025-06-30_h5-40"
    kwargs = app["jobs_db"].insert_job.await_args.kwargs
    assert kwargs["custom_name"] == "my-tag"
    assert kwargs["name"].startswith("my-tag__")


@pytest.mark.asyncio
async def test_create_blank_name_generates_random_tag(aiohttp_client, app):
    app["jobs_db"].insert_job = AsyncMock(return_value=43)
    client = await aiohttp_client(app)
    resp = await client.post("/api/backtests", json={
        "name": "", "symbols": ["SPY"], "timeframe": "1m",
        "horizons": [5], "range_start": "2025-01-02T00:00:00+00:00",
        "range_end":   "2025-01-10T00:00:00+00:00",
        "train_days": 1, "test_days": 1, "embargo_min": 5, "mode": "rolling",
    })
    assert resp.status == 201
    body = await resp.json()
    assert len(body["custom_name"]) == 8
    assert body["custom_name"].isalnum()
    assert body["name"].startswith(body["custom_name"] + "__")


@pytest.mark.asyncio
async def test_create_rejects_horizon_not_in_runtime_config(aiohttp_client, app):
    # Runtime config only has [5, 10] for 1m; requesting 30 should 400.
    app["runtime_config"].get = AsyncMock(side_effect=lambda key: {
        "label_horizons_1m": [5, 10],
        "label_horizons_1d": [1, 2, 3, 5, 12, 15],
    }[key])
    client = await aiohttp_client(app)
    resp = await client.post("/api/backtests", json={
        "name": "tag", "symbols": ["SPY"], "timeframe": "1m",
        "horizons": [30], "range_start": "2025-01-02T00:00:00+00:00",
        "range_end":   "2025-01-10T00:00:00+00:00",
        "train_days": 1, "test_days": 1, "embargo_min": 5, "mode": "rolling",
    })
    assert resp.status == 400
    body = await resp.json()
    assert "horizons" in body["errors"]
    assert "[5, 10]" in body["errors"]["horizons"]


@pytest.mark.asyncio
async def test_create_rejects_invalid_custom_name(aiohttp_client, app):
    client = await aiohttp_client(app)
    resp = await client.post("/api/backtests", json={
        "name": "has spaces!", "symbols": ["SPY"], "timeframe": "1m",
        "horizons": [5], "range_start": "2025-01-02T00:00:00+00:00",
        "range_end":   "2025-01-10T00:00:00+00:00",
        "train_days": 1, "test_days": 1, "embargo_min": 5, "mode": "rolling",
    })
    assert resp.status == 400
    body = await resp.json()
    assert "name" in body["errors"]


@pytest.mark.asyncio
async def test_trades_route(aiohttp_client, app):
    app["results_db"].list_trades = AsyncMock(return_value=[])
    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests/1/trades?limit=10&offset=0")
    assert resp.status == 200
    data = await resp.json()
    assert data["trades"] == []
