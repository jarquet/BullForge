import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone
from aiohttp import web

from services.dashboard_service.backtests_api import register_backtests_routes


def _make_results_db_mock():
    """Build a results_db AsyncMock with a db.pool.acquire() context manager."""
    results_db = AsyncMock()
    # Set up the nested db.pool.acquire() context manager used by new equity/folds handlers.
    # pool.acquire() must be a regular callable (not async) that returns an async context manager.
    conn_mock = AsyncMock()
    conn_mock.fetch = AsyncMock(return_value=[])
    conn_cm = AsyncMock()
    conn_cm.__aenter__ = AsyncMock(return_value=conn_mock)
    conn_cm.__aexit__ = AsyncMock(return_value=False)
    results_db.db = MagicMock()
    results_db.db.pool = MagicMock()
    results_db.db.pool.acquire = MagicMock(return_value=conn_cm)
    return results_db, conn_mock


def _make_db_mock():
    """Build a Database mock with pool.acquire() context manager."""
    conn_mock = AsyncMock()
    conn_mock.fetchval = AsyncMock(return_value=None)
    conn_cm = AsyncMock()
    conn_cm.__aenter__ = AsyncMock(return_value=conn_mock)
    conn_cm.__aexit__ = AsyncMock(return_value=False)
    db = MagicMock()
    db.pool = MagicMock()
    db.pool.acquire = MagicMock(return_value=conn_cm)
    return db, conn_mock


@pytest.fixture
def app():
    app = web.Application()
    results_db, _ = _make_results_db_mock()
    app["results_db"] = results_db
    app["jobs_db"] = AsyncMock()
    db, _ = _make_db_mock()
    app["db"] = db
    rc = AsyncMock()
    rc.get = AsyncMock(side_effect=lambda key: {
        "label_horizons_1m": [1, 2, 3, 5, 8, 10, 20, 40, 60, 80, 100, 120],
        "label_horizons_1d": [1, 2, 3, 5, 12, 15],
    }[key])
    app["runtime_config"] = rc
    register_backtests_routes(app)
    return app


@pytest.fixture
def backtests_app(app):
    """Alias for app fixture used by Task 5/6 tests."""
    return app


@pytest.mark.asyncio
async def test_list_returns_runs_and_pending_jobs(aiohttp_client, app):
    app["results_db"].list_runs = AsyncMock(return_value=[
        {"id": 1, "name": "r", "symbols": ["SPY"],
         "range_start": None, "range_end": None,
         "status": "completed",
         "started_at": None, "finished_at": None,
         "metrics": {"total_pnl": 1.0, "sharpe": 0.5, "max_dd": -0.1,
                     "n_trades": 2, "hit_rate": 0.5},
         "tags": [], "notes": None},
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
async def test_validate_new_job_accepts_1d_embargo_in_minutes(aiohttp_client, app):
    # For 1d timeframe, horizons are in days; embargo_min=30 should be valid
    # because 30 minutes < max(horizons)*1440 minutes per day.
    app["runtime_config"].get = AsyncMock(return_value=[1, 2, 5, 15])
    app["jobs_db"].insert_job = AsyncMock(return_value=99)
    client = await aiohttp_client(app)
    resp = await client.post("/api/backtests", json={
        "name": "d", "symbols": ["SPY"], "timeframe": "1d",
        "horizons": [1, 5], "range_start": "2025-01-01T00:00:00+00:00",
        "range_end":   "2025-06-30T00:00:00+00:00",
        "train_days": 60, "test_days": 5, "embargo_min": 30, "mode": "rolling",
    })
    assert resp.status == 201


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
    # New folds handler queries DB directly via results_db.db.pool.acquire()
    conn_mock = app["results_db"].db.pool.acquire.return_value.__aenter__.return_value
    conn_mock.fetch = AsyncMock(return_value=[
        {"id": 1, "fold_index": 0,
         "train_start": None, "train_end": None,
         "test_start": None, "test_end": None,
         "metrics_json": None},
    ])
    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests/1/folds")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 1
    # New shape: metrics are embedded directly in each row
    assert "pnl" in data[0]
    assert "sharpe" in data[0]


@pytest.mark.asyncio
async def test_equity_route(aiohttp_client, app):
    # New equity handler queries DB directly via results_db.db.pool.acquire()
    conn_mock = app["results_db"].db.pool.acquire.return_value.__aenter__.return_value
    conn_mock.fetch = AsyncMock(return_value=[
        {"timestamp": datetime(2025, 1, 1, tzinfo=timezone.utc),
         "equity": 100.0, "fold_id": 7},
    ])
    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests/1/equity")
    assert resp.status == 200
    data = await resp.json()
    # New shape: {series: [...], fold_boundaries: [...]}
    assert "series" in data
    assert "fold_boundaries" in data
    assert data["series"][0]["equity"] == 100.0
    assert "drawdown" in data["series"][0]


@pytest.mark.asyncio
async def test_create_composes_full_name_with_custom(aiohttp_client, app):
    _use_exec_rc(app)
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
    _use_exec_rc(app)
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


_DEFAULT_EXEC_RC = {
    "label_horizons_1m": [1, 2, 3, 5, 8, 10, 20, 40, 60, 80, 100, 120],
    "label_horizons_1d": [1, 2, 3, 5, 12, 15],
    "execution_min_confidence": 0.6,
    "execution_min_agreement": 2,
    "execution_direction_band": 0.3,
    "execution_min_signal_strength": 0.5,
    "execution_max_position_pct": 0.02,
    "execution_max_positions": 5,
    "execution_max_exposure": 0.2,
}


def _use_exec_rc(app, overrides=None):
    """Replace the app's runtime_config mock to return execution_* keys too."""
    data = {**_DEFAULT_EXEC_RC, **(overrides or {})}
    app["runtime_config"].get = AsyncMock(side_effect=lambda key: data[key])


def _base_new_job_body(**extra):
    body = {
        "name": "",
        "symbols": ["SPY"],
        "timeframe": "1m",
        "horizons": [5],
        "range_start": "2025-01-01T00:00:00+00:00",
        "range_end":   "2025-01-02T00:00:00+00:00",
        "train_days": 1,
        "test_days": 1,
        "embargo_min": 5,
        "mode": "rolling",
    }
    body.update(extra)
    return body


@pytest.mark.asyncio
async def test_create_without_overrides_snapshots_runtime_config(
    aiohttp_client, app,
):
    _use_exec_rc(app)
    app["jobs_db"].insert_job = AsyncMock(return_value=1)
    client = await aiohttp_client(app)
    resp = await client.post("/api/backtests", json=_base_new_job_body())
    assert resp.status == 201
    call_kwargs = app["jobs_db"].insert_job.call_args.kwargs
    config = call_kwargs["config"]
    assert config["min_confidence"] == 0.6
    assert config["min_agreement"] == 2
    assert config["direction_band"] == 0.3
    assert config["min_signal_strength"] == 0.5
    assert config["max_position_pct"] == 0.02
    assert config["max_positions"] == 5
    assert config["max_exposure"] == 0.2


@pytest.mark.asyncio
async def test_create_with_valid_overrides_persists_them(aiohttp_client, app):
    _use_exec_rc(app)
    app["jobs_db"].insert_job = AsyncMock(return_value=2)
    client = await aiohttp_client(app)
    body = _base_new_job_body(
        min_confidence=0.8,
        min_agreement=4,
        direction_band=0.25,
        min_signal_strength=0.7,
        max_position_pct=0.05,
        max_positions=3,
        max_exposure=0.1,
    )
    resp = await client.post("/api/backtests", json=body)
    assert resp.status == 201
    config = app["jobs_db"].insert_job.call_args.kwargs["config"]
    assert config["min_confidence"] == 0.8
    assert config["min_agreement"] == 4
    assert config["direction_band"] == 0.25
    assert config["min_signal_strength"] == 0.7
    assert config["max_position_pct"] == 0.05
    assert config["max_positions"] == 3
    assert config["max_exposure"] == 0.1


@pytest.mark.asyncio
async def test_create_rejects_out_of_range_override(aiohttp_client, app):
    _use_exec_rc(app)
    client = await aiohttp_client(app)
    resp = await client.post(
        "/api/backtests", json=_base_new_job_body(min_confidence=1.5),
    )
    assert resp.status == 400
    payload = await resp.json()
    assert "min_confidence" in payload["errors"]


@pytest.mark.asyncio
async def test_create_rejects_wrong_type_override(aiohttp_client, app):
    _use_exec_rc(app)
    client = await aiohttp_client(app)
    resp = await client.post(
        "/api/backtests", json=_base_new_job_body(min_agreement=3.5),
    )
    assert resp.status == 400
    payload = await resp.json()
    assert "min_agreement" in payload["errors"]


@pytest.mark.asyncio
async def test_create_partial_overrides_fill_rest_from_runtime_config(
    aiohttp_client, app,
):
    _use_exec_rc(app)
    app["jobs_db"].insert_job = AsyncMock(return_value=3)
    client = await aiohttp_client(app)
    resp = await client.post(
        "/api/backtests", json=_base_new_job_body(min_confidence=0.9),
    )
    assert resp.status == 201
    config = app["jobs_db"].insert_job.call_args.kwargs["config"]
    assert config["min_confidence"] == 0.9         # user-supplied
    assert config["min_agreement"] == 2            # snapshot
    assert config["direction_band"] == 0.3         # snapshot
    assert config["max_position_pct"] == 0.02      # snapshot


@pytest.mark.asyncio
async def test_equity_endpoint_includes_drawdown_and_fold_boundaries(
    aiohttp_client, backtests_app,
):
    """Existing /api/backtests/{id}/equity returns a shape; Plan 4 adds
    `drawdown` per point and a top-level `fold_boundaries` array."""
    conn_mock = backtests_app["results_db"].db.pool.acquire.return_value.__aenter__.return_value
    conn_mock.fetch = AsyncMock(return_value=[
        {"timestamp": datetime(2025, 1, 1, tzinfo=timezone.utc),
         "equity": 100000.0, "fold_id": 1},
        {"timestamp": datetime(2025, 1, 2, tzinfo=timezone.utc),
         "equity": 99000.0, "fold_id": 1},
    ])
    client = await aiohttp_client(backtests_app)
    resp = await client.get("/api/backtests/1/equity")
    assert resp.status == 200
    body = await resp.json()
    # Shape can be dict {series, fold_boundaries} OR list for back-compat.
    if isinstance(body, dict):
        assert "series" in body
        assert "fold_boundaries" in body
        for p in body["series"]:
            assert "drawdown" in p
    else:
        # Legacy shape — at minimum each point has drawdown.
        for p in body:
            assert "drawdown" in p


@pytest.mark.asyncio
async def test_folds_endpoint_embeds_metrics(aiohttp_client, backtests_app):
    conn_mock = backtests_app["results_db"].db.pool.acquire.return_value.__aenter__.return_value
    conn_mock.fetch = AsyncMock(return_value=[
        {"id": 1, "fold_index": 0,
         "train_start": None, "train_end": None,
         "test_start": None, "test_end": None,
         "metrics_json": '{"pnl": 500.0, "sharpe": 1.1, "max_drawdown": -0.05, "trades": 10, "hit_rate": 0.6}'},
    ])
    client = await aiohttp_client(backtests_app)
    resp = await client.get("/api/backtests/1/folds")
    assert resp.status == 200
    body = await resp.json()
    # Each fold row has extracted metrics.
    for row in body:
        for key in ("pnl", "sharpe", "max_drawdown", "trades", "hit_rate"):
            assert key in row


@pytest.mark.asyncio
async def test_list_includes_tags_and_notes(aiohttp_client, backtests_app):
    backtests_app["results_db"].list_runs = AsyncMock(return_value=[
        {"id": 1, "name": "r", "symbols": ["SPY"],
         "range_start": None, "range_end": None,
         "status": "completed",
         "started_at": None, "finished_at": None,
         "metrics": {"total_pnl": 1.0, "sharpe": 0.5, "max_dd": -0.1,
                     "n_trades": 2, "hit_rate": 0.5},
         "tags": ["baseline"], "notes": "v2 run"},
    ])
    backtests_app["jobs_db"].list_pending = AsyncMock(return_value=[])
    client = await aiohttp_client(backtests_app)
    resp = await client.get("/api/backtests")
    assert resp.status == 200
    body = await resp.json()
    assert "rows" in body
    for row in body["rows"]:
        assert "tags" in row
        assert "notes" in row


# --- Cancel endpoint tests ------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_job_by_job_id_returns_200(aiohttp_client, app):
    """POST /api/backtest-jobs/{job_id}/cancel on a queued job returns 200."""
    app["jobs_db"].cancel = AsyncMock(return_value="cancelled")
    client = await aiohttp_client(app)
    resp = await client.post("/api/backtest-jobs/42/cancel")
    assert resp.status == 200
    body = await resp.json()
    assert body["job_id"] == 42
    assert body["status"] == "cancelled"
    app["jobs_db"].cancel.assert_awaited_once_with(42)


@pytest.mark.asyncio
async def test_cancel_job_already_terminal_returns_409(aiohttp_client, app):
    app["jobs_db"].cancel = AsyncMock(return_value="already_terminal")
    client = await aiohttp_client(app)
    resp = await client.post("/api/backtest-jobs/42/cancel")
    assert resp.status == 409


@pytest.mark.asyncio
async def test_cancel_job_by_run_id_finds_and_cancels(aiohttp_client, app):
    """POST /api/backtests/{run_id}/cancel looks up the job and cancels it."""
    # The handler queries app["db"].pool.acquire() to find the job by run_id.
    conn_mock = app["db"].pool.acquire.return_value.__aenter__.return_value
    conn_mock.fetchval = AsyncMock(return_value=77)  # the job_id found
    app["jobs_db"].cancel = AsyncMock(return_value="cancelled")
    client = await aiohttp_client(app)
    resp = await client.post("/api/backtests/5/cancel")
    assert resp.status == 200
    body = await resp.json()
    assert body["run_id"] == 5
    assert body["job_id"] == 77
    assert body["status"] == "cancelled"
    app["jobs_db"].cancel.assert_awaited_once_with(77)


@pytest.mark.asyncio
async def test_cancel_by_run_id_nonexistent_returns_404(aiohttp_client, app):
    conn_mock = app["db"].pool.acquire.return_value.__aenter__.return_value
    conn_mock.fetchval = AsyncMock(return_value=None)  # no matching job
    client = await aiohttp_client(app)
    resp = await client.post("/api/backtests/999999999/cancel")
    assert resp.status == 404


@pytest.mark.asyncio
async def test_cancel_job_id_malformed_returns_400(aiohttp_client, app):
    client = await aiohttp_client(app)
    resp = await client.post("/api/backtest-jobs/abc/cancel")
    # aiohttp route expects `{job_id}`; a non-int may 400 via our ValueError
    # guard, or 404 if route constraint rejects first. Both mean rejection.
    assert resp.status in (400, 404)
