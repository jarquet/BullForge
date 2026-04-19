from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from services.dashboard_service.backtests_extra_api import register_backtests_extra_routes


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.query_backtest_symbols = AsyncMock(return_value=[
        {"symbol": "AAPL", "trades": 5, "wins": 3, "win_rate": 0.6,
         "total_pnl": 12.0, "avg_pnl": 2.4},
    ])
    db.query_backtest_distributions = AsyncMock(return_value={
        "daily_pnl": [], "trade_pnl_histogram":
            {"labels": [], "counts": [], "count": 0, "median": 0.0, "mean": 0.0},
        "hold_time_buckets": [], "side_breakdown": [],
    })
    db.query_backtest_live_overlay = AsyncMock(return_value={
        "has_overlap": True, "backtest_series": [], "live_series": [],
        "symbols": ["AAPL"],
        "range_start": datetime(2026, 3, 1, tzinfo=timezone.utc),
        "range_end": datetime(2026, 3, 5, tzinfo=timezone.utc),
    })
    db.query_backtest_funnel = AsyncMock(return_value={
        "signals": 10, "confident": 6,
        "orders_placed": 5, "orders_filled": 5,
        "orders_closed": 5, "min_confidence": 0.6,
    })
    db.query_backtest_compare = AsyncMock(return_value={
        "runs": [{"run_id": 1, "name": "a", "symbols": ["X"],
                  "tags": [], "notes": None,
                  "equity_series": [],
                  "metrics": {"total_pnl": 10, "avg_sharpe": 1.1,
                               "min_max_dd": -0.05, "total_trades": 20,
                               "avg_hit_rate": 0.6}}],
    })
    db.query_backtest_sensitivity = AsyncMock(return_value={
        "swept_param": "min_confidence", "group_by": ["symbols"],
        "points": [{"run_id": 1, "group_key": [["AAPL"]],
                    "param_value": 0.5,
                    "metrics": {"total_pnl": 5, "avg_sharpe": 0.9,
                                 "min_max_dd": -0.04}}],
    })
    return db


@pytest.fixture
def extras_app(mock_db):
    app = web.Application()
    app["db"] = mock_db
    register_backtests_extra_routes(app)
    return app


@pytest.mark.asyncio
async def test_symbols_endpoint(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/42/symbols")
    assert resp.status == 200
    body = await resp.json()
    assert len(body) == 1
    assert body[0]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_distributions_endpoint(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/42/distributions")
    assert resp.status == 200
    body = await resp.json()
    assert "daily_pnl" in body
    assert "trade_pnl_histogram" in body


@pytest.mark.asyncio
async def test_live_overlay_endpoint(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/42/live-overlay")
    assert resp.status == 200
    body = await resp.json()
    assert body["has_overlap"] is True


@pytest.mark.asyncio
async def test_funnel_endpoint(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/42/funnel")
    assert resp.status == 200
    body = await resp.json()
    assert body["signals"] == 10


@pytest.mark.asyncio
async def test_compare_endpoint_single_id(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/compare?ids=1")
    assert resp.status == 200
    body = await resp.json()
    assert "runs" in body


@pytest.mark.asyncio
async def test_compare_endpoint_missing_ids(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/compare")
    assert resp.status == 400


@pytest.mark.asyncio
async def test_compare_endpoint_malformed_ids(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/compare?ids=abc,def")
    assert resp.status == 400


@pytest.mark.asyncio
async def test_sensitivity_endpoint(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get(
        "/api/backtests/sensitivity?group_by=symbols&swept_param=min_confidence",
    )
    assert resp.status == 200
    body = await resp.json()
    assert body["swept_param"] == "min_confidence"


@pytest.mark.asyncio
async def test_sensitivity_missing_param(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/sensitivity")
    assert resp.status == 400


@pytest.mark.asyncio
async def test_invalid_run_id_returns_400(aiohttp_client, extras_app):
    client = await aiohttp_client(extras_app)
    resp = await client.get("/api/backtests/not-a-number/symbols")
    assert resp.status == 400


@pytest.fixture
def mock_db_with_writes(mock_db):
    mock_db.set_backtest_tags = AsyncMock(return_value=None)
    mock_db.get_backtest_tags = AsyncMock(return_value=["new-tag"])
    mock_db.set_backtest_notes = AsyncMock(return_value=None)
    mock_db.get_backtest_notes = AsyncMock(return_value="a note")
    mock_db.insert_runtime_config_audit = AsyncMock(return_value=1)
    return mock_db


@pytest.fixture
def extras_app_with_writes(mock_db_with_writes):
    app = web.Application()
    app["db"] = mock_db_with_writes
    # runtime_config fixture mock
    rc = AsyncMock()
    rc.get = AsyncMock(return_value=0.6)
    rc.set = AsyncMock(return_value=None)
    app["runtime_config"] = rc
    register_backtests_extra_routes(app)
    return app


@pytest.mark.asyncio
async def test_post_tags_replaces_list(aiohttp_client, extras_app_with_writes, mock_db_with_writes):
    client = await aiohttp_client(extras_app_with_writes)
    resp = await client.post(
        "/api/backtests/42/tags",
        json={"tags": ["tagA", "tagB"]},
    )
    assert resp.status == 200
    body = await resp.json()
    assert "tags" in body
    mock_db_with_writes.set_backtest_tags.assert_awaited_once_with(42, ["tagA", "tagB"])


@pytest.mark.asyncio
async def test_post_tags_rejects_non_list(aiohttp_client, extras_app_with_writes):
    client = await aiohttp_client(extras_app_with_writes)
    resp = await client.post(
        "/api/backtests/42/tags", json={"tags": "not-a-list"},
    )
    assert resp.status == 400


@pytest.mark.asyncio
async def test_post_notes_saves_string(aiohttp_client, extras_app_with_writes, mock_db_with_writes):
    client = await aiohttp_client(extras_app_with_writes)
    resp = await client.post(
        "/api/backtests/42/notes",
        json={"notes": "my notes"},
    )
    assert resp.status == 200
    mock_db_with_writes.set_backtest_notes.assert_awaited_once_with(42, "my notes")


@pytest.mark.asyncio
async def test_post_promote_config_writes_diff_and_audit(
    aiohttp_client, extras_app_with_writes, mock_db_with_writes,
):
    mock_db_with_writes.get_backtest_tags.return_value = []
    # query_backtest_compare is already mocked; use the compare fixture for run metadata.
    # We need the run's config_json to drive the diff — inject via a simple query mock.
    mock_db_with_writes.pool = AsyncMock()
    # Mock: the handler fetches the run's config_json itself via pool.acquire.

    async def fetchrow_mock(query, run_id):
        return {
            "config_json": {"min_confidence": 0.75, "max_position_pct": 0.03},
        }

    conn_cm = AsyncMock()
    conn_cm.__aenter__.return_value.fetchrow = fetchrow_mock
    conn_cm.__aexit__.return_value = False
    mock_db_with_writes.pool.acquire = lambda: conn_cm

    client = await aiohttp_client(extras_app_with_writes)
    resp = await client.post(
        "/api/backtests/42/promote-config",
        json={"keys": ["min_confidence"], "note": "promoted after sensitivity sweep"},
    )
    assert resp.status == 200
    body = await resp.json()
    assert "diff" in body
    # Old value 0.6 → new value 0.75.
    assert any(d["key"] == "execution_min_confidence" and
               d["new_value"] == 0.75 for d in body["diff"])
    # runtime_config.set was called with the execution_ prefix.
    extras_app_with_writes["runtime_config"].set.assert_awaited()
    mock_db_with_writes.insert_runtime_config_audit.assert_awaited()
