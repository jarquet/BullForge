from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from services.dashboard_service.trading_api import register_trading_routes


@pytest.fixture
def mock_db_trading():
    db = AsyncMock()
    db.query_equity_curve = AsyncMock(return_value=[
        {
            "timestamp": datetime(2026, 4, 10, tzinfo=timezone.utc),
            "realized_pnl": 100.0, "realized_equity": 100_100.0,
            "trailing_peak": 100_100.0, "drawdown": 0.0,
        },
    ])
    db.query_distributions = AsyncMock(return_value={
        "daily_pnl": [{"date": datetime(2026, 4, 10, tzinfo=timezone.utc),
                       "pnl": 100.0, "trades": 2}],
        "trade_pnl_histogram": {"labels": [-5, 0, 5],
                                 "counts": [1, 0, 2], "median": 3.0, "mean": 1.5},
        "hold_time_buckets": [{"bucket": "<5m", "trades": 2, "avg_pnl": 1.5}],
        "side_breakdown": [
            {"side": "long", "trades": 3, "wins": 2, "win_rate": 0.667,
             "total_pnl": 5.0, "avg_pnl": 1.67},
            {"side": "short", "trades": 1, "wins": 1, "win_rate": 1.0,
             "total_pnl": 3.0, "avg_pnl": 3.0},
        ],
    })
    db.query_streaks = AsyncMock(return_value={
        "current_streak_kind": "win", "current_streak_length": 2,
        "longest_win_streak": 3, "longest_loss_streak": 1,
        "ribbon": [{"timestamp": datetime(2026, 4, 10, tzinfo=timezone.utc),
                    "win": True}],
    })
    db.query_heatmap_time_of_day = AsyncMock(return_value={
        "x_labels": ["00:00"], "y_labels": ["Mon"],
        "cells": [[1.5]],
    })
    db.query_exposure_timeline = AsyncMock(return_value=[
        {"timestamp": datetime(2026, 4, 10, tzinfo=timezone.utc),
         "total_exposure_pct": 0.05, "by_symbol": {"AAPL": 0.05}},
    ])
    db.query_benchmark = AsyncMock(return_value={
        "strategy_series": [{"timestamp": datetime(2026, 4, 10, tzinfo=timezone.utc),
                             "equity": 100_100.0}],
        "spy_series": [{"timestamp": datetime(2026, 4, 10, tzinfo=timezone.utc),
                         "equity": 100_050.0}],
        "stats": {"annualized_return": 0.12, "annualized_vol": 0.18,
                  "sharpe": 0.67, "max_drawdown": -0.03,
                  "beta": 0.85, "alpha": 0.04},
    })
    db.query_symbol_drilldown = AsyncMock(return_value={
        "symbol": "AAPL", "trade_count": 1,
        "equity_series": [], "trades": [], "side_breakdown": [],
        "win_rate_over_time": [],
    })
    db.query_latest_metric_snapshot = AsyncMock(return_value={
        "metrics": {"equity": 100_000.0},
        "timestamp": datetime(2026, 4, 10, tzinfo=timezone.utc),
    })
    return db


@pytest.fixture
def trading_app(mock_db_trading):
    app = web.Application()
    app["db"] = mock_db_trading
    register_trading_routes(app)
    return app


@pytest.mark.asyncio
async def test_equity_endpoint(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/equity")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    assert body[0]["realized_equity"] == 100_100.0


@pytest.mark.asyncio
async def test_equity_with_window(aiohttp_client, trading_app, mock_db_trading):
    client = await aiohttp_client(trading_app)
    resp = await client.get(
        "/api/trading/equity?from=2026-04-01T00:00:00Z&to=2026-04-11T00:00:00Z",
    )
    assert resp.status == 200
    call = mock_db_trading.query_equity_curve.call_args
    assert call.kwargs.get("from_dt") is not None
    assert call.kwargs.get("to_dt") is not None


@pytest.mark.asyncio
async def test_distributions_endpoint(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/distributions")
    assert resp.status == 200
    body = await resp.json()
    assert "daily_pnl" in body
    assert "trade_pnl_histogram" in body
    assert "hold_time_buckets" in body
    assert "side_breakdown" in body


@pytest.mark.asyncio
async def test_streaks_endpoint(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/streaks")
    assert resp.status == 200
    body = await resp.json()
    assert body["current_streak_kind"] == "win"


@pytest.mark.asyncio
async def test_heatmap_endpoint(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/heatmap?kind=time")
    assert resp.status == 200
    body = await resp.json()
    assert "x_labels" in body
    assert "cells" in body


@pytest.mark.asyncio
async def test_exposure_endpoint(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/exposure")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)


@pytest.mark.asyncio
async def test_benchmark_endpoint(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/benchmark")
    assert resp.status == 200
    body = await resp.json()
    assert "strategy_series" in body
    assert "spy_series" in body
    assert body["stats"]["sharpe"] == pytest.approx(0.67)


@pytest.mark.asyncio
async def test_symbol_drilldown_endpoint(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/symbols/AAPL")
    assert resp.status == 200
    body = await resp.json()
    assert body["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_equity_rejects_malformed_from(aiohttp_client, trading_app):
    client = await aiohttp_client(trading_app)
    resp = await client.get("/api/trading/equity?from=garbage")
    assert resp.status == 400
