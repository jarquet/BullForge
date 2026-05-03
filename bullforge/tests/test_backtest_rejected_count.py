"""Tests for the per-fold rejected-orders metric.

The metric counts (a) signals dropped by the engine pre-gate when the next
bar's volume cap is non-positive and (b) orders the simulated broker rejects
(volume cap inside FillModel, missing next bar). These are all
volume-cap-related "couldn't submit" cases that the operator needs visibility
into when interpreting an otherwise-empty backtest.
"""
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import web

from services.backtest_service.fill_model import FillModel
from services.backtest_service.results_db import BacktestResultsDB
from services.backtest_service.simulated_broker import SimulatedBroker
from services.dashboard_service.backtests_api import register_backtests_routes


UTC = timezone.utc


def _bar(o, h, l, c, v):
    return {"open": o, "high": h, "low": l, "close": c, "volume": v}


@pytest.mark.asyncio
async def test_simulated_broker_tracks_rejected_count():
    fm = FillModel(slippage_bps=0.0, max_volume_pct=0.001)
    broker = SimulatedBroker(starting_cash=100_000.0, fill_model=fm)
    broker.set_clock(datetime(2026, 4, 20, 4, 0, tzinfo=UTC))

    assert broker.rejected_count == 0

    # 1) Reject due to FillModel volume cap (large qty vs tiny next-bar volume).
    broker.set_next_bar("SPY", _bar(100.0, 101.0, 99.5, 100.5, 1.0))
    await broker.submit_market_order("SPY", 500.0, "buy")
    assert broker.rejected_count == 1

    # 2) Reject due to no next bar set.
    await broker.submit_market_order("AAPL", 1.0, "buy")
    assert broker.rejected_count == 2

    # 3) A successful fill must NOT increment the rejected counter.
    broker.set_next_bar("MSFT", _bar(300.0, 301.0, 299.0, 300.5, 10_000_000))
    await broker.submit_market_order("MSFT", 1.0, "buy")
    assert broker.rejected_count == 2


@pytest.mark.asyncio
async def test_simulated_broker_rejected_count_independent_of_filled_orders():
    fm = FillModel(slippage_bps=0.0, max_volume_pct=1.0)
    broker = SimulatedBroker(starting_cash=100_000.0, fill_model=fm)
    broker.set_clock(datetime(2026, 4, 20, 4, 0, tzinfo=UTC))

    broker.set_next_bar("SPY", _bar(100.0, 101.0, 99.5, 100.5, 1_000_000))
    await broker.submit_market_order("SPY", 10.0, "buy")
    assert broker.rejected_count == 0


@pytest.mark.asyncio
async def test_update_fold_metrics_writes_rejected_orders_count():
    """BacktestResultsDB.update_fold_metrics writes rejected_orders_count to
    the new column on backtest_folds."""
    captured = {}

    async def _execute(sql, *args):
        captured["sql"] = sql
        captured["args"] = args
        return None

    conn = AsyncMock()
    conn.execute = _execute
    conn_cm = AsyncMock()
    conn_cm.__aenter__ = AsyncMock(return_value=conn)
    conn_cm.__aexit__ = AsyncMock(return_value=False)
    db = MagicMock()
    db.pool = MagicMock()
    db.pool.acquire = MagicMock(return_value=conn_cm)

    rdb = BacktestResultsDB(db)
    await rdb.update_fold_metrics(
        fold_id=42, metrics={"sharpe": 1.0}, rejected_orders_count=7,
    )

    assert "rejected_orders_count" in captured["sql"]
    # args order: metrics_json, rejected_orders_count, fold_id
    assert captured["args"][1] == 7
    assert captured["args"][2] == 42


@pytest.mark.asyncio
async def test_update_fold_metrics_default_kwarg_does_not_clobber_column():
    """Without an explicit ``rejected_orders_count`` kwarg, update_fold_metrics
    must NOT touch the column — earlier versions silently zeroed it on every
    metrics-only refresh, which is the bug this test locks against."""
    captured = {}

    async def _execute(sql, *args):
        captured["sql"] = sql
        captured["args"] = args
        return None

    conn = AsyncMock()
    conn.execute = _execute
    conn_cm = AsyncMock()
    conn_cm.__aenter__ = AsyncMock(return_value=conn)
    conn_cm.__aexit__ = AsyncMock(return_value=False)
    db = MagicMock()
    db.pool = MagicMock()
    db.pool.acquire = MagicMock(return_value=conn_cm)

    rdb = BacktestResultsDB(db)
    await rdb.update_fold_metrics(fold_id=1, metrics={})
    # SQL must NOT mention rejected_orders_count and must take exactly two
    # parameters (metrics_json, fold_id) — the column is left alone.
    assert "rejected_orders_count" not in captured["sql"]
    assert len(captured["args"]) == 2


@pytest.mark.asyncio
async def test_dashboard_folds_endpoint_surfaces_rejected_count(aiohttp_client):
    """GET /api/backtests/{id}/folds returns rejected_orders_count per fold."""
    app = web.Application()
    # Mock results_db with a connection that returns a row including the new col.
    conn_mock = AsyncMock()
    conn_mock.fetch = AsyncMock(return_value=[
        {
            "id": 1, "fold_index": 0,
            "train_start": None, "train_end": None,
            "test_start": None, "test_end": None,
            "metrics_json": '{"pnl": 0.0, "sharpe": 0.0, "max_drawdown": 0.0,'
                            ' "trades": 0, "hit_rate": 0.0}',
            "rejected_orders_count": 14,
        },
    ])
    conn_cm = AsyncMock()
    conn_cm.__aenter__ = AsyncMock(return_value=conn_mock)
    conn_cm.__aexit__ = AsyncMock(return_value=False)
    results_db = AsyncMock()
    results_db.db = MagicMock()
    results_db.db.pool = MagicMock()
    results_db.db.pool.acquire = MagicMock(return_value=conn_cm)
    app["results_db"] = results_db
    app["jobs_db"] = AsyncMock()
    app["db"] = MagicMock()
    app["runtime_config"] = AsyncMock()
    register_backtests_routes(app)

    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests/1/folds")
    assert resp.status == 200
    body = await resp.json()
    assert len(body) == 1
    assert body[0]["rejected_orders_count"] == 14
