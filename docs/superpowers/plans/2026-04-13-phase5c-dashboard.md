# Phase 5C: Dashboard Service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a lightweight web dashboard that displays real-time trading metrics, model fidelity status, trade/order history, and halt events — all served from a read-only aiohttp service querying the same database the monitoring service writes to.

**Architecture:** A standalone `dashboard_service` runs an aiohttp web server with two layers: a JSON REST API (`/api/*`) and a single-page static dashboard (`index.html` + vanilla JS). The API queries `metric_snapshots`, `model_status`, `halt_events`, `trades`, and `orders` tables — all populated by Phase 5A's monitoring service and execution service. No data is written by the dashboard. The static frontend uses `fetch()` with a configurable auto-refresh interval to poll the API and render data into pre-built HTML sections. No frontend framework — vanilla JS keeps the dependency footprint zero and the build step nonexistent.

**Tech Stack:** Python 3.12+, aiohttp, asyncpg (via shared/db.py), pytest, pytest-asyncio

**Depends on:** Phase 5A (monitoring service) must be implemented first. The dashboard reads tables and data that 5A creates and populates. Phase 5B (Telegram bot) is independent — the dashboard does not interact with Telegram.

---

## File Structure

```
bullforge/
  shared/
    config.py                              — (modify) Add dashboard_port setting
    db.py                                  — (modify) Add query_metric_snapshots, query_latest_metric_snapshot, query_halt_history methods

  services/
    dashboard_service/
      __init__.py                          — Empty package marker
      Dockerfile                           — Container image for dashboard service
      requirements.txt                     — aiohttp, asyncpg
      main.py                              — Entrypoint: start aiohttp server
      api.py                               — REST API route handlers (all read-only)
      static/
        index.html                         — Single-page dashboard with tabbed layout
        dashboard.js                       — Fetch API data + render into DOM
        style.css                          — Dashboard styles (dark theme)

  tests/
    test_dashboard_db.py                   — Tests for new DB query methods
    test_dashboard_api.py                  — Tests for API route handlers
```

---

## Task 1: Config Settings & Dependency

**Files:**
- Modify: `shared/config.py`
- Create: `services/dashboard_service/requirements.txt`
- Modify: `tests/test_config.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_config.py`:

```python
def test_dashboard_settings_defaults(settings):
    assert settings.dashboard_port == 8084
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd bullforge && python -m pytest tests/test_config.py::test_dashboard_settings_defaults -v`
Expected: FAIL — `AttributeError: 'Settings' object has no attribute 'dashboard_port'`

- [ ] **Step 3: Add dashboard setting to Settings**

In `shared/config.py`, append this field inside the `Settings` class after the monitoring settings block (after the `drawdown_halt_threshold` field):

```python
    # Dashboard service
    dashboard_port: int = field(
        default_factory=lambda: int(os.getenv("DASHBOARD_PORT", "8084"))
    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd bullforge && python -m pytest tests/test_config.py -v`
Expected: ALL PASS

- [ ] **Step 5: Create requirements.txt**

Create `services/dashboard_service/requirements.txt`:

```
aiohttp>=3.9
asyncpg>=0.29
```

- [ ] **Step 6: Create __init__.py**

Create `services/dashboard_service/__init__.py` as an empty file.

- [ ] **Step 7: Commit**

```bash
git add shared/config.py tests/test_config.py services/dashboard_service/requirements.txt services/dashboard_service/__init__.py
git commit -m "feat: add dashboard config setting and service skeleton"
```

---

## Task 2: DB Query Methods for Dashboard

**Files:**
- Modify: `shared/db.py`
- Create: `tests/test_dashboard_db.py`

The dashboard needs to read `metric_snapshots` and `halt_events` history. The existing DB methods (`get_active_halts`, `get_all_model_statuses`, `query_closed_trades`, `query_orders`) already cover most needs. We need three new methods:

1. `query_latest_metric_snapshot(category, scope, window)` — get the most recent snapshot for a given category/scope/window
2. `query_metric_snapshots(category, scope, window, limit)` — get recent snapshots for time-series display
3. `query_halt_history(limit)` — get all halt events (active and resolved), ordered by time

- [ ] **Step 1: Write the failing tests**

Create `tests/test_dashboard_db.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_dashboard_db.py -v`
Expected: FAIL — `AttributeError: 'Database' object has no attribute 'query_latest_metric_snapshot'`

- [ ] **Step 3: Add the three new DB methods**

In `shared/db.py`, append after the `insert_metric_snapshot` method (at the end of the file):

```python
    async def query_latest_metric_snapshot(
        self,
        category: str,
        scope: str,
        window: str,
    ) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, timestamp, category, scope, "window", metrics
                FROM metric_snapshots
                WHERE category = $1 AND scope = $2 AND "window" = $3
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                category,
                scope,
                window,
            )
        if row is None:
            return None
        return {
            "id": row["id"],
            "timestamp": row["timestamp"],
            "category": row["category"],
            "scope": row["scope"],
            "window": row["window"],
            "metrics": json.loads(row["metrics"]),
        }

    async def query_metric_snapshots(
        self,
        category: str,
        scope: str,
        window: str,
        limit: int = 60,
    ) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, timestamp, category, scope, "window", metrics
                FROM metric_snapshots
                WHERE category = $1 AND scope = $2 AND "window" = $3
                ORDER BY timestamp DESC
                LIMIT $4
                """,
                category,
                scope,
                window,
                limit,
            )
        return [
            {
                "id": r["id"],
                "timestamp": r["timestamp"],
                "category": r["category"],
                "scope": r["scope"],
                "window": r["window"],
                "metrics": json.loads(r["metrics"]),
            }
            for r in rows
        ]

    async def query_halt_history(self, limit: int = 50) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, tier, reason, direction, activated_at, resolved_at
                FROM halt_events
                ORDER BY activated_at DESC
                LIMIT $1
                """,
                limit,
            )
        return [dict(r) for r in rows]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_dashboard_db.py -v`
Expected: ALL PASS (4 tests)

- [ ] **Step 5: Run the full test suite**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add shared/db.py tests/test_dashboard_db.py
git commit -m "feat: add DB query methods for dashboard metric snapshots and halt history"
```

---

## Task 3: REST API Route Handlers

**Files:**
- Create: `services/dashboard_service/api.py`
- Create: `tests/test_dashboard_api.py`

The API module defines aiohttp route handlers that query the DB and return JSON. All endpoints are read-only. The `aiohttp.web.Application` is created with the DB instance stored in `app["db"]`.

API endpoints:
- `GET /api/status` — system overview: latest account metrics, active halts, model statuses
- `GET /api/metrics/trading?window=all` — trading metrics for a given window (all, last_20, last_50)
- `GET /api/metrics/account` — latest account metrics
- `GET /api/metrics/symbols` — per-symbol trading metrics
- `GET /api/metrics/history?category=trading&scope=&window=all&limit=60` — time-series of metric snapshots
- `GET /api/models` — all model statuses
- `GET /api/trades?limit=50` — recent closed trades
- `GET /api/orders?limit=50` — recent orders
- `GET /api/halts` — active halts + recent halt history

- [ ] **Step 1: Write the failing tests**

Create `tests/test_dashboard_api.py`:

```python
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from shared.models import ModelStatus, Trade, Order


@pytest.fixture
def mock_db():
    db = AsyncMock()

    # Status endpoint
    db.query_latest_metric_snapshot = AsyncMock(return_value={
        "id": 1,
        "timestamp": datetime(2026, 4, 13, 14, 0, 0, tzinfo=timezone.utc),
        "category": "account",
        "scope": "",
        "window": "all",
        "metrics": {
            "equity": 100000.0,
            "buying_power": 200000.0,
            "cash": 100000.0,
            "portfolio_value": 100000.0,
            "current_drawdown": 0.02,
            "max_drawdown": 0.05,
            "peak_equity": 102000.0,
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


@pytest.fixture
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd bullforge && python -m pytest tests/test_dashboard_api.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'services.dashboard_service.api'`

- [ ] **Step 3: Implement the API module**

Create `services/dashboard_service/api.py`:

```python
from __future__ import annotations

import logging
from datetime import datetime, timezone

from aiohttp import web

from shared.db import Database

logger = logging.getLogger(__name__)


def _serialize_datetime(obj):
    """Convert datetime objects to ISO format strings for JSON."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def _serialize_dict(d: dict) -> dict:
    return {
        k: _serialize_datetime(v) if isinstance(v, datetime) else v
        for k, v in d.items()
    }


def create_api_app(db: Database) -> web.Application:
    app = web.Application()
    app["db"] = db

    app.router.add_get("/api/status", _handle_status)
    app.router.add_get("/api/metrics/trading", _handle_metrics_trading)
    app.router.add_get("/api/metrics/account", _handle_metrics_account)
    app.router.add_get("/api/metrics/symbols", _handle_metrics_symbols)
    app.router.add_get("/api/metrics/history", _handle_metrics_history)
    app.router.add_get("/api/models", _handle_models)
    app.router.add_get("/api/trades", _handle_trades)
    app.router.add_get("/api/orders", _handle_orders)
    app.router.add_get("/api/halts", _handle_halts)

    return app


async def _handle_status(request: web.Request) -> web.Response:
    db: Database = request.app["db"]

    account = await db.query_latest_metric_snapshot("account", "", "all")
    halts = await db.get_active_halts()
    models = await db.get_all_model_statuses()

    return web.json_response({
        "account": _serialize_dict(account["metrics"]) if account else None,
        "account_updated": (
            account["timestamp"].isoformat() if account else None
        ),
        "halts": [_serialize_dict(h) for h in halts],
        "is_halted": len(halts) > 0 and any(h["tier"] == 4 for h in halts),
        "models": [
            {
                "model_id": m.model_id,
                "status": m.status,
                "reason": m.reason,
                "updated_at": m.updated_at.isoformat(),
            }
            for m in models
        ],
    })


async def _handle_metrics_trading(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    window = request.query.get("window", "all")

    snapshot = await db.query_latest_metric_snapshot("trading", "", window)

    if snapshot is None:
        return web.json_response({"metrics": None, "timestamp": None})

    return web.json_response({
        "metrics": snapshot["metrics"],
        "timestamp": snapshot["timestamp"].isoformat(),
    })


async def _handle_metrics_account(request: web.Request) -> web.Response:
    db: Database = request.app["db"]

    snapshot = await db.query_latest_metric_snapshot("account", "", "all")

    if snapshot is None:
        return web.json_response({"metrics": None, "timestamp": None})

    return web.json_response({
        "metrics": snapshot["metrics"],
        "timestamp": snapshot["timestamp"].isoformat(),
    })


async def _handle_metrics_symbols(request: web.Request) -> web.Response:
    db: Database = request.app["db"]

    # The monitoring service stores per-symbol metrics with
    # category="symbol", scope=<symbol_name>, window="all".
    # Discover symbols from recent closed trades, then query each.
    closed_trades = await db.query_closed_trades(limit=500)
    symbols = sorted(set(t.symbol for t in closed_trades))

    result = {}
    for sym in symbols:
        snap = await db.query_latest_metric_snapshot("symbol", sym, "all")
        if snap:
            result[sym] = {
                "metrics": snap["metrics"],
                "timestamp": snap["timestamp"].isoformat(),
            }

    return web.json_response(result)


async def _handle_metrics_history(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    category = request.query.get("category", "trading")
    scope = request.query.get("scope", "")
    window = request.query.get("window", "all")
    limit = min(int(request.query.get("limit", "60")), 500)

    snapshots = await db.query_metric_snapshots(category, scope, window, limit)

    return web.json_response([
        {
            "timestamp": s["timestamp"].isoformat(),
            "metrics": s["metrics"],
        }
        for s in snapshots
    ])


async def _handle_models(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    models = await db.get_all_model_statuses()

    return web.json_response([
        {
            "model_id": m.model_id,
            "status": m.status,
            "reason": m.reason,
            "updated_at": m.updated_at.isoformat(),
        }
        for m in models
    ])


async def _handle_trades(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    limit = min(int(request.query.get("limit", "50")), 500)

    trades = await db.query_closed_trades(limit=limit)

    return web.json_response([
        {
            "id": t.id,
            "symbol": t.symbol,
            "side": t.side,
            "entry_time": t.entry_time.isoformat(),
            "exit_time": t.exit_time.isoformat() if t.exit_time else None,
            "entry_price": t.entry_price,
            "exit_price": t.exit_price,
            "quantity": t.quantity,
            "pnl": t.pnl,
            "hold_duration_seconds": t.hold_duration_seconds,
        }
        for t in trades
    ])


async def _handle_orders(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    limit = min(int(request.query.get("limit", "50")), 500)

    orders = await db.query_orders(limit=limit)

    return web.json_response([
        {
            "id": o.id,
            "symbol": o.symbol,
            "side": o.side,
            "quantity": o.quantity,
            "order_type": o.order_type,
            "submitted_at": o.submitted_at.isoformat(),
            "signal_strength": o.signal_strength,
            "status": o.status,
            "filled_at": o.filled_at.isoformat() if o.filled_at else None,
            "filled_price": o.filled_price,
            "filled_quantity": o.filled_quantity,
        }
        for o in orders
    ])


async def _handle_halts(request: web.Request) -> web.Response:
    db: Database = request.app["db"]

    active = await db.get_active_halts()
    history = await db.query_halt_history(limit=50)

    return web.json_response({
        "active": [_serialize_dict(h) for h in active],
        "history": [_serialize_dict(h) for h in history],
    })
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd bullforge && python -m pytest tests/test_dashboard_api.py -v`
Expected: ALL PASS (11 tests)

- [ ] **Step 5: Run the full test suite**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add services/dashboard_service/api.py tests/test_dashboard_api.py
git commit -m "feat: dashboard REST API with read-only endpoints for metrics, trades, orders, halts"
```

---

## Task 4: Static Dashboard (HTML + JS + CSS)

**Files:**
- Create: `services/dashboard_service/static/index.html`
- Create: `services/dashboard_service/static/dashboard.js`
- Create: `services/dashboard_service/static/style.css`

This task creates the frontend. No tests — it's static files. Verification is manual via browser.

- [ ] **Step 1: Create the HTML page**

Create `services/dashboard_service/static/index.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BullForge Dashboard</title>
    <link rel="stylesheet" href="/static/style.css">
</head>
<body>
    <header>
        <h1>BullForge</h1>
        <div id="system-status" class="status-badge status-unknown">Loading...</div>
        <div id="last-updated" class="last-updated"></div>
    </header>

    <nav>
        <button class="tab active" data-tab="overview">Overview</button>
        <button class="tab" data-tab="trading">Trading</button>
        <button class="tab" data-tab="models">Models</button>
        <button class="tab" data-tab="trades">Trades</button>
        <button class="tab" data-tab="orders">Orders</button>
        <button class="tab" data-tab="halts">Halts</button>
    </nav>

    <!-- Overview Tab -->
    <section id="tab-overview" class="tab-content active">
        <div class="card-grid">
            <div class="card">
                <h3>Equity</h3>
                <div id="ov-equity" class="metric-value">--</div>
            </div>
            <div class="card">
                <h3>Buying Power</h3>
                <div id="ov-buying-power" class="metric-value">--</div>
            </div>
            <div class="card">
                <h3>Drawdown</h3>
                <div id="ov-drawdown" class="metric-value">--</div>
            </div>
            <div class="card">
                <h3>Max Drawdown</h3>
                <div id="ov-max-drawdown" class="metric-value">--</div>
            </div>
        </div>

        <div class="card-grid">
            <div class="card">
                <h3>Total Trades</h3>
                <div id="ov-total-trades" class="metric-value">--</div>
            </div>
            <div class="card">
                <h3>Win Rate</h3>
                <div id="ov-win-rate" class="metric-value">--</div>
            </div>
            <div class="card">
                <h3>Total P&amp;L</h3>
                <div id="ov-total-pnl" class="metric-value">--</div>
            </div>
            <div class="card">
                <h3>Profit Factor</h3>
                <div id="ov-profit-factor" class="metric-value">--</div>
            </div>
        </div>

        <h2>Active Halts</h2>
        <div id="ov-halts" class="halt-list">
            <p class="empty-state">No active halts</p>
        </div>

        <h2>Model Status</h2>
        <table id="ov-models" class="data-table">
            <thead>
                <tr>
                    <th>Model ID</th>
                    <th>Status</th>
                    <th>Reason</th>
                    <th>Updated</th>
                </tr>
            </thead>
            <tbody></tbody>
        </table>
    </section>

    <!-- Trading Tab -->
    <section id="tab-trading" class="tab-content">
        <div class="window-selector">
            <button class="window-btn active" data-window="all">All Time</button>
            <button class="window-btn" data-window="last_50">Last 50</button>
            <button class="window-btn" data-window="last_20">Last 20</button>
        </div>

        <div class="card-grid">
            <div class="card">
                <h3>Total Trades</h3>
                <div id="tr-total-trades" class="metric-value">--</div>
            </div>
            <div class="card">
                <h3>Win Rate</h3>
                <div id="tr-win-rate" class="metric-value">--</div>
            </div>
            <div class="card">
                <h3>Total P&amp;L</h3>
                <div id="tr-total-pnl" class="metric-value">--</div>
            </div>
            <div class="card">
                <h3>Profit Factor</h3>
                <div id="tr-profit-factor" class="metric-value">--</div>
            </div>
            <div class="card">
                <h3>Sharpe Ratio</h3>
                <div id="tr-sharpe" class="metric-value">--</div>
            </div>
            <div class="card">
                <h3>Avg Win</h3>
                <div id="tr-avg-win" class="metric-value">--</div>
            </div>
            <div class="card">
                <h3>Avg Loss</h3>
                <div id="tr-avg-loss" class="metric-value">--</div>
            </div>
            <div class="card">
                <h3>Avg Hold Time</h3>
                <div id="tr-avg-hold" class="metric-value">--</div>
            </div>
        </div>

        <h2>Per-Symbol Breakdown</h2>
        <div id="tr-symbols" class="symbol-grid"></div>
    </section>

    <!-- Models Tab -->
    <section id="tab-models" class="tab-content">
        <table id="models-table" class="data-table">
            <thead>
                <tr>
                    <th>Model ID</th>
                    <th>Status</th>
                    <th>Reason</th>
                    <th>Updated</th>
                </tr>
            </thead>
            <tbody></tbody>
        </table>
    </section>

    <!-- Trades Tab -->
    <section id="tab-trades" class="tab-content">
        <table id="trades-table" class="data-table">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Symbol</th>
                    <th>Side</th>
                    <th>Entry</th>
                    <th>Exit</th>
                    <th>Qty</th>
                    <th>P&amp;L</th>
                    <th>Duration</th>
                </tr>
            </thead>
            <tbody></tbody>
        </table>
    </section>

    <!-- Orders Tab -->
    <section id="tab-orders" class="tab-content">
        <table id="orders-table" class="data-table">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Symbol</th>
                    <th>Side</th>
                    <th>Qty</th>
                    <th>Type</th>
                    <th>Status</th>
                    <th>Signal</th>
                    <th>Filled Price</th>
                    <th>Submitted</th>
                </tr>
            </thead>
            <tbody></tbody>
        </table>
    </section>

    <!-- Halts Tab -->
    <section id="tab-halts" class="tab-content">
        <h2>Active Halts</h2>
        <div id="halts-active" class="halt-list">
            <p class="empty-state">No active halts</p>
        </div>

        <h2>Halt History</h2>
        <table id="halts-table" class="data-table">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Tier</th>
                    <th>Reason</th>
                    <th>Direction</th>
                    <th>Activated</th>
                    <th>Resolved</th>
                </tr>
            </thead>
            <tbody></tbody>
        </table>
    </section>

    <script src="/static/dashboard.js"></script>
</body>
</html>
```

- [ ] **Step 2: Create the CSS**

Create `services/dashboard_service/static/style.css`:

```css
:root {
    --bg: #1a1a2e;
    --bg-card: #16213e;
    --bg-header: #0f3460;
    --text: #e0e0e0;
    --text-muted: #888;
    --accent: #00b4d8;
    --green: #2ecc71;
    --red: #e74c3c;
    --yellow: #f39c12;
    --border: #2a2a4a;
}

* { margin: 0; padding: 0; box-sizing: border-box; }

body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
    background: var(--bg);
    color: var(--text);
    line-height: 1.5;
}

header {
    background: var(--bg-header);
    padding: 1rem 2rem;
    display: flex;
    align-items: center;
    gap: 1rem;
    border-bottom: 1px solid var(--border);
}

header h1 {
    font-size: 1.4rem;
    color: var(--accent);
}

.status-badge {
    padding: 0.25rem 0.75rem;
    border-radius: 4px;
    font-size: 0.85rem;
    font-weight: 600;
}

.status-active { background: var(--green); color: #000; }
.status-halted { background: var(--red); color: #fff; }
.status-unknown { background: var(--text-muted); color: #000; }

.last-updated {
    margin-left: auto;
    font-size: 0.8rem;
    color: var(--text-muted);
}

nav {
    display: flex;
    gap: 0;
    background: var(--bg-card);
    border-bottom: 1px solid var(--border);
}

.tab {
    padding: 0.75rem 1.5rem;
    background: none;
    border: none;
    color: var(--text-muted);
    cursor: pointer;
    font-size: 0.9rem;
    border-bottom: 2px solid transparent;
    transition: color 0.2s, border-color 0.2s;
}

.tab:hover { color: var(--text); }
.tab.active { color: var(--accent); border-bottom-color: var(--accent); }

.tab-content { display: none; padding: 1.5rem 2rem; }
.tab-content.active { display: block; }

.card-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: 1rem;
    margin-bottom: 1.5rem;
}

.card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem;
}

.card h3 {
    font-size: 0.75rem;
    text-transform: uppercase;
    color: var(--text-muted);
    margin-bottom: 0.5rem;
}

.metric-value {
    font-size: 1.5rem;
    font-weight: 700;
}

.metric-value.positive { color: var(--green); }
.metric-value.negative { color: var(--red); }

.data-table {
    width: 100%;
    border-collapse: collapse;
    background: var(--bg-card);
    border-radius: 8px;
    overflow: hidden;
    margin-bottom: 1.5rem;
}

.data-table th,
.data-table td {
    padding: 0.6rem 1rem;
    text-align: left;
    border-bottom: 1px solid var(--border);
    font-size: 0.85rem;
}

.data-table th {
    background: var(--bg-header);
    font-size: 0.75rem;
    text-transform: uppercase;
    color: var(--text-muted);
}

.data-table tr:hover td {
    background: rgba(0, 180, 216, 0.05);
}

.status-cell {
    padding: 0.15rem 0.5rem;
    border-radius: 3px;
    font-size: 0.8rem;
    font-weight: 600;
    display: inline-block;
}

.status-cell.active { background: rgba(46, 204, 113, 0.2); color: var(--green); }
.status-cell.warned { background: rgba(243, 156, 18, 0.2); color: var(--yellow); }
.status-cell.disabled { background: rgba(231, 76, 60, 0.2); color: var(--red); }

.halt-list .halt-item {
    background: rgba(231, 76, 60, 0.1);
    border: 1px solid var(--red);
    border-radius: 6px;
    padding: 0.75rem 1rem;
    margin-bottom: 0.5rem;
}

.halt-item .tier { font-weight: 700; color: var(--red); }

.empty-state {
    color: var(--text-muted);
    font-style: italic;
    padding: 1rem 0;
}

.window-selector {
    display: flex;
    gap: 0.5rem;
    margin-bottom: 1.5rem;
}

.window-btn {
    padding: 0.4rem 1rem;
    background: var(--bg-card);
    border: 1px solid var(--border);
    color: var(--text-muted);
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.85rem;
}

.window-btn.active {
    background: var(--accent);
    color: #000;
    border-color: var(--accent);
}

.symbol-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
    gap: 1rem;
}

.symbol-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem;
}

.symbol-card h3 { color: var(--accent); margin-bottom: 0.5rem; }
.symbol-card .stat { display: flex; justify-content: space-between; font-size: 0.85rem; padding: 0.2rem 0; }
.symbol-card .stat-label { color: var(--text-muted); }

h2 {
    font-size: 1.1rem;
    margin-bottom: 1rem;
    color: var(--text);
}
```

- [ ] **Step 3: Create the JavaScript**

Create `services/dashboard_service/static/dashboard.js`:

```javascript
const REFRESH_INTERVAL = 30000; // 30 seconds

// ── Tab switching ────────────────────────────────────────────────

document.querySelectorAll('.tab').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(s => s.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
    });
});

// ── Helpers ──────────────────────────────────────────────────────

function $(id) { return document.getElementById(id); }

function fmt(n, decimals) {
    if (n == null) return '--';
    return Number(n).toFixed(decimals !== undefined ? decimals : 2);
}

function fmtPct(n) {
    if (n == null) return '--';
    return (n * 100).toFixed(1) + '%';
}

function fmtMoney(n) {
    if (n == null) return '--';
    const sign = n >= 0 ? '' : '-';
    return sign + '$' + Math.abs(n).toLocaleString('en-US', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    });
}

function fmtDuration(seconds) {
    if (seconds == null) return '--';
    if (seconds < 60) return seconds + 's';
    if (seconds < 3600) return Math.floor(seconds / 60) + 'm';
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    return h + 'h ' + m + 'm';
}

function fmtTime(iso) {
    if (!iso) return '--';
    const d = new Date(iso);
    return d.toLocaleString();
}

function setPnlClass(el, value) {
    el.classList.remove('positive', 'negative');
    if (value > 0) el.classList.add('positive');
    else if (value < 0) el.classList.add('negative');
}

function setHtml(id, html) {
    const el = $(id);
    if (el) el.innerHTML = html;
}

// ── API fetch ────────────────────────────────────────────────────

async function api(path) {
    const resp = await fetch(path);
    if (!resp.ok) throw new Error(`API error: ${resp.status}`);
    return resp.json();
}

// ── Overview ─────────────────────────────────────────────────────

async function loadOverview() {
    const status = await api('/api/status');
    const trading = await api('/api/metrics/trading?window=all');

    // System status badge
    const badge = $('system-status');
    if (status.is_halted) {
        badge.textContent = 'HALTED';
        badge.className = 'status-badge status-halted';
    } else {
        badge.textContent = 'ACTIVE';
        badge.className = 'status-badge status-active';
    }

    // Account metrics
    const acct = status.account;
    if (acct) {
        setHtml('ov-equity', fmtMoney(acct.equity));
        setHtml('ov-buying-power', fmtMoney(acct.buying_power));

        const dd = $('ov-drawdown');
        dd.innerHTML = fmtPct(acct.current_drawdown);
        setPnlClass(dd, -(acct.current_drawdown || 0));

        const mdd = $('ov-max-drawdown');
        mdd.innerHTML = fmtPct(acct.max_drawdown);
        setPnlClass(mdd, -(acct.max_drawdown || 0));
    }

    // Trading summary
    const tm = trading.metrics;
    if (tm) {
        setHtml('ov-total-trades', tm.total_trades);
        setHtml('ov-win-rate', fmtPct(tm.win_rate));

        const pnl = $('ov-total-pnl');
        pnl.innerHTML = fmtMoney(tm.total_pnl);
        setPnlClass(pnl, tm.total_pnl);

        setHtml('ov-profit-factor', fmt(tm.profit_factor));
    }

    // Active halts
    const haltsDiv = $('ov-halts');
    if (status.halts.length === 0) {
        haltsDiv.innerHTML = '<p class="empty-state">No active halts</p>';
    } else {
        haltsDiv.innerHTML = status.halts.map(h =>
            `<div class="halt-item">
                <span class="tier">Tier ${h.tier}</span> — ${h.reason}
                <br><small>${fmtTime(h.activated_at)}</small>
            </div>`
        ).join('');
    }

    // Model status table
    const tbody = $('ov-models').querySelector('tbody');
    tbody.innerHTML = status.models.map(m =>
        `<tr>
            <td>${m.model_id}</td>
            <td><span class="status-cell ${m.status}">${m.status}</span></td>
            <td>${m.reason}</td>
            <td>${fmtTime(m.updated_at)}</td>
        </tr>`
    ).join('');

    $('last-updated').textContent = 'Updated: ' + new Date().toLocaleTimeString();
}

// ── Trading ──────────────────────────────────────────────────────

let currentWindow = 'all';

document.querySelectorAll('.window-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.window-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        currentWindow = btn.dataset.window;
        loadTrading();
    });
});

async function loadTrading() {
    const data = await api('/api/metrics/trading?window=' + currentWindow);
    const m = data.metrics;

    if (!m) {
        ['tr-total-trades', 'tr-win-rate', 'tr-total-pnl', 'tr-profit-factor',
         'tr-sharpe', 'tr-avg-win', 'tr-avg-loss', 'tr-avg-hold'].forEach(id =>
            setHtml(id, '--')
        );
        return;
    }

    setHtml('tr-total-trades', m.total_trades);
    setHtml('tr-win-rate', fmtPct(m.win_rate));

    const pnl = $('tr-total-pnl');
    pnl.innerHTML = fmtMoney(m.total_pnl);
    setPnlClass(pnl, m.total_pnl);

    setHtml('tr-profit-factor', fmt(m.profit_factor));
    setHtml('tr-sharpe', fmt(m.sharpe_ratio));

    const avgWin = $('tr-avg-win');
    avgWin.innerHTML = fmtMoney(m.avg_win);
    setPnlClass(avgWin, m.avg_win);

    const avgLoss = $('tr-avg-loss');
    avgLoss.innerHTML = fmtMoney(m.avg_loss);
    setPnlClass(avgLoss, m.avg_loss);

    setHtml('tr-avg-hold', fmtDuration(m.avg_hold_duration_seconds));

    // Per-symbol breakdown
    const symbols = await api('/api/metrics/symbols');
    const grid = $('tr-symbols');
    const entries = Object.entries(symbols);
    if (entries.length === 0) {
        grid.innerHTML = '<p class="empty-state">No symbol data yet</p>';
    } else {
        grid.innerHTML = entries.map(([sym, d]) => {
            const sm = d.metrics;
            return `<div class="symbol-card">
                <h3>${sym}</h3>
                <div class="stat"><span class="stat-label">Trades</span><span>${sm.total_trades}</span></div>
                <div class="stat"><span class="stat-label">Win Rate</span><span>${fmtPct(sm.win_rate)}</span></div>
                <div class="stat"><span class="stat-label">P&L</span><span>${fmtMoney(sm.total_pnl)}</span></div>
                <div class="stat"><span class="stat-label">Profit Factor</span><span>${fmt(sm.profit_factor)}</span></div>
            </div>`;
        }).join('');
    }
}

// ── Models ───────────────────────────────────────────────────────

async function loadModels() {
    const models = await api('/api/models');
    const tbody = $('models-table').querySelector('tbody');
    tbody.innerHTML = models.map(m =>
        `<tr>
            <td>${m.model_id}</td>
            <td><span class="status-cell ${m.status}">${m.status}</span></td>
            <td>${m.reason}</td>
            <td>${fmtTime(m.updated_at)}</td>
        </tr>`
    ).join('');
}

// ── Trades ───────────────────────────────────────────────────────

async function loadTrades() {
    const trades = await api('/api/trades?limit=100');
    const tbody = $('trades-table').querySelector('tbody');
    tbody.innerHTML = trades.map(t => {
        const pnlClass = t.pnl > 0 ? 'positive' : t.pnl < 0 ? 'negative' : '';
        return `<tr>
            <td>${t.id}</td>
            <td>${t.symbol}</td>
            <td>${t.side}</td>
            <td>${fmtMoney(t.entry_price)}</td>
            <td>${fmtMoney(t.exit_price)}</td>
            <td>${fmt(t.quantity, 1)}</td>
            <td class="${pnlClass}">${fmtMoney(t.pnl)}</td>
            <td>${fmtDuration(t.hold_duration_seconds)}</td>
        </tr>`;
    }).join('');
}

// ── Orders ───────────────────────────────────────────────────────

async function loadOrders() {
    const orders = await api('/api/orders?limit=100');
    const tbody = $('orders-table').querySelector('tbody');
    tbody.innerHTML = orders.map(o =>
        `<tr>
            <td>${o.id}</td>
            <td>${o.symbol}</td>
            <td>${o.side}</td>
            <td>${fmt(o.quantity, 1)}</td>
            <td>${o.order_type}</td>
            <td><span class="status-cell ${o.status === 'filled' ? 'active' : o.status === 'cancelled' ? 'disabled' : 'warned'}">${o.status}</span></td>
            <td>${fmtPct(o.signal_strength)}</td>
            <td>${fmtMoney(o.filled_price)}</td>
            <td>${fmtTime(o.submitted_at)}</td>
        </tr>`
    ).join('');
}

// ── Halts ────────────────────────────────────────────────────────

async function loadHalts() {
    const data = await api('/api/halts');

    const activeDiv = $('halts-active');
    if (data.active.length === 0) {
        activeDiv.innerHTML = '<p class="empty-state">No active halts</p>';
    } else {
        activeDiv.innerHTML = data.active.map(h =>
            `<div class="halt-item">
                <span class="tier">Tier ${h.tier}</span> — ${h.reason}
                ${h.direction ? ' (' + h.direction + ')' : ''}
                <br><small>Since ${fmtTime(h.activated_at)}</small>
            </div>`
        ).join('');
    }

    const tbody = $('halts-table').querySelector('tbody');
    tbody.innerHTML = data.history.map(h =>
        `<tr>
            <td>${h.id}</td>
            <td>Tier ${h.tier}</td>
            <td>${h.reason}</td>
            <td>${h.direction || '--'}</td>
            <td>${fmtTime(h.activated_at)}</td>
            <td>${h.resolved_at ? fmtTime(h.resolved_at) : '<span class="status-cell disabled">Active</span>'}</td>
        </tr>`
    ).join('');
}

// ── Refresh loop ─────────────────────────────────────────────────

async function refreshAll() {
    try {
        await Promise.all([
            loadOverview(),
            loadTrading(),
            loadModels(),
            loadTrades(),
            loadOrders(),
            loadHalts(),
        ]);
    } catch (err) {
        console.error('Dashboard refresh error:', err);
    }
}

refreshAll();
setInterval(refreshAll, REFRESH_INTERVAL);
```

- [ ] **Step 4: Verify the directory structure**

Run: `ls -la bullforge/services/dashboard_service/static/`
Expected: `index.html`, `dashboard.js`, `style.css`

- [ ] **Step 5: Commit**

```bash
git add services/dashboard_service/static/
git commit -m "feat: static dashboard with tabbed UI, dark theme, and auto-refresh"
```

---

## Task 5: Dashboard Service Entrypoint

**Files:**
- Create: `services/dashboard_service/main.py`

The entrypoint starts the aiohttp server, serving both the API routes and the static files directory. It also adds a `/health` endpoint.

- [ ] **Step 1: Create main.py**

Create `services/dashboard_service/main.py`:

```python
from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path

from aiohttp import web

from shared.config import Settings
from shared.db import Database
from services.dashboard_service.api import create_api_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("dashboard_service")

STATIC_DIR = Path(__file__).parent / "static"


async def main() -> None:
    settings = Settings()
    db = Database(settings)

    logger.info("Connecting to TimescaleDB...")
    await db.connect()

    # Build the aiohttp app with API routes
    app = create_api_app(db)

    # Health check
    async def health_handler(request: web.Request) -> web.Response:
        try:
            async with db.pool.acquire() as conn:
                await conn.execute("SELECT 1")
            return web.json_response({"db": "ok"}, status=200)
        except Exception as e:
            return web.json_response({"db": f"error: {e}"}, status=503)

    app.router.add_get("/health", health_handler)

    # Serve static files
    app.router.add_static("/static", STATIC_DIR, show_index=False)

    # Serve index.html at root
    async def index_handler(request: web.Request) -> web.FileResponse:
        return web.FileResponse(STATIC_DIR / "index.html")

    app.router.add_get("/", index_handler)

    # Start the server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", settings.dashboard_port)
    await site.start()

    logger.info("Dashboard running on http://0.0.0.0:%d", settings.dashboard_port)

    # Graceful shutdown
    shutdown_event = asyncio.Event()

    def handle_signal():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    await shutdown_event.wait()

    logger.info("Shutting down...")
    await runner.cleanup()
    await db.disconnect()
    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Verify the module imports correctly**

Run: `cd bullforge && python -c "from services.dashboard_service.main import main; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Run the full test suite**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add services/dashboard_service/main.py
git commit -m "feat: dashboard service entrypoint with aiohttp server"
```

---

## Task 6: Dockerfile & Docker Compose

**Files:**
- Create: `services/dashboard_service/Dockerfile`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Create the Dockerfile**

Create `services/dashboard_service/Dockerfile`:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY services/dashboard_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ /app/shared/
COPY services/dashboard_service/ /app/services/dashboard_service/
COPY migrations/ /app/migrations/

ENV PYTHONPATH=/app
CMD ["python", "-m", "services.dashboard_service.main"]
```

- [ ] **Step 2: Add dashboard_service to docker-compose.yml**

In `docker-compose.yml`, add the `dashboard_service` block after the `monitoring_service` block and before the `volumes:` section:

```yaml
  dashboard_service:
    build:
      context: .
      dockerfile: services/dashboard_service/Dockerfile
    env_file:
      - .env
    environment:
      TIMESCALEDB_HOST: timescaledb
      REDIS_HOST: redis
    ports:
      - "18084:8084"
    depends_on:
      timescaledb:
        condition: service_healthy
      monitoring_service:
        condition: service_started
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8084/health')"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 10s
    restart: unless-stopped
```

- [ ] **Step 3: Verify docker-compose syntax**

Run: `cd bullforge && docker compose config --quiet`
Expected: No errors

- [ ] **Step 4: Run the full test suite one final time**

Run: `cd bullforge && python -m pytest --tb=short -q`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add services/dashboard_service/Dockerfile docker-compose.yml
git commit -m "feat: dashboard service Dockerfile and Docker Compose integration"
```

---

## Summary

| Component | Description |
|---|---|
| **shared/config.py** | `dashboard_port` setting (default 8084) |
| **shared/db.py** | 3 new query methods: `query_latest_metric_snapshot`, `query_metric_snapshots`, `query_halt_history` |
| **api.py** | 9 REST endpoints: status, trading metrics, account metrics, symbol metrics, metric history, models, trades, orders, halts |
| **static/** | Single-page dashboard with 6 tabs (Overview, Trading, Models, Trades, Orders, Halts), dark theme, 30s auto-refresh |
| **main.py** | aiohttp entrypoint serving API + static files + health check on port 8084 |
| **Dockerfile** | Lightweight Python 3.12 container |

**API endpoints:**

| Endpoint | Description |
|---|---|
| `GET /api/status` | System overview: account, halts, model statuses |
| `GET /api/metrics/trading?window=all` | Trading metrics by window (all, last_20, last_50) |
| `GET /api/metrics/account` | Latest account metrics |
| `GET /api/metrics/symbols` | Per-symbol trading metrics |
| `GET /api/metrics/history?category=&scope=&window=&limit=` | Time-series of metric snapshots |
| `GET /api/models` | Model status list |
| `GET /api/trades?limit=50` | Recent closed trades |
| `GET /api/orders?limit=50` | Recent orders |
| `GET /api/halts` | Active halts + halt history |
| `GET /health` | Health check (DB connectivity) |
| `GET /` | Dashboard HTML page |

**Dashboard tabs:**

| Tab | Shows |
|---|---|
| Overview | Equity, buying power, drawdown, trade summary KPIs, active halts, model status |
| Trading | Per-window metrics (all/last 50/last 20), per-symbol breakdown |
| Models | Model ID, status badge, reason, last updated |
| Trades | Trade history table with P&L coloring |
| Orders | Order history with status badges |
| Halts | Active halt cards + halt history table |
