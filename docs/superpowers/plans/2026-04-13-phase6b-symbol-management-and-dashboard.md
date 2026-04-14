# Phase 6B — Symbol Management & Dashboard Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move trading symbols from ENV into Redis via `RuntimeConfig`, make them editable via dashboard (auto-triggering backfill), add freshness/fidelity/config-editor tabs, and replace meaningless numeric `model_id` with human labels throughout.

**Architecture:** Extend `RuntimeConfig` with JSON-typed `trading_symbols_equity`/`trading_symbols_crypto`. Dashboard API gains symbol CRUD, freshness, and config endpoints. Data pipeline subscribes to `config:symbols_changed` and hot-(un)subscribes to Alpaca streams and triggers historical backfill. Models use the next nightly retrain cycle to pick up new symbols.

**Tech Stack:** Python 3.12, aiohttp backend, HTMX frontend with Chart.js, Redis pub/sub, TimescaleDB.

**Depends on:** Phase 6A (RuntimeConfig must exist).

---

## File Structure

**Create:**
- `bullforge/services/dashboard_service/symbols_api.py` — `/api/symbols/*` handlers
- `bullforge/services/dashboard_service/config_api.py` — `/api/config` handlers
- `bullforge/services/dashboard_service/freshness_api.py` — `/api/freshness` handler
- `bullforge/services/dashboard_service/static/symbols.html`
- `bullforge/services/dashboard_service/static/config.html`
- `bullforge/services/dashboard_service/static/freshness.html`
- `bullforge/tests/test_symbols_api.py`
- `bullforge/tests/test_config_api.py`
- `bullforge/tests/test_freshness_api.py`
- `bullforge/tests/test_symbol_hot_reload.py`

**Modify:**
- `bullforge/shared/runtime_config.py` — add `trading_symbols_equity`, `trading_symbols_crypto` (json), `last_retrain_time` (str)
- `bullforge/shared/db.py` — add `query_latest_bar_timestamps`, `query_latest_feature_timestamps`, `query_model_details`, `query_symbol_bar_stats`
- `bullforge/services/dashboard_service/api.py` — upgrade `/api/models` to use `query_model_details` with human label
- `bullforge/services/dashboard_service/main.py` — register new routes, pass `RuntimeConfig`
- `bullforge/services/data_pipeline/main.py` — subscribe to `config:symbols_changed`, use RuntimeConfig for initial symbol list
- `bullforge/services/data_pipeline/stream.py` — allow dynamic subscribe/unsubscribe
- `bullforge/services/data_pipeline/backfill.py` — expose `backfill_symbol(symbol, timeframe)` as an async operation

---

## Task 1: Extend CONFIG_SCHEMA with symbols + retrain-timestamp

**Files:**
- Modify: `bullforge/shared/runtime_config.py`
- Modify: `bullforge/tests/test_runtime_config.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_runtime_config.py`:

```python
@pytest.mark.asyncio
async def test_symbols_stored_as_json(fake_redis):
    fake_redis.client.hget = AsyncMock(return_value='["SPY", "QQQ"]')
    cfg = RuntimeConfig(fake_redis)
    val = await cfg.get("trading_symbols_equity")
    assert val == ["SPY", "QQQ"]


@pytest.mark.asyncio
async def test_set_symbols_serializes_json(fake_redis):
    cfg = RuntimeConfig(fake_redis)
    await cfg.set("trading_symbols_equity", ["SPY", "AAPL", "MSFT"])
    args, _ = fake_redis.client.hset.call_args
    assert args[1] == "trading_symbols_equity"
    assert args[2] == '["SPY", "AAPL", "MSFT"]'


def test_schema_includes_symbols_and_last_retrain():
    from shared.runtime_config import CONFIG_SCHEMA
    assert CONFIG_SCHEMA["trading_symbols_equity"]["type"] == "json"
    assert CONFIG_SCHEMA["trading_symbols_crypto"]["type"] == "json"
    assert CONFIG_SCHEMA["last_retrain_time"]["type"] == "str"
```

- [ ] **Step 2: Run, fail**

Run: `cd bullforge && pytest tests/test_runtime_config.py -v -k "symbols or last_retrain"`
Expected: FAIL

- [ ] **Step 3: Extend `CONFIG_SCHEMA`**

Append entries in `bullforge/shared/runtime_config.py`:

```python
    "trading_symbols_equity": {
        "default": ["SPY", "QQQ", "AAPL", "MSFT", "NVDA"],
        "type": "json", "label": "Equity Symbols",
        "description": "Equity/ETF tickers to trade. Adding a symbol triggers historical backfill and live stream subscription. Removing stops streaming (historical data retained).",
        "reference": "https://en.wikipedia.org/wiki/Ticker_symbol",
    },
    "trading_symbols_crypto": {
        "default": ["BTC/USD", "ETH/USD"],
        "type": "json", "label": "Crypto Pairs",
        "description": "Crypto trading pairs (base/quote format). Trades 24/7.",
        "reference": "https://en.wikipedia.org/wiki/Cryptocurrency",
    },
    "last_retrain_time": {
        "default": "",
        "type": "str", "label": "Last Retrain (UTC ISO)",
        "description": "Timestamp of last successful retrain cycle. Written by the scheduler.",
        "reference": "",
    },
```

- [ ] **Step 4: Run, PASS**

Run: `cd bullforge && pytest tests/test_runtime_config.py -v`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/runtime_config.py bullforge/tests/test_runtime_config.py
git commit -m "feat: add trading_symbols_{equity,crypto} and last_retrain_time to CONFIG_SCHEMA"
```

---

## Task 2: DB queries for freshness + model details

**Files:**
- Modify: `bullforge/shared/db.py`
- Modify: `bullforge/tests/test_db.py` (or create a new test file)

- [ ] **Step 1: Write failing tests (integration, real test DB)**

```python
# Append to bullforge/tests/test_db.py (or create test_db_phase6b.py)
import pytest
from datetime import datetime, timedelta, timezone


@pytest.mark.asyncio
async def test_query_latest_bar_timestamps(real_test_db):
    db = real_test_db
    base = datetime(2026, 4, 13, 14, 0, tzinfo=timezone.utc)
    for symbol, offset in [("SPY", 0), ("QQQ", 2), ("AAPL", 5)]:
        await db.pool.execute(
            "INSERT INTO bars_1m (symbol, timestamp, open, high, low, close, volume) "
            "VALUES ($1, $2, 100, 100, 100, 100, 1)",
            symbol, base + timedelta(minutes=offset),
        )
    result = await db.query_latest_bar_timestamps()
    d = {r["symbol"]: r["last_bar"] for r in result}
    assert d["SPY"] == base
    assert d["AAPL"] == base + timedelta(minutes=5)


@pytest.mark.asyncio
async def test_query_model_details_joins_status(real_test_db):
    db = real_test_db
    # Insert a model and status
    from shared.models import ModelMetadata
    meta = ModelMetadata(
        symbol="SPY", timeframe="1m", horizon_minutes=5,
        model_type="gradient_boost", artifact_path="/tmp/x",
        metrics={"accuracy": 0.6}, feature_columns=["a"],
        trained_at=datetime.now(timezone.utc),
        train_samples=100, test_samples=20, is_active=True,
    )
    model_id = await db.insert_model_metadata(meta)
    await db.pool.execute(
        "INSERT INTO model_status (model_id, status, reason) VALUES ($1, 'active', 'ok')",
        model_id,
    )

    details = await db.query_model_details()
    row = next(r for r in details if r["model_id"] == model_id)
    assert row["symbol"] == "SPY"
    assert row["model_type"] == "gradient_boost"
    assert row["horizon_minutes"] == 5
    assert row["status"] == "active"
    assert row["metrics"]["accuracy"] == 0.6
```

- [ ] **Step 2: Implement DB methods in `shared/db.py`**

```python
async def query_latest_bar_timestamps(self) -> list[dict]:
    async with self.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT symbol, MAX(timestamp) AS last_bar "
            "FROM bars_1m GROUP BY symbol ORDER BY symbol"
        )
    return [dict(r) for r in rows]


async def query_latest_feature_timestamps(self) -> list[dict]:
    async with self.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT symbol, MAX(timestamp) AS last_feature "
            "FROM features GROUP BY symbol ORDER BY symbol"
        )
    return [dict(r) for r in rows]


async def query_symbol_bar_stats(self, symbol: str) -> dict:
    async with self.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS bar_count, MAX(timestamp) AS last_bar "
            "FROM bars_1m WHERE symbol = $1",
            symbol,
        )
    return {"symbol": symbol, "bar_count": row["bar_count"], "last_bar": row["last_bar"]}


async def query_model_details(self) -> list[dict]:
    async with self.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT mr.id AS model_id, mr.symbol, mr.timeframe, mr.horizon_minutes,
                   mr.model_type, mr.trained_at, mr.metrics, mr.is_active,
                   COALESCE(ms.status, 'active') AS status,
                   COALESCE(ms.reason, '') AS reason,
                   ms.updated_at AS status_updated_at
            FROM model_registry mr
            LEFT JOIN model_status ms ON mr.id = ms.model_id
            WHERE mr.is_active = TRUE
            ORDER BY mr.symbol, mr.timeframe, mr.horizon_minutes, mr.model_type
            """
        )
    result = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("metrics"), str):
            d["metrics"] = json.loads(d["metrics"])
        result.append(d)
    return result
```

- [ ] **Step 3: Run tests, PASS**

Run: `cd bullforge && pytest tests/test_db_phase6b.py -v` (or wherever the new tests live)
Expected: pass

- [ ] **Step 4: Commit**

```bash
git add bullforge/shared/db.py bullforge/tests/test_db_phase6b.py
git commit -m "feat: db methods for freshness and model details (phase 6B)"
```

---

## Task 3: Human-labeled models in `/api/models`

**Files:**
- Modify: `bullforge/services/dashboard_service/api.py`
- Test: existing dashboard API test file

- [ ] **Step 1: Update `_handle_models` handler**

Edit `bullforge/services/dashboard_service/api.py`:

```python
async def _handle_models(request: web.Request) -> web.Response:
    db: Database = request.app["db"]
    details = await db.query_model_details()
    payload = []
    for d in details:
        label = f"{d['model_type']} · {d['symbol']} · {d['timeframe']} · {d['horizon_minutes']}m"
        payload.append({
            "model_id": d["model_id"],
            "label": label,
            "symbol": d["symbol"],
            "timeframe": d["timeframe"],
            "horizon_minutes": d["horizon_minutes"],
            "model_type": d["model_type"],
            "status": d["status"],
            "reason": d["reason"],
            "metrics": d["metrics"],
            "trained_at": d["trained_at"].isoformat() if d["trained_at"] else None,
            "status_updated_at": d["status_updated_at"].isoformat() if d["status_updated_at"] else None,
        })
    return web.json_response(payload)
```

- [ ] **Step 2: Write integration test**

```python
# bullforge/tests/test_dashboard_models_label.py
import pytest
from aiohttp.test_utils import TestClient, TestServer
from services.dashboard_service.api import create_api_app


@pytest.mark.asyncio
async def test_models_endpoint_returns_human_label(real_test_db, tmp_path):
    # Seed DB with a model/status like in Task 2 test
    # ... (reuse fixture logic)
    app = create_api_app(real_test_db)
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await client.get("/api/models")
            assert resp.status == 200
            data = await resp.json()
            assert len(data) >= 1
            m = data[0]
            assert "label" in m
            assert "·" in m["label"]  # human separator present
            assert m["model_type"] in m["label"]
            assert m["symbol"] in m["label"]
```

- [ ] **Step 3: Run, PASS**

- [ ] **Step 4: Update frontend to use `label`**

Grep for `model_id` rendering in dashboard static files:

```bash
cd bullforge && grep -rn "model_id" services/dashboard_service/static/
```

Replace occurrences that display the bare integer with `label`. For example, in the model table row template, show `{model.label}` instead of `#{model.model_id}`.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/dashboard_service/api.py bullforge/services/dashboard_service/static/ bullforge/tests/test_dashboard_models_label.py
git commit -m "feat: /api/models returns human labels (model_type · symbol · tf · horizon)"
```

---

## Task 4: `/api/config` endpoint for config editor

**Files:**
- Create: `bullforge/services/dashboard_service/config_api.py`
- Modify: `bullforge/services/dashboard_service/main.py`
- Test: `bullforge/tests/test_config_api.py`

- [ ] **Step 1: Write failing test**

```python
# bullforge/tests/test_config_api.py
import pytest
from unittest.mock import AsyncMock
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from services.dashboard_service.config_api import register_config_routes
from shared.runtime_config import CONFIG_SCHEMA


@pytest.fixture
def app():
    app = web.Application()
    rc = AsyncMock()
    rc.get_all = AsyncMock(return_value={
        k: v["default"] for k, v in CONFIG_SCHEMA.items()
    })
    rc.get_schema = lambda: CONFIG_SCHEMA
    rc.set = AsyncMock()
    app["runtime_config"] = rc
    register_config_routes(app)
    return app


@pytest.mark.asyncio
async def test_get_config_returns_values_and_schema(app):
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await client.get("/api/config")
            assert resp.status == 200
            body = await resp.json()
            assert "values" in body
            assert "schema" in body
            assert body["values"]["retrain_schedule_hour"] == 1
            assert "label" in body["schema"]["retrain_schedule_hour"]


@pytest.mark.asyncio
async def test_put_config_updates_single_key(app):
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await client.put("/api/config/retrain_schedule_hour",
                                    json={"value": 3})
            assert resp.status == 200
            app["runtime_config"].set.assert_awaited_once_with("retrain_schedule_hour", 3)


@pytest.mark.asyncio
async def test_put_config_rejects_out_of_range(app):
    app["runtime_config"].set = AsyncMock(
        side_effect=ValueError("must be between 0 and 23")
    )
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await client.put("/api/config/retrain_schedule_hour",
                                    json={"value": 99})
            assert resp.status == 400
            body = await resp.json()
            assert "must be between" in body["error"]
```

- [ ] **Step 2: Implement handlers**

```python
# bullforge/services/dashboard_service/config_api.py
from __future__ import annotations
from aiohttp import web

from shared.runtime_config import RuntimeConfig


def register_config_routes(app: web.Application) -> None:
    app.router.add_get("/api/config", _get_config)
    app.router.add_put("/api/config/{key}", _put_config)


async def _get_config(request: web.Request) -> web.Response:
    rc: RuntimeConfig = request.app["runtime_config"]
    values = await rc.get_all()
    schema = rc.get_schema()
    return web.json_response({"values": values, "schema": schema})


async def _put_config(request: web.Request) -> web.Response:
    rc: RuntimeConfig = request.app["runtime_config"]
    key = request.match_info["key"]
    body = await request.json()
    value = body.get("value")
    try:
        await rc.set(key, value)
    except KeyError:
        return web.json_response({"error": f"unknown key {key}"}, status=404)
    except ValueError as e:
        return web.json_response({"error": str(e)}, status=400)
    return web.json_response({"ok": True, "key": key, "value": value})
```

- [ ] **Step 3: Register in `main.py`**

In `bullforge/services/dashboard_service/main.py`, wire `RuntimeConfig`:

```python
from shared.runtime_config import RuntimeConfig
from services.dashboard_service.config_api import register_config_routes

# after redis_client.connect():
runtime_config = RuntimeConfig(redis_client)
app["runtime_config"] = runtime_config
register_config_routes(app)
```

- [ ] **Step 4: Run, PASS**

Run: `cd bullforge && pytest tests/test_config_api.py -v`

- [ ] **Step 5: Build config editor HTML**

```html
<!-- bullforge/services/dashboard_service/static/config.html -->
<!-- Loaded into dashboard tab. Uses fetch/fetch-put, no framework. -->
<div id="config-editor">
  <h2>Runtime Configuration</h2>
  <div id="config-list">Loading…</div>
</div>
<script>
(async function () {
  const resp = await fetch("/api/config");
  const { values, schema } = await resp.json();
  const container = document.getElementById("config-list");
  container.innerHTML = "";
  for (const [key, meta] of Object.entries(schema)) {
    if (key === "last_retrain_time") continue; // read-only
    const row = document.createElement("div");
    row.className = "config-row";
    row.innerHTML = `
      <label>${meta.label}</label>
      <input type="${meta.type === 'float' ? 'number' : (meta.type === 'int' ? 'number' : 'text')}"
             step="${meta.type === 'float' ? 'any' : '1'}"
             min="${meta.min ?? ''}" max="${meta.max ?? ''}"
             id="cfg-${key}" value="${values[key]}" />
      <button data-key="${key}">Save</button>
      <p class="description">${meta.description}
        ${meta.reference ? `<a href="${meta.reference}" target="_blank">Learn more →</a>` : ""}
      </p>
      <div class="save-status" id="status-${key}"></div>
    `;
    container.appendChild(row);
  }
  container.addEventListener("click", async (e) => {
    if (e.target.tagName !== "BUTTON") return;
    const key = e.target.dataset.key;
    const input = document.getElementById(`cfg-${key}`);
    const status = document.getElementById(`status-${key}`);
    let value = input.value;
    if (schema[key].type === "int") value = parseInt(value, 10);
    else if (schema[key].type === "float") value = parseFloat(value);
    else if (schema[key].type === "json") value = JSON.parse(value);
    const r = await fetch(`/api/config/${key}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value }),
    });
    const body = await r.json();
    status.textContent = r.ok ? "Saved ✓" : `Error: ${body.error}`;
    status.style.color = r.ok ? "green" : "red";
    setTimeout(() => { status.textContent = ""; }, 3000);
  });
})();
</script>
```

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/dashboard_service/config_api.py bullforge/services/dashboard_service/main.py bullforge/services/dashboard_service/static/config.html bullforge/tests/test_config_api.py
git commit -m "feat: /api/config GET and PUT with editor UI"
```

---

## Task 5: `/api/freshness` endpoint

**Files:**
- Create: `bullforge/services/dashboard_service/freshness_api.py`
- Test: `bullforge/tests/test_freshness_api.py`

- [ ] **Step 1: Write failing test**

```python
# bullforge/tests/test_freshness_api.py
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from services.dashboard_service.freshness_api import register_freshness_routes


@pytest.mark.asyncio
async def test_freshness_endpoint():
    now = datetime.now(timezone.utc)
    db = AsyncMock()
    db.query_latest_bar_timestamps = AsyncMock(return_value=[
        {"symbol": "SPY", "last_bar": now - timedelta(minutes=2)},
        {"symbol": "BTC/USD", "last_bar": now - timedelta(seconds=30)},
    ])
    db.query_latest_feature_timestamps = AsyncMock(return_value=[
        {"symbol": "SPY", "last_feature": now - timedelta(minutes=3)},
    ])
    db.query_model_details = AsyncMock(return_value=[
        {"model_id": 1, "symbol": "SPY", "model_type": "gradient_boost",
         "timeframe": "1m", "horizon_minutes": 5,
         "trained_at": now - timedelta(hours=12), "status": "active",
         "metrics": {"accuracy": 0.6, "f1_macro": 0.58}, "reason": ""},
    ])

    app = web.Application()
    app["db"] = db
    register_freshness_routes(app)

    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await client.get("/api/freshness")
            assert resp.status == 200
            body = await resp.json()
            assert "bars" in body and "features" in body and "models" in body
            spy_bar = next(b for b in body["bars"] if b["symbol"] == "SPY")
            assert spy_bar["color"] == "green"  # <5m for equity
            btc_bar = next(b for b in body["bars"] if b["symbol"] == "BTC/USD")
            assert btc_bar["color"] == "green"  # <2m for crypto
```

- [ ] **Step 2: Implement**

```python
# bullforge/services/dashboard_service/freshness_api.py
from __future__ import annotations

from datetime import datetime, timezone
from aiohttp import web

from shared.symbol_utils import is_crypto


def _age_seconds(ts: datetime) -> float:
    return (datetime.now(timezone.utc) - ts).total_seconds()


def _bar_color(symbol: str, age_sec: float) -> str:
    if is_crypto(symbol):
        if age_sec < 120:
            return "green"
        if age_sec < 300:
            return "yellow"
        return "red"
    # equity
    if age_sec < 300:
        return "green"
    if age_sec < 900:
        return "yellow"
    return "red"


def _model_age_color(age_hours: float) -> str:
    if age_hours < 24:
        return "green"
    if age_hours < 48:
        return "yellow"
    return "red"


def register_freshness_routes(app: web.Application) -> None:
    app.router.add_get("/api/freshness", _get_freshness)


async def _get_freshness(request: web.Request) -> web.Response:
    db = request.app["db"]
    bar_rows = await db.query_latest_bar_timestamps()
    feat_rows = await db.query_latest_feature_timestamps()
    models = await db.query_model_details()

    bars = []
    for r in bar_rows:
        age = _age_seconds(r["last_bar"])
        bars.append({
            "symbol": r["symbol"],
            "last_bar": r["last_bar"].isoformat(),
            "age_seconds": age,
            "color": _bar_color(r["symbol"], age),
        })
    features = []
    for r in feat_rows:
        age = _age_seconds(r["last_feature"])
        features.append({
            "symbol": r["symbol"],
            "last_feature": r["last_feature"].isoformat(),
            "age_seconds": age,
            "color": _bar_color(r["symbol"], age),
        })
    model_rows = []
    for m in models:
        age_h = _age_seconds(m["trained_at"]) / 3600
        model_rows.append({
            "model_id": m["model_id"],
            "label": f"{m['model_type']} · {m['symbol']} · {m['timeframe']} · {m['horizon_minutes']}m",
            "trained_at": m["trained_at"].isoformat(),
            "age_hours": age_h,
            "age_color": _model_age_color(age_h),
            "status": m["status"],
            "accuracy": m["metrics"].get("accuracy"),
            "f1_macro": m["metrics"].get("f1_macro"),
        })

    return web.json_response({
        "bars": bars, "features": features, "models": model_rows,
    })
```

- [ ] **Step 3: Run, PASS; register in `main.py`; build a simple freshness HTML tab**

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/dashboard_service/freshness_api.py bullforge/services/dashboard_service/main.py bullforge/services/dashboard_service/static/freshness.html bullforge/tests/test_freshness_api.py
git commit -m "feat: /api/freshness endpoint + freshness dashboard tab"
```

---

## Task 6: `/api/symbols` CRUD + Redis event on change

**Files:**
- Create: `bullforge/services/dashboard_service/symbols_api.py`
- Test: `bullforge/tests/test_symbols_api.py`

- [ ] **Step 1: Write failing tests**

```python
# bullforge/tests/test_symbols_api.py
import pytest
from unittest.mock import AsyncMock
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from services.dashboard_service.symbols_api import register_symbol_routes


@pytest.fixture
def app():
    app = web.Application()
    rc = AsyncMock()
    rc.get = AsyncMock(side_effect=lambda k: {
        "trading_symbols_equity": ["SPY", "QQQ"],
        "trading_symbols_crypto": ["BTC/USD"],
    }[k])
    rc.set = AsyncMock()
    app["runtime_config"] = rc

    redis = AsyncMock()
    redis.publish = AsyncMock()
    app["redis_client"] = redis

    db = AsyncMock()
    db.query_symbol_bar_stats = AsyncMock(return_value={
        "symbol": "AAPL", "bar_count": 0, "last_bar": None,
    })
    app["db"] = db

    register_symbol_routes(app)
    return app


@pytest.mark.asyncio
async def test_get_symbols_lists_both_groups(app):
    async with TestServer(app) as server, TestClient(server) as client:
        r = await client.get("/api/symbols")
        body = await r.json()
        assert body == {"equity": ["SPY", "QQQ"], "crypto": ["BTC/USD"]}


@pytest.mark.asyncio
async def test_add_equity_publishes_event_and_updates_rc(app):
    async with TestServer(app) as server, TestClient(server) as client:
        r = await client.post("/api/symbols", json={"symbol": "AAPL", "kind": "equity"})
        assert r.status == 200
        app["runtime_config"].set.assert_awaited()
        (key, val), _ = app["runtime_config"].set.call_args
        assert key == "trading_symbols_equity"
        assert "AAPL" in val
        app["redis_client"].publish.assert_awaited()


@pytest.mark.asyncio
async def test_add_crypto_requires_slash(app):
    async with TestServer(app) as server, TestClient(server) as client:
        r = await client.post("/api/symbols", json={"symbol": "DOGE", "kind": "crypto"})
        assert r.status == 400


@pytest.mark.asyncio
async def test_delete_symbol(app):
    async with TestServer(app) as server, TestClient(server) as client:
        r = await client.delete("/api/symbols/SPY?kind=equity")
        assert r.status == 200
        (key, val), _ = app["runtime_config"].set.call_args
        assert key == "trading_symbols_equity"
        assert "SPY" not in val


@pytest.mark.asyncio
async def test_symbol_status(app):
    async with TestServer(app) as server, TestClient(server) as client:
        r = await client.get("/api/symbols/AAPL/status")
        body = await r.json()
        assert body["status"] == "pending"  # bar_count == 0
```

- [ ] **Step 2: Implement**

```python
# bullforge/services/dashboard_service/symbols_api.py
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from aiohttp import web

from shared.symbol_utils import is_crypto


def register_symbol_routes(app: web.Application) -> None:
    app.router.add_get("/api/symbols", _get_symbols)
    app.router.add_post("/api/symbols", _add_symbol)
    app.router.add_delete("/api/symbols/{symbol}", _remove_symbol)
    app.router.add_get("/api/symbols/{symbol}/status", _symbol_status)


async def _get_symbols(request: web.Request) -> web.Response:
    rc = request.app["runtime_config"]
    return web.json_response({
        "equity": await rc.get("trading_symbols_equity"),
        "crypto": await rc.get("trading_symbols_crypto"),
    })


async def _add_symbol(request: web.Request) -> web.Response:
    body = await request.json()
    symbol = body["symbol"].strip().upper()
    kind = body["kind"]
    if kind not in ("equity", "crypto"):
        return web.json_response({"error": "kind must be equity or crypto"}, status=400)
    if kind == "crypto" and "/" not in symbol:
        return web.json_response(
            {"error": "crypto symbols must contain a slash, e.g. BTC/USD"}, status=400,
        )
    if kind == "equity" and is_crypto(symbol):
        return web.json_response(
            {"error": "equity symbols must not contain a slash"}, status=400,
        )

    rc = request.app["runtime_config"]
    redis = request.app["redis_client"]
    key = f"trading_symbols_{kind}"
    current = await rc.get(key)
    if symbol in current:
        return web.json_response({"ok": True, "added": False, "reason": "already present"})
    current = current + [symbol]
    await rc.set(key, current)
    await redis.publish(
        "config:symbols_changed",
        json.dumps({"action": "add", "kind": kind, "symbol": symbol}),
    )
    return web.json_response({"ok": True, "added": True, "symbol": symbol})


async def _remove_symbol(request: web.Request) -> web.Response:
    symbol = request.match_info["symbol"]
    kind = request.query.get("kind")
    if kind not in ("equity", "crypto"):
        return web.json_response({"error": "kind query param required"}, status=400)
    rc = request.app["runtime_config"]
    redis = request.app["redis_client"]
    key = f"trading_symbols_{kind}"
    current = await rc.get(key)
    if symbol not in current:
        return web.json_response({"ok": True, "removed": False})
    current = [s for s in current if s != symbol]
    await rc.set(key, current)
    await redis.publish(
        "config:symbols_changed",
        json.dumps({"action": "remove", "kind": kind, "symbol": symbol}),
    )
    return web.json_response({"ok": True, "removed": True})


async def _symbol_status(request: web.Request) -> web.Response:
    symbol = request.match_info["symbol"]
    db = request.app["db"]
    stats = await db.query_symbol_bar_stats(symbol)
    bar_count = stats["bar_count"]
    last_bar = stats["last_bar"]
    if bar_count == 0:
        status = "pending"
    elif last_bar is None:
        status = "pending"
    else:
        age = datetime.now(timezone.utc) - last_bar
        status = "complete" if age < timedelta(minutes=10) else "backfilling"
    return web.json_response({
        "symbol": symbol, "status": status,
        "bar_count": bar_count,
        "last_bar": last_bar.isoformat() if last_bar else None,
    })
```

- [ ] **Step 3: Register in `main.py`, build `symbols.html` tab, run tests, commit**

Tab HTML mirrors Task 4's pattern: list with remove buttons, input + add button per group, polls `/api/symbols/{sym}/status` after add.

```bash
git add bullforge/services/dashboard_service/symbols_api.py bullforge/services/dashboard_service/main.py bullforge/services/dashboard_service/static/symbols.html bullforge/tests/test_symbols_api.py
git commit -m "feat: /api/symbols CRUD with config:symbols_changed event"
```

---

## Task 7: Data pipeline hot-reloads on `config:symbols_changed`

**Files:**
- Modify: `bullforge/services/data_pipeline/main.py`
- Modify: `bullforge/services/data_pipeline/stream.py` (if needed to support dynamic sub/unsub)
- Test: `bullforge/tests/test_symbol_hot_reload.py`

- [ ] **Step 1: Write test**

```python
# bullforge/tests/test_symbol_hot_reload.py
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock
from services.data_pipeline.main import handle_symbol_change_event


@pytest.mark.asyncio
async def test_add_equity_triggers_backfill_and_subscribe():
    stream = AsyncMock()
    backfill = AsyncMock()
    event = json.dumps({"action": "add", "kind": "equity", "symbol": "AAPL"})
    await handle_symbol_change_event(event, stream=stream, backfill=backfill)
    backfill.backfill_symbol.assert_awaited_once_with("AAPL", kind="equity")
    stream.subscribe_symbol.assert_awaited_once_with("AAPL", kind="equity")


@pytest.mark.asyncio
async def test_remove_unsubscribes_only():
    stream = AsyncMock()
    backfill = AsyncMock()
    event = json.dumps({"action": "remove", "kind": "crypto", "symbol": "BTC/USD"})
    await handle_symbol_change_event(event, stream=stream, backfill=backfill)
    stream.unsubscribe_symbol.assert_awaited_once_with("BTC/USD", kind="crypto")
    backfill.backfill_symbol.assert_not_called()
```

- [ ] **Step 2: Implement handler in data_pipeline/main.py**

```python
async def handle_symbol_change_event(message: str, *, stream, backfill) -> None:
    data = json.loads(message)
    action, kind, symbol = data["action"], data["kind"], data["symbol"]
    if action == "add":
        # Backfill first so feature engine has history, then subscribe to live
        await backfill.backfill_symbol(symbol, kind=kind)
        await stream.subscribe_symbol(symbol, kind=kind)
    elif action == "remove":
        await stream.unsubscribe_symbol(symbol, kind=kind)
    # We intentionally do not delete historical data on remove.


# In main() setup:
async def on_symbols_changed(msg: str) -> None:
    await handle_symbol_change_event(msg, stream=stream, backfill=backfill)

await redis_client.subscribe("config:symbols_changed", on_symbols_changed)
```

- [ ] **Step 3: Ensure `stream.subscribe_symbol` / `unsubscribe_symbol` / `backfill.backfill_symbol` exist**

Read current `stream.py` and `backfill.py`. If they start with a static symbol list at startup, add the dynamic methods:

```python
# stream.py
async def subscribe_symbol(self, symbol: str, *, kind: str) -> None:
    """Add symbol to live Alpaca stream subscription."""
    self._active.add(symbol)
    # alpaca-py SDK: subscribe via websocket client
    if kind == "equity":
        await self._stock_client.subscribe_bars(symbol)
    else:
        await self._crypto_client.subscribe_bars(symbol)

async def unsubscribe_symbol(self, symbol: str, *, kind: str) -> None:
    self._active.discard(symbol)
    if kind == "equity":
        await self._stock_client.unsubscribe_bars(symbol)
    else:
        await self._crypto_client.unsubscribe_bars(symbol)
```

```python
# backfill.py
async def backfill_symbol(self, symbol: str, *, kind: str, lookback_days: int = 30) -> int:
    """Backfill historical bars for a newly added symbol. Returns bars inserted."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=lookback_days)
    bars = await self._alpaca_client.fetch_bars(symbol, "1Min", start, end)
    inserted = await self._db.insert_bars(bars)
    return inserted
```

(Adapt to existing stream/backfill class APIs.)

- [ ] **Step 4: Swap `Settings.trading_symbols_*` → `RuntimeConfig` in data_pipeline `main()`**

Replace initial symbol source at startup:

```python
runtime_config = RuntimeConfig(redis_client)
equity = await runtime_config.get("trading_symbols_equity")
crypto = await runtime_config.get("trading_symbols_crypto")
all_symbols = equity + crypto
```

- [ ] **Step 5: Run tests, verify other services still work**

Run: `cd bullforge && pytest tests/ -v`
Expected: pass

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/data_pipeline/main.py bullforge/services/data_pipeline/stream.py bullforge/services/data_pipeline/backfill.py bullforge/tests/test_symbol_hot_reload.py
git commit -m "feat: data pipeline hot-reloads on config:symbols_changed"
```

---

## Task 8: Thread RuntimeConfig-sourced symbols into model_service and execution_service

**Files:**
- Modify: `bullforge/services/model_service/main.py` (symbol list from RuntimeConfig on each retrain cycle — already covered in 6A scheduler `_symbols` arg)
- Modify: `bullforge/services/execution_service/main.py` (read symbols from RuntimeConfig on startup; optionally subscribe to `config:symbols_changed` for in-session awareness)

- [ ] **Step 1: Update model_service main to pass RuntimeConfig-based symbols to scheduler**

Because the scheduler is constructed with a static list in 6A, either:

(a) Construct the scheduler with a callable that reads fresh symbols each cycle, or
(b) Simpler: have the scheduler re-read from `runtime_config` inside `run_once()`.

Pick (b). Edit `scheduler.py` `run_once`:

```python
config = await self._rc.get_all()
self._symbols = config["trading_symbols_equity"] + config["trading_symbols_crypto"]
```

Remove `symbols` from the `__init__` arguments (or leave as an override for tests). Update tests accordingly.

- [ ] **Step 2: Update execution_service to source symbols from RuntimeConfig**

Minimal change at startup — since execution reacts to Redis events per-symbol, the static list is less critical. Read the union and filter if needed.

- [ ] **Step 3: Run full test suite**

Run: `cd bullforge && pytest tests/ -v`

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/model_service/scheduler.py bullforge/services/execution_service/main.py bullforge/tests/test_scheduler.py
git commit -m "feat: services read symbols from RuntimeConfig"
```

---

## Final Self-Review Checklist

- [ ] `trading_symbols_equity` / `trading_symbols_crypto` stored as JSON in Redis
- [ ] Dashboard can GET/POST/DELETE symbols with validation (crypto requires `/`)
- [ ] Adding a symbol triggers backfill + live subscribe (via `config:symbols_changed`)
- [ ] Removing a symbol stops live stream, retains historical data
- [ ] `/api/symbols/{sym}/status` reports pending/backfilling/complete from DB bar count
- [ ] `/api/models` returns human labels (model_type · symbol · tf · horizon)
- [ ] Freshness endpoint returns colored statuses per equity/crypto rules
- [ ] Config editor tab renders CONFIG_SCHEMA with descriptions + reference links
- [ ] Retrain scheduler re-reads symbols from RuntimeConfig each cycle
