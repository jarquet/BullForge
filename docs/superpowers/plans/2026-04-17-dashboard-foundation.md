# Dashboard Expansion Plan 1 — Shared Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the reusable dashboard components (router, time-window picker, refresh control, chart wrappers, leaderboard primitive, drill-down frame), add `?from/?to` query-param support to existing metrics endpoints, and stub every new Plan 2-4 endpoint as HTTP 501 — without changing any user-visible behavior.

**Architecture:** Six small vanilla-JS modules under `bullforge/services/dashboard_service/static/components/`, each with a standalone in-browser test harness extending the existing `test/fmttime.html` pattern. Backend adds a shared `_time_window` helper used by all current metrics endpoints, and a single new `stubs_api.py` file that registers placeholder routes returning HTTP 501 for every Plan 2-4 endpoint. No framework, no bundler, no backend refactor — existing dashboard keeps working exactly as it does today.

**Tech Stack:** Vanilla JS (ES2015-compatible to match existing `dashboard.js` style), Chart.js 4.4 (already loaded via CDN), aiohttp, asyncpg, pytest + pytest-asyncio.

**Spec:** [docs/superpowers/specs/2026-04-17-dashboard-expansion-design.md](../specs/2026-04-17-dashboard-expansion-design.md)

**Depends on (for later plans, not this one):** [2026-04-17-backtest-run-scoping-design.md](../specs/2026-04-17-backtest-run-scoping-design.md). Plan 1 only adds HTTP 501 stubs, so it does not touch the tables scoping re-keys.

---

## Pre-Task: Worktree

The execution skill (`superpowers:subagent-driven-development` or `superpowers:executing-plans`) must create `.worktrees/dashboard-foundation` branched from master. Suggested branch: `feat/dashboard-foundation`. All subsequent task paths are relative to that worktree.

**Dev-iteration setup.** The dashboard service ships static assets baked into the Docker image (`COPY services/dashboard_service/` in the Dockerfile). To iterate on the new components without rebuilding after every edit, create a gitignored `bullforge/docker-compose.override.yml` with:

```yaml
services:
  dashboard_service:
    volumes:
      - ./services/dashboard_service/static:/app/services/dashboard_service/static
```

Then `cd bullforge && docker compose up -d dashboard_service`. Browser hard-reload (Ctrl+Shift+R) picks up edits. **Remove the override file before the final commit.** Tasks below reference "reload the dashboard" assuming this is set up.

---

## File Structure

### New files

```
bullforge/services/dashboard_service/static/
  components/
    router.js                    # URL hash → {tab, subtab, entity_kind, entity_id, query}, emits routeChanged
    timewindow.js                # Preset dropdown + custom-range inputs, emits windowChanged
    refresh.js                   # Interval selector + pause/resume, emits refresh
    charts.js                    # lineChart, barChart, histogramChart, scatterChart, sparkline, heatmapChart
    leaderboard.js               # Sortable table primitive: {columns, rows, getRowHref}
    drilldown.js                 # Back-button + title + breadcrumb frame for entity detail routes
  test/
    router.html                  # Standalone test harness for router.js
    timewindow.html              # Standalone test harness for timewindow.js
    refresh.html                 # Standalone test harness for refresh.js
    charts.html                  # Standalone test harness for charts.js (visual + assertions)
    leaderboard.html             # Standalone test harness for leaderboard.js
    drilldown.html               # Standalone test harness for drilldown.js

bullforge/services/dashboard_service/
  stubs_api.py                   # Registers HTTP 501 handlers for Plan 2-4 endpoints

bullforge/shared/
  time_window.py                 # parse_time_window(request) → (from_dt, to_dt, sql_clause, params)

bullforge/tests/
  test_time_window.py            # Unit tests for parse_time_window
  test_dashboard_stubs.py        # Tests that every Plan 2-4 endpoint returns 501 with {error, enabled_after_plan}
```

### Modified files

```
bullforge/services/dashboard_service/api.py                # accept ?from/?to on existing metrics endpoints
bullforge/services/dashboard_service/main.py               # register_stub_routes(app)
bullforge/services/dashboard_service/static/index.html     # load components before dashboard.js
bullforge/shared/db.py                                     # query_metric_snapshots and friends accept from/to
```

**Not modified by Plan 1:** `dashboard.js`, `backtests.js`, `backtests.css`, `style.css`, existing tabs, existing DB methods that already work. Plan 1 is additive.

---

## Task 1: Shared backend time-window helper

**Files:**
- Create: `bullforge/shared/time_window.py`
- Create: `bullforge/tests/test_time_window.py`

- [ ] **Step 1: Write the failing test**

Create `bullforge/tests/test_time_window.py`:

```python
from datetime import datetime, timezone

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request

from shared.time_window import parse_time_window


def _req(query: str = ""):
    return make_mocked_request("GET", "/test" + (("?" + query) if query else ""))


def test_no_params_returns_none_tuple():
    from_dt, to_dt = parse_time_window(_req())
    assert from_dt is None
    assert to_dt is None


def test_parses_iso_with_z():
    from_dt, to_dt = parse_time_window(
        _req("from=2026-04-01T00:00:00Z&to=2026-04-17T00:00:00Z"),
    )
    assert from_dt == datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert to_dt == datetime(2026, 4, 17, 0, 0, 0, tzinfo=timezone.utc)


def test_parses_iso_with_offset():
    from_dt, _ = parse_time_window(_req("from=2026-04-01T00:00:00+00:00"))
    assert from_dt == datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_naive_iso_is_treated_as_utc():
    from_dt, _ = parse_time_window(_req("from=2026-04-01T00:00:00"))
    assert from_dt == datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_rejects_malformed_from():
    with pytest.raises(web.HTTPBadRequest):
        parse_time_window(_req("from=not-a-date"))


def test_rejects_malformed_to():
    with pytest.raises(web.HTTPBadRequest):
        parse_time_window(_req("to=not-a-date"))


def test_only_from_is_valid():
    from_dt, to_dt = parse_time_window(_req("from=2026-04-01T00:00:00Z"))
    assert from_dt == datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert to_dt is None


def test_only_to_is_valid():
    from_dt, to_dt = parse_time_window(_req("to=2026-04-17T00:00:00Z"))
    assert from_dt is None
    assert to_dt == datetime(2026, 4, 17, 0, 0, 0, tzinfo=timezone.utc)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bullforge && python -m pytest tests/test_time_window.py -v`

Expected: FAIL — `ModuleNotFoundError: No module named 'shared.time_window'`.

- [ ] **Step 3: Implement `parse_time_window`**

Create `bullforge/shared/time_window.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Tuple

from aiohttp import web


def _parse_iso(raw: str, field: str) -> datetime:
    """Parse an ISO 8601 string to a UTC-aware datetime.

    Accepts Z suffix, explicit offset, or naive (treated as UTC).
    Raises HTTPBadRequest on any malformed input.
    """
    try:
        # datetime.fromisoformat handles '+00:00' and naive; normalize 'Z' first.
        s = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
        dt = datetime.fromisoformat(s)
    except ValueError as exc:
        raise web.HTTPBadRequest(
            reason=f"invalid ISO datetime for '{field}': {raw!r}",
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def parse_time_window(
    request: web.Request,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Extract optional ?from= and ?to= query params as UTC datetimes.

    Returns (from_dt, to_dt). Either or both can be None when absent.
    Raises HTTPBadRequest on malformed input.
    """
    raw_from = request.query.get("from")
    raw_to = request.query.get("to")
    from_dt = _parse_iso(raw_from, "from") if raw_from else None
    to_dt = _parse_iso(raw_to, "to") if raw_to else None
    return from_dt, to_dt
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bullforge && python -m pytest tests/test_time_window.py -v`

Expected: All 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add bullforge/shared/time_window.py bullforge/tests/test_time_window.py
git commit -m "feat(shared): add parse_time_window helper for dashboard ?from/?to"
```

---

## Task 2: Wire time-window into existing metrics endpoints

**Files:**
- Modify: `bullforge/services/dashboard_service/api.py`
- Modify: `bullforge/tests/test_dashboard_api.py`
- Modify: `bullforge/shared/db.py` (extend `query_metric_snapshots` only)

- [ ] **Step 1: Write failing test that existing endpoint accepts `?from/?to`**

Append to `bullforge/tests/test_dashboard_api.py` (after the existing tests):

```python
@pytest.mark.asyncio
async def test_metrics_history_accepts_time_window(aiohttp_client, mock_db):
    from services.dashboard_service.api import create_api_app
    app = create_api_app(mock_db)
    client = await aiohttp_client(app)
    resp = await client.get(
        "/api/metrics/history"
        "?category=trading&window=all"
        "&from=2026-04-01T00:00:00Z&to=2026-04-17T00:00:00Z",
    )
    assert resp.status == 200

    # Confirm the DB was called with the parsed datetimes.
    call = mock_db.query_metric_snapshots.call_args
    kwargs = call.kwargs
    assert kwargs.get("from_dt") == datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert kwargs.get("to_dt") == datetime(2026, 4, 17, 0, 0, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_metrics_history_rejects_malformed_from(aiohttp_client, mock_db):
    from services.dashboard_service.api import create_api_app
    app = create_api_app(mock_db)
    client = await aiohttp_client(app)
    resp = await client.get(
        "/api/metrics/history?category=trading&from=not-a-date",
    )
    assert resp.status == 400
```

- [ ] **Step 2: Run to verify failure**

Run: `cd bullforge && python -m pytest tests/test_dashboard_api.py::test_metrics_history_accepts_time_window tests/test_dashboard_api.py::test_metrics_history_rejects_malformed_from -v`

Expected: FAIL — `query_metric_snapshots` doesn't take `from_dt`/`to_dt`, and endpoint doesn't call `parse_time_window`.

- [ ] **Step 3: Extend `query_metric_snapshots` to accept `from_dt`/`to_dt`**

In `bullforge/shared/db.py`, find `async def query_metric_snapshots` and replace the method body to accept and filter on optional `from_dt`/`to_dt`. The new signature:

```python
    async def query_metric_snapshots(
        self,
        category: str,
        scope: str,
        window: str,
        limit: int = 60,
        *,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> list[dict]:
        query = """
            SELECT id, timestamp, category, scope, window, metrics
            FROM metric_snapshots
            WHERE category = $1 AND scope = $2 AND window = $3
        """
        args: list = [category, scope, window]
        idx = 4
        if from_dt is not None:
            query += f" AND timestamp >= ${idx}"
            args.append(from_dt)
            idx += 1
        if to_dt is not None:
            query += f" AND timestamp < ${idx}"
            args.append(to_dt)
            idx += 1
        query += f" ORDER BY timestamp DESC LIMIT ${idx}"
        args.append(limit)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [
            {
                "id": r["id"],
                "timestamp": r["timestamp"],
                "category": r["category"],
                "scope": r["scope"],
                "window": r["window"],
                "metrics": json.loads(r["metrics"]) if isinstance(r["metrics"], str) else r["metrics"],
            }
            for r in rows
        ]
```

Preserve the existing method signature's positional args (`category`, `scope`, `window`, `limit`) so all existing callers keep working. The new `from_dt`/`to_dt` are keyword-only.

- [ ] **Step 4: Wire the parser into `_handle_metrics_history`**

In `bullforge/services/dashboard_service/api.py`:

1. Add import at the top:
   ```python
   from shared.time_window import parse_time_window
   ```

2. Replace `_handle_metrics_history`:
   ```python
   async def _handle_metrics_history(request: web.Request) -> web.Response:
       db: Database = request.app["db"]
       category = request.query.get("category", "trading")
       scope = request.query.get("scope", "")
       window = request.query.get("window", "all")
       limit = min(int(request.query.get("limit", "60")), 500)

       from_dt, to_dt = parse_time_window(request)

       snapshots = await db.query_metric_snapshots(
           category, scope, window, limit,
           from_dt=from_dt, to_dt=to_dt,
       )

       return web.json_response([
           {
               "timestamp": s["timestamp"].isoformat(),
               "metrics": s["metrics"],
           }
           for s in snapshots
       ])
   ```

- [ ] **Step 5: Run tests**

Run: `cd bullforge && python -m pytest tests/test_dashboard_api.py tests/test_time_window.py -v`

Expected: All PASS, including the two new tests.

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/dashboard_service/api.py \
        bullforge/shared/db.py \
        bullforge/tests/test_dashboard_api.py
git commit -m "feat(dashboard): accept ?from/?to on /api/metrics/history"
```

---

## Task 3: Stub all Plan 2-4 endpoints as HTTP 501

**Files:**
- Create: `bullforge/services/dashboard_service/stubs_api.py`
- Create: `bullforge/tests/test_dashboard_stubs.py`
- Modify: `bullforge/services/dashboard_service/main.py`

- [ ] **Step 1: Write failing tests**

Create `bullforge/tests/test_dashboard_stubs.py`:

```python
import pytest
from aiohttp import web

from services.dashboard_service.stubs_api import register_stub_routes, STUB_ENDPOINTS


@pytest.fixture
def stub_app():
    app = web.Application()
    register_stub_routes(app)
    return app


@pytest.mark.asyncio
async def test_every_stub_returns_501(aiohttp_client, stub_app):
    client = await aiohttp_client(stub_app)
    for path, meta in STUB_ENDPOINTS.items():
        # Substitute path params with dummy values for the GET call.
        concrete = (
            path.replace("{id}", "1")
                .replace("{sym}", "AAPL")
                .replace("{run_id}", "1")
        )
        resp = await client.get(concrete)
        assert resp.status == 501, f"{concrete} returned {resp.status}"
        body = await resp.json()
        assert body["error"] == "not_implemented"
        assert body["enabled_after_plan"] == meta["enabled_after_plan"]


@pytest.mark.asyncio
async def test_stubs_include_every_spec_endpoint():
    expected_prefixes = {
        "/api/models/{id}/history",
        "/api/models/{id}/calibration",
        "/api/models/{id}/simulated_pnl",
        "/api/models/{id}/feature_importance",
        "/api/models/{id}/regime_breakdown",
        "/api/models/{id}/signal_history",
        "/api/trading/equity",
        "/api/trading/distributions",
        "/api/trading/heatmap",
        "/api/trading/streaks",
        "/api/trading/exposure",
        "/api/trading/benchmark",
        "/api/trading/symbols/{sym}",
        "/api/backtests/{run_id}/equity",
        "/api/backtests/{run_id}/folds",
        "/api/backtests/{run_id}/symbols",
        "/api/backtests/{run_id}/distributions",
        "/api/backtests/{run_id}/live-overlay",
        "/api/backtests/{run_id}/funnel",
        "/api/backtests/compare",
        "/api/backtests/sensitivity",
    }
    assert expected_prefixes <= set(STUB_ENDPOINTS)
```

- [ ] **Step 2: Run to verify failure**

Run: `cd bullforge && python -m pytest tests/test_dashboard_stubs.py -v`

Expected: FAIL — `ModuleNotFoundError: No module named 'services.dashboard_service.stubs_api'`.

- [ ] **Step 3: Implement stubs module**

Create `bullforge/services/dashboard_service/stubs_api.py`:

```python
from __future__ import annotations

import logging

from aiohttp import web

logger = logging.getLogger(__name__)

# Each entry: path → {enabled_after_plan}. These 501s are placeholders for
# Plans 2, 3, 4; downstream plans will replace a handler by registering a
# real route with the same path (aiohttp resolves most-recently-registered
# last, but in practice each downstream plan removes the stub line from
# STUB_ENDPOINTS as it implements the real route).
STUB_ENDPOINTS: dict[str, dict] = {
    # --- Plan 2: Models domain ---------------------------------------
    "/api/models/{id}/history":             {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/calibration":         {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/simulated_pnl":       {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/feature_importance":  {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/regime_breakdown":    {"enabled_after_plan": "plan_2"},
    "/api/models/{id}/signal_history":      {"enabled_after_plan": "plan_2"},

    # --- Plan 3: Trading domain --------------------------------------
    "/api/trading/equity":                  {"enabled_after_plan": "plan_3"},
    "/api/trading/distributions":           {"enabled_after_plan": "plan_3"},
    "/api/trading/heatmap":                 {"enabled_after_plan": "plan_3"},
    "/api/trading/streaks":                 {"enabled_after_plan": "plan_3"},
    "/api/trading/exposure":                {"enabled_after_plan": "plan_3"},
    "/api/trading/benchmark":               {"enabled_after_plan": "plan_3"},
    "/api/trading/symbols/{sym}":           {"enabled_after_plan": "plan_3"},

    # --- Plan 4: Backtests domain ------------------------------------
    "/api/backtests/{run_id}/equity":       {"enabled_after_plan": "plan_4"},
    "/api/backtests/{run_id}/folds":        {"enabled_after_plan": "plan_4"},
    "/api/backtests/{run_id}/symbols":      {"enabled_after_plan": "plan_4"},
    "/api/backtests/{run_id}/distributions":{"enabled_after_plan": "plan_4"},
    "/api/backtests/{run_id}/live-overlay": {"enabled_after_plan": "plan_4"},
    "/api/backtests/{run_id}/funnel":       {"enabled_after_plan": "plan_4"},
    "/api/backtests/compare":               {"enabled_after_plan": "plan_4"},
    "/api/backtests/sensitivity":           {"enabled_after_plan": "plan_4"},
}


def _make_handler(enabled_after_plan: str):
    async def handler(request: web.Request) -> web.Response:
        return web.json_response(
            {
                "error": "not_implemented",
                "enabled_after_plan": enabled_after_plan,
            },
            status=501,
        )
    return handler


def register_stub_routes(app: web.Application) -> None:
    for path, meta in STUB_ENDPOINTS.items():
        app.router.add_get(path, _make_handler(meta["enabled_after_plan"]))
```

- [ ] **Step 4: Register stub routes in main.py**

In `bullforge/services/dashboard_service/main.py`, add the import:

```python
from services.dashboard_service.stubs_api import register_stub_routes
```

And register them after the other route registrations (right before `app.router.add_get("/health", health_handler)`):

```python
    register_stub_routes(app)
```

- [ ] **Step 5: Run tests**

Run: `cd bullforge && python -m pytest tests/test_dashboard_stubs.py -v`

Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/dashboard_service/stubs_api.py \
        bullforge/services/dashboard_service/main.py \
        bullforge/tests/test_dashboard_stubs.py
git commit -m "feat(dashboard): stub Plan 2-4 endpoints as HTTP 501"
```

---

## Task 4: Router component

**Files:**
- Create: `bullforge/services/dashboard_service/static/components/router.js`
- Create: `bullforge/services/dashboard_service/static/test/router.html`

- [ ] **Step 1: Write the failing browser-assertion test harness**

Create `bullforge/services/dashboard_service/static/test/router.html`:

```html
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Router tests</title>
<style>body{font-family:monospace}.pass{color:green}.fail{color:red}</style>
</head>
<body>
<h1>Router tests</h1>
<div id="results"></div>
<script src="/static/components/router.js"></script>
<script>
(function () {
  var results = document.getElementById('results');
  var passed = 0, failed = 0;

  function assertDeep(name, actual, expected) {
    var a = JSON.stringify(actual);
    var e = JSON.stringify(expected);
    var ok = a === e;
    var div = document.createElement('div');
    div.className = ok ? 'pass' : 'fail';
    div.style.whiteSpace = 'pre';
    div.textContent = (ok ? 'PASS' : 'FAIL') + ' — ' + name +
      (ok ? '' : '\n  expected: ' + e + '\n  actual:   ' + a);
    results.appendChild(div);
    if (ok) passed++; else failed++;
  }

  var R = window.Router;

  assertDeep('empty hash → overview',
    R.parse(''),
    {tab: 'overview', subtab: null, entity_kind: null, entity_id: null, query: {}});

  assertDeep('#models → models tab',
    R.parse('#models'),
    {tab: 'models', subtab: null, entity_kind: null, entity_id: null, query: {}});

  assertDeep('#models/leaderboard → subtab',
    R.parse('#models/leaderboard'),
    {tab: 'models', subtab: 'leaderboard', entity_kind: null, entity_id: null, query: {}});

  assertDeep('#models/42 → numeric entity',
    R.parse('#models/42'),
    {tab: 'models', subtab: null, entity_kind: 'models', entity_id: '42', query: {}});

  assertDeep('#trading/symbols/AAPL → entity kind + id',
    R.parse('#trading/symbols/AAPL'),
    {tab: 'trading', subtab: null, entity_kind: 'symbols', entity_id: 'AAPL', query: {}});

  assertDeep('#backtests/17/folds → entity + subtab',
    R.parse('#backtests/17/folds'),
    {tab: 'backtests', subtab: 'folds', entity_kind: 'backtests', entity_id: '17', query: {}});

  assertDeep('#backtests/compare?ids=17,22 → compare with query',
    R.parse('#backtests/compare?ids=17,22'),
    {tab: 'backtests', subtab: 'compare', entity_kind: null, entity_id: null, query: {ids: '17,22'}});

  assertDeep('#unknown → unknown tab pass-through',
    R.parse('#garbage'),
    {tab: 'garbage', subtab: null, entity_kind: null, entity_id: null, query: {}});

  // Subscribe/notify
  var captured = null;
  var unsubscribe = R.subscribe(function (route) { captured = route; });
  R.navigate('#models/calibration');
  assertDeep('navigate fires subscribers',
    captured,
    {tab: 'models', subtab: 'calibration', entity_kind: null, entity_id: null, query: {}});
  unsubscribe();
  R.navigate('#trading');
  // After unsubscribe, captured should still reflect the earlier notification.
  assertDeep('unsubscribe stops notifications',
    captured,
    {tab: 'models', subtab: 'calibration', entity_kind: null, entity_id: null, query: {}});

  // Summary
  var summary = document.createElement('div');
  summary.style.marginTop = '1em';
  summary.textContent = 'Results: ' + passed + ' passed, ' + failed + ' failed';
  results.appendChild(summary);
  if (failed === 0) document.title = 'PASS — Router tests';
  else document.title = 'FAIL — Router tests';
})();
</script>
</body>
</html>
```

- [ ] **Step 2: Implement router**

Create `bullforge/services/dashboard_service/static/components/router.js`:

```javascript
// components/router.js
// URL hash router. Parses #tab/subtab/entity-kind/entity-id?query=... into
// a canonical {tab, subtab, entity_kind, entity_id, query} object.
//
// Known entity kinds: "models" (when second segment is numeric),
// "symbols", "backtests" (numeric run id as third segment under backtests/).
//
// Subscribers are notified on hashchange and on explicit navigate().
//
// Public API: window.Router = {parse, navigate, subscribe, current}
(function (global) {
  var subscribers = [];

  var NUMERIC = /^\d+$/;

  function parseQuery(qs) {
    var out = {};
    if (!qs) return out;
    qs.split('&').forEach(function (pair) {
      if (!pair) return;
      var eq = pair.indexOf('=');
      var k = eq >= 0 ? pair.slice(0, eq) : pair;
      var v = eq >= 0 ? pair.slice(eq + 1) : '';
      out[decodeURIComponent(k)] = decodeURIComponent(v);
    });
    return out;
  }

  function parse(hash) {
    if (!hash) hash = '';
    if (hash.charAt(0) === '#') hash = hash.slice(1);

    // Split off query string if present.
    var qIdx = hash.indexOf('?');
    var query = {};
    if (qIdx >= 0) {
      query = parseQuery(hash.slice(qIdx + 1));
      hash = hash.slice(0, qIdx);
    }

    var route = {tab: 'overview', subtab: null, entity_kind: null, entity_id: null, query: query};
    if (!hash) return route;

    var parts = hash.split('/').filter(function (p) { return p.length > 0; });
    if (parts.length === 0) return route;

    route.tab = parts[0];

    if (parts.length === 1) return route;

    // Classify second segment.
    //   #<tab>/<numeric>           → entity (kind = tab) + id = numeric
    //   #<tab>/<text>              → subtab
    //   #<tab>/<kind>/<id>         → entity kind + id
    //   #<tab>/<kind>/<id>/<sub>   → entity kind + id + subtab
    if (parts.length === 2) {
      if (NUMERIC.test(parts[1])) {
        route.entity_kind = parts[0];
        route.entity_id = parts[1];
      } else {
        route.subtab = parts[1];
      }
      return route;
    }

    // 3+ segments.
    route.entity_kind = parts[1];
    route.entity_id = parts[2];
    if (parts.length >= 4) {
      route.subtab = parts[3];
    }
    return route;
  }

  function current() {
    return parse(global.location.hash);
  }

  function notify() {
    var route = current();
    subscribers.slice().forEach(function (fn) {
      try { fn(route); } catch (e) { console.error('Router subscriber error:', e); }
    });
  }

  function navigate(hash) {
    if (hash && hash.charAt(0) !== '#') hash = '#' + hash;
    if (global.location.hash === hash) {
      notify();  // force re-fire if same hash
      return;
    }
    global.location.hash = hash || '';
    // hashchange event will fire notify() too; avoid double firing by
    // only notifying here if we forced same-hash above.
  }

  function subscribe(fn) {
    subscribers.push(fn);
    return function unsubscribe() {
      var i = subscribers.indexOf(fn);
      if (i >= 0) subscribers.splice(i, 1);
    };
  }

  global.addEventListener('hashchange', notify);

  global.Router = {
    parse: parse,
    navigate: navigate,
    subscribe: subscribe,
    current: current,
  };
})(window);
```

- [ ] **Step 3: Run test harness manually**

In a browser (dashboard dev setup running), open:
`http://localhost:18084/static/test/router.html`

Expected: All PASS lines; document title `PASS — Router tests`.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/dashboard_service/static/components/router.js \
        bullforge/services/dashboard_service/static/test/router.html
git commit -m "feat(dashboard): add Router component with hash-based routing"
```

---

## Task 5: Time-window picker component

**Files:**
- Create: `bullforge/services/dashboard_service/static/components/timewindow.js`
- Create: `bullforge/services/dashboard_service/static/test/timewindow.html`

- [ ] **Step 1: Write the failing test harness**

Create `bullforge/services/dashboard_service/static/test/timewindow.html`:

```html
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>TimeWindow tests</title>
<style>body{font-family:monospace}.pass{color:green}.fail{color:red}
#host{border:1px solid #888;padding:1em;margin:1em 0;display:inline-block}
</style>
</head>
<body>
<h1>TimeWindow tests</h1>
<div id="host"></div>
<div id="results"></div>
<script src="/static/components/timewindow.js"></script>
<script>
(function () {
  var results = document.getElementById('results');
  var host = document.getElementById('host');
  var passed = 0, failed = 0;

  function assert(name, cond, detail) {
    var ok = !!cond;
    var div = document.createElement('div');
    div.className = ok ? 'pass' : 'fail';
    div.style.whiteSpace = 'pre';
    div.textContent = (ok ? 'PASS' : 'FAIL') + ' — ' + name + (ok || !detail ? '' : '\n  ' + detail);
    results.appendChild(div);
    if (ok) passed++; else failed++;
  }

  // Clear any prior sessionStorage for determinism.
  sessionStorage.removeItem('tw.test');

  var lastEvent = null;
  var tw = window.TimeWindow.create({
    mountEl: host,
    storageKey: 'tw.test',
    onChange: function (range) { lastEvent = range; },
  });

  assert('mounts a <select> element', host.querySelector('select') !== null);

  var presets = Array.prototype.map.call(host.querySelectorAll('select option'),
    function (o) { return o.value; });
  assert('has expected presets',
    presets.indexOf('1d') >= 0 && presets.indexOf('7d') >= 0 &&
    presets.indexOf('30d') >= 0 && presets.indexOf('90d') >= 0 &&
    presets.indexOf('180d') >= 0 && presets.indexOf('1y') >= 0 &&
    presets.indexOf('all') >= 0 && presets.indexOf('custom') >= 0,
    'presets found: ' + presets.join(','));

  tw.setPreset('7d');
  assert('setPreset fires onChange', lastEvent !== null);
  assert('7d preset has from and to',
    lastEvent && lastEvent.from && lastEvent.to,
    'got: ' + JSON.stringify(lastEvent));

  // Window of 7d means to - from ≈ 7 * 86400 * 1000 ms.
  var diff = new Date(lastEvent.to) - new Date(lastEvent.from);
  var expected = 7 * 86400 * 1000;
  assert('7d preset diff is ~7 days',
    Math.abs(diff - expected) < 60 * 1000,
    'diff ms: ' + diff);

  tw.setPreset('all');
  assert('all preset has from=null and to=null',
    lastEvent && lastEvent.from === null && lastEvent.to === null,
    'got: ' + JSON.stringify(lastEvent));

  // Persistence across a fresh instance.
  tw.setPreset('30d');
  var tw2 = window.TimeWindow.create({
    mountEl: document.createElement('div'),
    storageKey: 'tw.test',
    onChange: function () {},
  });
  assert('preset persisted via sessionStorage',
    tw2.getCurrentPreset() === '30d',
    'got: ' + tw2.getCurrentPreset());

  // Custom range
  tw.setCustom('2026-04-01T00:00:00Z', '2026-04-17T00:00:00Z');
  assert('custom range sets preset to custom',
    tw.getCurrentPreset() === 'custom');
  assert('custom range fires onChange with exact values',
    lastEvent.from === '2026-04-01T00:00:00Z' &&
    lastEvent.to === '2026-04-17T00:00:00Z',
    'got: ' + JSON.stringify(lastEvent));

  sessionStorage.removeItem('tw.test');
  var summary = document.createElement('div');
  summary.style.marginTop = '1em';
  summary.textContent = 'Results: ' + passed + ' passed, ' + failed + ' failed';
  results.appendChild(summary);
  document.title = failed === 0 ? 'PASS — TimeWindow tests' : 'FAIL — TimeWindow tests';
})();
</script>
</body>
</html>
```

- [ ] **Step 2: Implement the time-window component**

Create `bullforge/services/dashboard_service/static/components/timewindow.js`:

```javascript
// components/timewindow.js
// Shared time-window picker. Emits {from: ISO|null, to: ISO|null} on change.
// Persists the chosen preset/custom range in sessionStorage under a caller-
// supplied key so switching tabs doesn't reset selections.
//
// Usage:
//   var tw = TimeWindow.create({
//     mountEl: document.getElementById('slot'),
//     storageKey: 'models.timewindow',
//     onChange: function (range) { ... },
//   });
(function (global) {
  var PRESETS = [
    {value: '1d',   label: 'Last 1 day',    days: 1},
    {value: '7d',   label: 'Last 7 days',   days: 7},
    {value: '30d',  label: 'Last 30 days',  days: 30},
    {value: '90d',  label: 'Last 90 days',  days: 90},
    {value: '180d', label: 'Last 180 days', days: 180},
    {value: '1y',   label: 'Last 1 year',   days: 365},
    {value: 'all',  label: 'All time',      days: null},
    {value: 'custom', label: 'Custom…',     days: null},
  ];

  function nowIso() { return new Date().toISOString(); }

  function daysAgoIso(days) {
    var d = new Date(Date.now() - days * 86400 * 1000);
    return d.toISOString();
  }

  function rangeForPreset(preset, custom) {
    if (preset === 'all') return {from: null, to: null};
    if (preset === 'custom') return {from: custom.from, to: custom.to};
    var p = PRESETS.filter(function (x) { return x.value === preset; })[0];
    if (!p || p.days === null) return {from: null, to: null};
    return {from: daysAgoIso(p.days), to: nowIso()};
  }

  function create(opts) {
    if (!opts || !opts.mountEl) throw new Error('TimeWindow.create requires mountEl');
    var storageKey = opts.storageKey || null;
    var onChange = opts.onChange || function () {};

    // Restore persisted state.
    var saved = null;
    if (storageKey) {
      try { saved = JSON.parse(sessionStorage.getItem(storageKey) || 'null'); }
      catch (e) { saved = null; }
    }
    var currentPreset = (saved && saved.preset) || '30d';
    var customFrom = (saved && saved.customFrom) || '';
    var customTo = (saved && saved.customTo) || '';

    // Build DOM.
    opts.mountEl.innerHTML = '';
    var select = document.createElement('select');
    select.className = 'tw-select';
    PRESETS.forEach(function (p) {
      var opt = document.createElement('option');
      opt.value = p.value;
      opt.textContent = p.label;
      select.appendChild(opt);
    });
    select.value = currentPreset;
    opts.mountEl.appendChild(select);

    var customFromInput = document.createElement('input');
    customFromInput.type = 'datetime-local';
    customFromInput.className = 'tw-custom tw-from';
    customFromInput.style.marginLeft = '0.5em';
    var customToInput = document.createElement('input');
    customToInput.type = 'datetime-local';
    customToInput.className = 'tw-custom tw-to';
    customToInput.style.marginLeft = '0.5em';
    opts.mountEl.appendChild(customFromInput);
    opts.mountEl.appendChild(customToInput);

    function updateCustomVisibility() {
      var visible = currentPreset === 'custom';
      customFromInput.style.display = visible ? '' : 'none';
      customToInput.style.display = visible ? '' : 'none';
    }
    updateCustomVisibility();

    function persist() {
      if (!storageKey) return;
      sessionStorage.setItem(storageKey, JSON.stringify({
        preset: currentPreset,
        customFrom: customFrom,
        customTo: customTo,
      }));
    }

    function emit() {
      var range = rangeForPreset(currentPreset, {from: customFrom, to: customTo});
      persist();
      onChange(range);
    }

    function setPreset(preset) {
      currentPreset = preset;
      select.value = preset;
      updateCustomVisibility();
      emit();
    }

    function setCustom(fromIso, toIso) {
      currentPreset = 'custom';
      select.value = 'custom';
      customFrom = fromIso;
      customTo = toIso;
      customFromInput.value = fromIso ? fromIso.slice(0, 16) : '';
      customToInput.value = toIso ? toIso.slice(0, 16) : '';
      updateCustomVisibility();
      emit();
    }

    select.addEventListener('change', function () {
      setPreset(select.value);
    });
    customFromInput.addEventListener('change', function () {
      var v = customFromInput.value;
      customFrom = v ? new Date(v).toISOString() : '';
      if (currentPreset === 'custom') emit();
    });
    customToInput.addEventListener('change', function () {
      var v = customToInput.value;
      customTo = v ? new Date(v).toISOString() : '';
      if (currentPreset === 'custom') emit();
    });

    // Initial emit so the consumer gets the restored state.
    emit();

    return {
      setPreset: setPreset,
      setCustom: setCustom,
      getCurrentPreset: function () { return currentPreset; },
      getCurrentRange: function () {
        return rangeForPreset(currentPreset, {from: customFrom, to: customTo});
      },
    };
  }

  global.TimeWindow = {create: create, PRESETS: PRESETS};
})(window);
```

- [ ] **Step 3: Run test harness**

In a browser, open `http://localhost:18084/static/test/timewindow.html`.

Expected: All PASS; title `PASS — TimeWindow tests`.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/dashboard_service/static/components/timewindow.js \
        bullforge/services/dashboard_service/static/test/timewindow.html
git commit -m "feat(dashboard): add TimeWindow picker with sessionStorage persistence"
```

---

## Task 6: Refresh control component

**Files:**
- Create: `bullforge/services/dashboard_service/static/components/refresh.js`
- Create: `bullforge/services/dashboard_service/static/test/refresh.html`

- [ ] **Step 1: Write the failing test harness**

Create `bullforge/services/dashboard_service/static/test/refresh.html`:

```html
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Refresh tests</title>
<style>body{font-family:monospace}.pass{color:green}.fail{color:red}
#host{border:1px solid #888;padding:1em;margin:1em 0;display:inline-block}
</style>
</head>
<body>
<h1>Refresh tests</h1>
<div id="host"></div>
<div id="results"></div>
<script src="/static/components/refresh.js"></script>
<script>
(function () {
  var results = document.getElementById('results');
  var host = document.getElementById('host');
  var passed = 0, failed = 0;

  function assert(name, cond, detail) {
    var ok = !!cond;
    var div = document.createElement('div');
    div.className = ok ? 'pass' : 'fail';
    div.style.whiteSpace = 'pre';
    div.textContent = (ok ? 'PASS' : 'FAIL') + ' — ' + name + (ok || !detail ? '' : '\n  ' + detail);
    results.appendChild(div);
    if (ok) passed++; else failed++;
  }

  sessionStorage.removeItem('refresh.test');

  var fireCount = 0;
  var ctrl = window.Refresh.create({
    mountEl: host,
    storageKey: 'refresh.test',
    onRefresh: function () { fireCount++; },
    tickMs: 100,  // 100ms for test purposes
    initialIntervalSec: 0.1,  // 100ms
  });

  assert('has interval selector', host.querySelector('select') !== null);
  assert('has pause button', host.querySelector('button') !== null);

  // Let it fire a couple of times.
  setTimeout(function () {
    assert('fires onRefresh while running', fireCount >= 1,
      'fired ' + fireCount + ' times in 250ms');

    ctrl.pause();
    var beforePause = fireCount;
    setTimeout(function () {
      assert('does not fire while paused', fireCount === beforePause,
        'paused, but fired ' + (fireCount - beforePause) + ' more times');

      // Persistence.
      var ctrl2 = window.Refresh.create({
        mountEl: document.createElement('div'),
        storageKey: 'refresh.test',
        onRefresh: function () {},
        tickMs: 100,
        initialIntervalSec: 30,
      });
      assert('paused state persists', ctrl2.isPaused() === true);
      ctrl2.destroy();
      ctrl.destroy();

      sessionStorage.removeItem('refresh.test');
      var summary = document.createElement('div');
      summary.style.marginTop = '1em';
      summary.textContent = 'Results: ' + passed + ' passed, ' + failed + ' failed';
      results.appendChild(summary);
      document.title = failed === 0 ? 'PASS — Refresh tests' : 'FAIL — Refresh tests';
    }, 250);
  }, 250);
})();
</script>
</body>
</html>
```

- [ ] **Step 2: Implement the refresh control**

Create `bullforge/services/dashboard_service/static/components/refresh.js`:

```javascript
// components/refresh.js
// Shared refresh control: interval selector + pause/resume button.
// Fires onRefresh() on a user-configurable interval, persisted in
// sessionStorage when a storageKey is provided.
//
// Usage:
//   var ctrl = Refresh.create({
//     mountEl: document.getElementById('slot'),
//     storageKey: 'trading.refresh',
//     onRefresh: function () { loadData(); },
//   });
(function (global) {
  var INTERVALS = [
    {value: 10, label: '10s'},
    {value: 30, label: '30s'},
    {value: 60, label: '60s'},
    {value: 0,  label: 'Manual'},
  ];

  function create(opts) {
    if (!opts || !opts.mountEl) throw new Error('Refresh.create requires mountEl');
    var storageKey = opts.storageKey || null;
    var onRefresh = opts.onRefresh || function () {};
    var tickMs = opts.tickMs || 1000;  // granularity for internal timer

    // Restore persisted state.
    var saved = null;
    if (storageKey) {
      try { saved = JSON.parse(sessionStorage.getItem(storageKey) || 'null'); }
      catch (e) { saved = null; }
    }
    var intervalSec = saved && typeof saved.intervalSec === 'number'
      ? saved.intervalSec
      : (typeof opts.initialIntervalSec === 'number' ? opts.initialIntervalSec : 30);
    var paused = saved && saved.paused === true;

    opts.mountEl.innerHTML = '';
    var select = document.createElement('select');
    select.className = 'refresh-interval';
    INTERVALS.forEach(function (i) {
      var opt = document.createElement('option');
      opt.value = String(i.value);
      opt.textContent = i.label;
      select.appendChild(opt);
    });
    select.value = String(intervalSec);
    opts.mountEl.appendChild(select);

    var btn = document.createElement('button');
    btn.className = 'refresh-btn';
    btn.style.marginLeft = '0.5em';
    function updateBtn() { btn.textContent = paused ? '▶ Resume' : '⏸ Pause'; }
    updateBtn();
    opts.mountEl.appendChild(btn);

    var manualBtn = document.createElement('button');
    manualBtn.className = 'refresh-manual';
    manualBtn.style.marginLeft = '0.5em';
    manualBtn.textContent = '↻ Refresh now';
    opts.mountEl.appendChild(manualBtn);

    var lastFireTs = Date.now();

    function persist() {
      if (!storageKey) return;
      sessionStorage.setItem(storageKey, JSON.stringify({
        intervalSec: intervalSec, paused: paused,
      }));
    }

    function tick() {
      if (paused || intervalSec === 0) return;
      if (Date.now() - lastFireTs >= intervalSec * 1000) {
        lastFireTs = Date.now();
        try { onRefresh(); } catch (e) { console.error('Refresh.onRefresh error:', e); }
      }
    }

    var timerId = setInterval(tick, tickMs);

    select.addEventListener('change', function () {
      intervalSec = parseInt(select.value, 10);
      lastFireTs = Date.now();
      persist();
    });

    btn.addEventListener('click', function () {
      paused = !paused;
      updateBtn();
      persist();
      if (!paused) { lastFireTs = Date.now(); }
    });

    manualBtn.addEventListener('click', function () {
      lastFireTs = Date.now();
      try { onRefresh(); } catch (e) { console.error('Refresh.onRefresh error:', e); }
    });

    function destroy() {
      clearInterval(timerId);
      opts.mountEl.innerHTML = '';
    }

    function pause() { if (!paused) { paused = true; updateBtn(); persist(); } }
    function resume() { if (paused) { paused = false; lastFireTs = Date.now(); updateBtn(); persist(); } }

    return {
      destroy: destroy,
      pause: pause,
      resume: resume,
      isPaused: function () { return paused; },
      getIntervalSec: function () { return intervalSec; },
    };
  }

  global.Refresh = {create: create, INTERVALS: INTERVALS};
})(window);
```

- [ ] **Step 3: Run test harness**

In a browser, open `http://localhost:18084/static/test/refresh.html`.

Expected: All PASS after ~500ms; title `PASS — Refresh tests`.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/dashboard_service/static/components/refresh.js \
        bullforge/services/dashboard_service/static/test/refresh.html
git commit -m "feat(dashboard): add Refresh control with interval + pause persistence"
```

---

## Task 7: Chart wrappers

**Files:**
- Create: `bullforge/services/dashboard_service/static/components/charts.js`
- Create: `bullforge/services/dashboard_service/static/test/charts.html`

- [ ] **Step 1: Write the visual test harness**

Create `bullforge/services/dashboard_service/static/test/charts.html`:

```html
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Charts tests</title>
<style>
body { font-family: monospace; background: #1a1a2e; color: #e0e0e0; }
.pass { color: #2ecc71; } .fail { color: #e74c3c; }
.chart-host { background: #16213e; padding: 1em; margin: 1em 0;
  width: 600px; height: 260px; border: 1px solid #2a2a4a; }
</style>
</head>
<body>
<h1>Charts tests</h1>
<div id="results"></div>

<h2>lineChart (equity curve demo)</h2>
<div id="h-line" class="chart-host"></div>

<h2>barChart (daily P&L demo)</h2>
<div id="h-bar" class="chart-host"></div>

<h2>histogramChart (trade P&L distribution demo)</h2>
<div id="h-hist" class="chart-host"></div>

<h2>scatterChart (calibration demo)</h2>
<div id="h-scatter" class="chart-host"></div>

<h2>sparkline (inline trend demo)</h2>
<div id="h-spark" style="width:100px;height:24px;"></div>

<h2>heatmapChart (weekday × hour demo)</h2>
<div id="h-heat" class="chart-host"></div>

<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<script src="/static/components/charts.js"></script>
<script>
(function () {
  var results = document.getElementById('results');
  var passed = 0, failed = 0;
  function assert(name, cond, detail) {
    var ok = !!cond;
    var div = document.createElement('div');
    div.className = ok ? 'pass' : 'fail';
    div.style.whiteSpace = 'pre';
    div.textContent = (ok ? 'PASS' : 'FAIL') + ' — ' + name + (ok || !detail ? '' : '\n  ' + detail);
    results.appendChild(div);
    if (ok) passed++; else failed++;
  }

  var C = window.Charts;

  // lineChart
  var line = C.lineChart(document.getElementById('h-line'), {
    series: [{label: 'Equity', data: [
      {x: '2026-04-10T00:00:00Z', y: 100000},
      {x: '2026-04-11T00:00:00Z', y: 101200},
      {x: '2026-04-12T00:00:00Z', y: 100800},
      {x: '2026-04-13T00:00:00Z', y: 102500},
    ]}],
    xIsTime: true,
  });
  assert('lineChart returns a Chart instance', line && typeof line.destroy === 'function');

  // barChart
  var bar = C.barChart(document.getElementById('h-bar'), {
    labels: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'],
    values: [120, -45, 80, -30, 210],
    colorByValue: true,
  });
  assert('barChart returns a Chart instance', bar && typeof bar.destroy === 'function');

  // histogramChart
  var vals = [];
  for (var i = 0; i < 500; i++) vals.push((Math.random() - 0.5) * 200);
  var hist = C.histogramChart(document.getElementById('h-hist'), {
    values: vals, bins: 20,
  });
  assert('histogramChart returns a Chart instance', hist && typeof hist.destroy === 'function');

  // scatterChart
  var scatter = C.scatterChart(document.getElementById('h-scatter'), {
    points: [
      {x: 0.1, y: 0.08}, {x: 0.3, y: 0.28}, {x: 0.5, y: 0.52},
      {x: 0.7, y: 0.68}, {x: 0.9, y: 0.85},
    ],
    referenceDiagonal: true,
  });
  assert('scatterChart returns a Chart instance', scatter && typeof scatter.destroy === 'function');

  // sparkline
  var spark = C.sparkline(document.getElementById('h-spark'), {
    values: [1, 3, 2, 5, 4, 7, 6, 8, 10, 9],
  });
  assert('sparkline creates an SVG', document.querySelector('#h-spark svg') !== null);

  // heatmapChart (custom grid, no Chart.js)
  var heat = C.heatmapChart(document.getElementById('h-heat'), {
    xLabels: ['09:30', '10:00', '10:30', '11:00'],
    yLabels: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'],
    cells: [
      [ 0.002, -0.001,  0.003, 0.000],
      [-0.005,  0.001, -0.002, 0.004],
      [ 0.001,  0.003, -0.001, 0.002],
      [ 0.000, -0.004,  0.001, 0.003],
      [-0.002,  0.005,  0.000, 0.001],
    ],
  });
  assert('heatmapChart creates a grid element', document.querySelector('#h-heat .heatmap-grid') !== null);

  // Re-render smoke test: creating a second line chart on same element should not throw.
  var line2 = C.lineChart(document.getElementById('h-line'), {
    series: [{label: 'Equity', data: [{x: '2026-04-10T00:00:00Z', y: 100000}]}],
    xIsTime: true,
  });
  assert('lineChart re-render destroys prior instance',
    line2 && typeof line2.destroy === 'function');

  var summary = document.createElement('div');
  summary.style.marginTop = '1em';
  summary.textContent = 'Results: ' + passed + ' passed, ' + failed + ' failed';
  results.appendChild(summary);
  document.title = failed === 0 ? 'PASS — Charts tests' : 'FAIL — Charts tests';
})();
</script>
</body>
</html>
```

- [ ] **Step 2: Implement the chart wrappers**

Create `bullforge/services/dashboard_service/static/components/charts.js`:

```javascript
// components/charts.js
// Thin wrappers over Chart.js for consistent dark-theme styling and
// automatic destroy-on-rerender. heatmapChart is a colored-cell grid
// implemented directly (not Chart.js) because Chart.js heatmap plugin
// adds a dependency we don't want.
//
// Public API:
//   Charts.lineChart(el, {series, xIsTime, yLabel, drawdownShading})
//   Charts.barChart(el, {labels, values, colorByValue, yLabel})
//   Charts.histogramChart(el, {values, bins, xLabel})
//   Charts.scatterChart(el, {points, referenceDiagonal, xLabel, yLabel})
//   Charts.sparkline(el, {values})
//   Charts.heatmapChart(el, {xLabels, yLabels, cells, colorScale})
(function (global) {
  var THEME = {
    text: '#e0e0e0',
    muted: '#888',
    grid: 'rgba(224, 224, 224, 0.08)',
    accent: '#00b4d8',
    green: '#2ecc71',
    red: '#e74c3c',
    yellow: '#f39c12',
    bgCard: '#16213e',
  };

  // Per-host Chart.js instance cache so re-renders destroy prior chart.
  var instances = new WeakMap();

  function _prepHost(host) {
    var prior = instances.get(host);
    if (prior && typeof prior.destroy === 'function') {
      prior.destroy();
    }
    host.innerHTML = '';
    var canvas = document.createElement('canvas');
    canvas.style.width = '100%';
    canvas.style.height = '100%';
    host.appendChild(canvas);
    return canvas;
  }

  function _baseOptions(xIsTime, yLabel) {
    return {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: {labels: {color: THEME.text}},
        tooltip: {backgroundColor: THEME.bgCard, titleColor: THEME.text, bodyColor: THEME.text},
      },
      scales: {
        x: xIsTime
          ? {type: 'time', ticks: {color: THEME.muted}, grid: {color: THEME.grid}}
          : {ticks: {color: THEME.muted}, grid: {color: THEME.grid}},
        y: {ticks: {color: THEME.muted}, grid: {color: THEME.grid},
            title: yLabel ? {display: true, text: yLabel, color: THEME.muted} : undefined},
      },
    };
  }

  function lineChart(host, opts) {
    var canvas = _prepHost(host);
    var chart = new Chart(canvas, {
      type: 'line',
      data: {
        datasets: (opts.series || []).map(function (s, i) {
          return {
            label: s.label,
            data: s.data,
            borderColor: s.color || (i === 0 ? THEME.accent : [THEME.green, THEME.yellow, THEME.red][(i - 1) % 3]),
            backgroundColor: 'transparent',
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.1,
          };
        }),
      },
      options: _baseOptions(!!opts.xIsTime, opts.yLabel),
    });
    instances.set(host, chart);
    return chart;
  }

  function barChart(host, opts) {
    var canvas = _prepHost(host);
    var colors = (opts.values || []).map(function (v) {
      if (!opts.colorByValue) return THEME.accent;
      return v >= 0 ? THEME.green : THEME.red;
    });
    var chart = new Chart(canvas, {
      type: 'bar',
      data: {
        labels: opts.labels || [],
        datasets: [{
          label: opts.yLabel || '', data: opts.values || [],
          backgroundColor: colors,
        }],
      },
      options: _baseOptions(false, opts.yLabel),
    });
    instances.set(host, chart);
    return chart;
  }

  function histogramChart(host, opts) {
    var values = opts.values || [];
    var bins = opts.bins || 20;
    if (values.length === 0) {
      return barChart(host, {labels: [], values: [], yLabel: 'Count'});
    }
    var min = Math.min.apply(null, values);
    var max = Math.max.apply(null, values);
    if (min === max) { min -= 1; max += 1; }
    var width = (max - min) / bins;
    var counts = new Array(bins).fill(0);
    var labels = [];
    for (var i = 0; i < bins; i++) {
      labels.push((min + i * width).toFixed(2));
    }
    values.forEach(function (v) {
      var idx = Math.min(bins - 1, Math.floor((v - min) / width));
      counts[idx]++;
    });
    return barChart(host, {
      labels: labels, values: counts, yLabel: 'Count', colorByValue: false,
    });
  }

  function scatterChart(host, opts) {
    var canvas = _prepHost(host);
    var datasets = [{
      label: 'Points',
      data: (opts.points || []).map(function (p) { return {x: p.x, y: p.y, r: p.r || 3}; }),
      backgroundColor: THEME.accent,
    }];
    if (opts.referenceDiagonal) {
      datasets.push({
        label: 'Perfect',
        type: 'line',
        data: [{x: 0, y: 0}, {x: 1, y: 1}],
        borderColor: THEME.muted,
        borderDash: [4, 4],
        pointRadius: 0,
        borderWidth: 1,
      });
    }
    var chart = new Chart(canvas, {
      type: 'scatter',
      data: {datasets: datasets},
      options: _baseOptions(false, opts.yLabel),
    });
    instances.set(host, chart);
    return chart;
  }

  function sparkline(host, opts) {
    host.innerHTML = '';
    var values = opts.values || [];
    if (values.length === 0) return;
    var w = host.clientWidth || 100;
    var h = host.clientHeight || 24;
    var min = Math.min.apply(null, values);
    var max = Math.max.apply(null, values);
    if (min === max) { min -= 1; max += 1; }
    var pts = values.map(function (v, i) {
      var x = (i / (values.length - 1)) * w;
      var y = h - ((v - min) / (max - min)) * h;
      return x.toFixed(1) + ',' + y.toFixed(1);
    });
    var svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ' + w + ' ' + h +
      '" width="' + w + '" height="' + h + '" preserveAspectRatio="none">' +
      '<polyline fill="none" stroke="' + THEME.accent + '" stroke-width="1.5" points="' +
      pts.join(' ') + '"/></svg>';
    host.innerHTML = svg;
  }

  function _cellColor(v, scale) {
    // Diverging red-white-green around zero.
    if (v === 0 || v === null || isNaN(v)) return THEME.bgCard;
    var absV = Math.abs(v);
    var intensity = Math.min(1, absV / (scale || 0.01));
    if (v > 0) {
      return 'rgba(46, 204, 113, ' + intensity.toFixed(2) + ')';
    }
    return 'rgba(231, 76, 60, ' + intensity.toFixed(2) + ')';
  }

  function heatmapChart(host, opts) {
    host.innerHTML = '';
    var xLabels = opts.xLabels || [];
    var yLabels = opts.yLabels || [];
    var cells = opts.cells || [];

    // Compute scale if not provided.
    var scale = opts.colorScale;
    if (!scale) {
      var maxAbs = 0;
      cells.forEach(function (row) {
        row.forEach(function (v) { if (typeof v === 'number') maxAbs = Math.max(maxAbs, Math.abs(v)); });
      });
      scale = maxAbs || 0.01;
    }

    var grid = document.createElement('div');
    grid.className = 'heatmap-grid';
    grid.style.display = 'grid';
    grid.style.gridTemplateColumns = '80px repeat(' + xLabels.length + ', 1fr)';
    grid.style.gap = '1px';
    grid.style.fontSize = '11px';
    grid.style.color = THEME.text;

    // Header row.
    var corner = document.createElement('div');
    corner.textContent = '';
    grid.appendChild(corner);
    xLabels.forEach(function (lbl) {
      var cell = document.createElement('div');
      cell.textContent = lbl;
      cell.style.padding = '2px 4px';
      cell.style.textAlign = 'center';
      cell.style.color = THEME.muted;
      grid.appendChild(cell);
    });

    // Data rows.
    yLabels.forEach(function (yLbl, i) {
      var rowHeader = document.createElement('div');
      rowHeader.textContent = yLbl;
      rowHeader.style.padding = '2px 4px';
      rowHeader.style.color = THEME.muted;
      grid.appendChild(rowHeader);
      var row = cells[i] || [];
      for (var j = 0; j < xLabels.length; j++) {
        var v = row[j];
        var cell = document.createElement('div');
        cell.style.background = _cellColor(v, scale);
        cell.style.padding = '6px 2px';
        cell.style.textAlign = 'center';
        cell.style.minHeight = '18px';
        cell.title = yLbl + ' × ' + xLabels[j] + ': ' + (typeof v === 'number' ? v.toFixed(4) : '—');
        cell.textContent = typeof v === 'number' ? v.toFixed(3) : '';
        grid.appendChild(cell);
      }
    });
    host.appendChild(grid);
    return grid;
  }

  global.Charts = {
    lineChart: lineChart,
    barChart: barChart,
    histogramChart: histogramChart,
    scatterChart: scatterChart,
    sparkline: sparkline,
    heatmapChart: heatmapChart,
    THEME: THEME,
  };
})(window);
```

- [ ] **Step 3: Run the visual test harness**

In a browser, open `http://localhost:18084/static/test/charts.html`.

Expected: All PASS lines at top; charts render below each heading; title `PASS — Charts tests`.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/dashboard_service/static/components/charts.js \
        bullforge/services/dashboard_service/static/test/charts.html
git commit -m "feat(dashboard): add Chart.js wrappers + heatmap grid component"
```

---

## Task 8: Leaderboard primitive

**Files:**
- Create: `bullforge/services/dashboard_service/static/components/leaderboard.js`
- Create: `bullforge/services/dashboard_service/static/test/leaderboard.html`

- [ ] **Step 1: Write the failing test harness**

Create `bullforge/services/dashboard_service/static/test/leaderboard.html`:

```html
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Leaderboard tests</title>
<style>
body { font-family: monospace; background: #1a1a2e; color: #e0e0e0; }
.pass { color: #2ecc71; } .fail { color: #e74c3c; }
#host { background: #16213e; padding: 1em; margin: 1em 0; }
</style>
</head>
<body>
<h1>Leaderboard tests</h1>
<div id="host"></div>
<div id="results"></div>
<script src="/static/components/leaderboard.js"></script>
<script>
(function () {
  var host = document.getElementById('host');
  var results = document.getElementById('results');
  var passed = 0, failed = 0;
  function assert(name, cond, detail) {
    var ok = !!cond;
    var div = document.createElement('div');
    div.className = ok ? 'pass' : 'fail';
    div.style.whiteSpace = 'pre';
    div.textContent = (ok ? 'PASS' : 'FAIL') + ' — ' + name + (ok || !detail ? '' : '\n  ' + detail);
    results.appendChild(div);
    if (ok) passed++; else failed++;
  }

  var rows = [
    {symbol: 'AAPL', horizon: 5, accuracy: 0.62, pnl: 123.45},
    {symbol: 'MSFT', horizon: 5, accuracy: 0.58, pnl: -45.10},
    {symbol: 'SPY',  horizon: 10, accuracy: 0.71, pnl: 210.00},
  ];

  var lb = window.Leaderboard.create({
    mountEl: host,
    columns: [
      {key: 'symbol', label: 'Symbol', sortable: true},
      {key: 'horizon', label: 'Horizon', sortable: true},
      {key: 'accuracy', label: 'Accuracy', sortable: true,
       render: function (v) { return (v * 100).toFixed(1) + '%'; }},
      {key: 'pnl', label: 'P&L', sortable: true,
       render: function (v) { return (v >= 0 ? '+' : '') + '$' + v.toFixed(2); }},
    ],
    rows: rows,
    getRowHref: function (r) { return '#models/' + r.symbol; },
  });

  assert('renders a table', host.querySelector('table') !== null);
  assert('renders N rows', host.querySelectorAll('tbody tr').length === 3);

  // Click sort on accuracy.
  var accHeader = host.querySelectorAll('thead th')[2];
  accHeader.click();
  var cells = host.querySelectorAll('tbody tr td:first-child');
  assert('sort by accuracy desc places SPY first',
    cells[0].textContent === 'SPY',
    'got: ' + cells[0].textContent);

  accHeader.click();  // toggle asc
  cells = host.querySelectorAll('tbody tr td:first-child');
  assert('sort by accuracy asc places MSFT first',
    cells[0].textContent === 'MSFT',
    'got: ' + cells[0].textContent);

  // Row href
  var firstLink = host.querySelector('tbody tr td a');
  assert('rows are clickable links via getRowHref',
    firstLink && firstLink.getAttribute('href') === '#models/MSFT',
    'first row link: ' + (firstLink && firstLink.getAttribute('href')));

  // Re-render updates data.
  lb.update([{symbol: 'NVDA', horizon: 5, accuracy: 0.9, pnl: 1000}]);
  assert('update() replaces rows', host.querySelectorAll('tbody tr').length === 1);

  var summary = document.createElement('div');
  summary.style.marginTop = '1em';
  summary.textContent = 'Results: ' + passed + ' passed, ' + failed + ' failed';
  results.appendChild(summary);
  document.title = failed === 0 ? 'PASS — Leaderboard tests' : 'FAIL — Leaderboard tests';
})();
</script>
</body>
</html>
```

- [ ] **Step 2: Implement leaderboard primitive**

Create `bullforge/services/dashboard_service/static/components/leaderboard.js`:

```javascript
// components/leaderboard.js
// Sortable, clickable leaderboard table primitive.
//
// Public API:
//   var lb = Leaderboard.create({
//     mountEl: host,
//     columns: [{key, label, sortable, render(value, row)}],
//     rows: [...],
//     getRowHref: function (row) { return '#models/' + row.id; },
//     initialSort: {key: 'accuracy', order: 'desc'},
//   });
//   lb.update(newRows);
(function (global) {
  function _compare(a, b, key, order) {
    var av = a[key], bv = b[key];
    if (av === bv) return 0;
    if (av === null || av === undefined) return 1;
    if (bv === null || bv === undefined) return -1;
    if (typeof av === 'number' && typeof bv === 'number') {
      return order === 'asc' ? av - bv : bv - av;
    }
    var as = String(av).toLowerCase(), bs = String(bv).toLowerCase();
    if (as < bs) return order === 'asc' ? -1 : 1;
    if (as > bs) return order === 'asc' ? 1 : -1;
    return 0;
  }

  function _escape(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
    });
  }

  function create(opts) {
    if (!opts || !opts.mountEl) throw new Error('Leaderboard.create requires mountEl');
    var mountEl = opts.mountEl;
    var columns = opts.columns || [];
    var rows = (opts.rows || []).slice();
    var getRowHref = opts.getRowHref || null;
    var sort = opts.initialSort || null;

    function render() {
      var sortedRows = rows.slice();
      if (sort) {
        sortedRows.sort(function (a, b) { return _compare(a, b, sort.key, sort.order); });
      }

      var thead = '<thead><tr>' + columns.map(function (c) {
        var arrow = '';
        if (sort && sort.key === c.key) arrow = sort.order === 'desc' ? ' ▼' : ' ▲';
        var cls = c.sortable ? ' class="sortable"' : '';
        var attr = c.sortable ? ' data-sort-key="' + _escape(c.key) + '"' : '';
        return '<th' + cls + attr + '>' + _escape(c.label) + arrow + '</th>';
      }).join('') + '</tr></thead>';

      var tbody = '<tbody>' + sortedRows.map(function (row) {
        var href = getRowHref ? getRowHref(row) : null;
        return '<tr>' + columns.map(function (c, ci) {
          var v = row[c.key];
          var rendered = c.render ? c.render(v, row) : (v == null ? '' : _escape(v));
          if (href && ci === 0) {
            return '<td><a href="' + _escape(href) + '">' + rendered + '</a></td>';
          }
          return '<td>' + rendered + '</td>';
        }).join('') + '</tr>';
      }).join('') + '</tbody>';

      mountEl.innerHTML = '<table class="data-table leaderboard-table">' + thead + tbody + '</table>';

      // Wire up sort clicks.
      mountEl.querySelectorAll('thead th.sortable').forEach(function (th) {
        th.style.cursor = 'pointer';
        th.addEventListener('click', function () {
          var key = th.getAttribute('data-sort-key');
          if (sort && sort.key === key) {
            sort = {key: key, order: sort.order === 'desc' ? 'asc' : 'desc'};
          } else {
            sort = {key: key, order: 'desc'};
          }
          render();
        });
      });
    }

    render();

    return {
      update: function (newRows) {
        rows = newRows.slice();
        render();
      },
      setSort: function (newSort) { sort = newSort; render(); },
      getSort: function () { return sort; },
    };
  }

  global.Leaderboard = {create: create};
})(window);
```

- [ ] **Step 3: Run test harness**

In a browser, open `http://localhost:18084/static/test/leaderboard.html`.

Expected: All PASS; title `PASS — Leaderboard tests`.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/dashboard_service/static/components/leaderboard.js \
        bullforge/services/dashboard_service/static/test/leaderboard.html
git commit -m "feat(dashboard): add Leaderboard sortable-table primitive"
```

---

## Task 9: Drill-down frame

**Files:**
- Create: `bullforge/services/dashboard_service/static/components/drilldown.js`
- Create: `bullforge/services/dashboard_service/static/test/drilldown.html`

- [ ] **Step 1: Write the failing test harness**

Create `bullforge/services/dashboard_service/static/test/drilldown.html`:

```html
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>DrillDown tests</title>
<style>
body { font-family: monospace; background: #1a1a2e; color: #e0e0e0; }
.pass { color: #2ecc71; } .fail { color: #e74c3c; }
#host { background: #16213e; padding: 1em; margin: 1em 0; }
</style>
</head>
<body>
<h1>DrillDown tests</h1>
<div id="host"></div>
<div id="results"></div>
<script src="/static/components/drilldown.js"></script>
<script>
(function () {
  var host = document.getElementById('host');
  var results = document.getElementById('results');
  var passed = 0, failed = 0;
  function assert(name, cond, detail) {
    var ok = !!cond;
    var div = document.createElement('div');
    div.className = ok ? 'pass' : 'fail';
    div.style.whiteSpace = 'pre';
    div.textContent = (ok ? 'PASS' : 'FAIL') + ' — ' + name + (ok || !detail ? '' : '\n  ' + detail);
    results.appendChild(div);
    if (ok) passed++; else failed++;
  }

  var dd = window.DrillDown.create({
    mountEl: host,
    title: 'Model #42 — AAPL · 1m · 5m',
    backHref: '#models/leaderboard',
    breadcrumbs: [
      {label: 'Models', href: '#models'},
      {label: 'Leaderboard', href: '#models/leaderboard'},
      {label: 'AAPL · 1m · 5m'},
    ],
  });

  assert('renders back button', host.querySelector('.dd-back') !== null);
  assert('renders title', host.querySelector('.dd-title').textContent.indexOf('AAPL') >= 0);
  assert('renders breadcrumbs',
    host.querySelectorAll('.dd-breadcrumbs a').length === 2);
  assert('last breadcrumb has no link',
    host.querySelectorAll('.dd-breadcrumbs > *').length === 3 &&
    host.querySelectorAll('.dd-breadcrumbs a').length === 2);

  var contentSlot = dd.getContentSlot();
  contentSlot.innerHTML = '<p>hello world</p>';
  assert('getContentSlot returns writable element',
    contentSlot.innerHTML === '<p>hello world</p>');
  assert('contentSlot is inside mountEl',
    host.contains(contentSlot));

  // Update title.
  dd.setTitle('New title');
  assert('setTitle updates DOM',
    host.querySelector('.dd-title').textContent === 'New title');

  var summary = document.createElement('div');
  summary.style.marginTop = '1em';
  summary.textContent = 'Results: ' + passed + ' passed, ' + failed + ' failed';
  results.appendChild(summary);
  document.title = failed === 0 ? 'PASS — DrillDown tests' : 'FAIL — DrillDown tests';
})();
</script>
</body>
</html>
```

- [ ] **Step 2: Implement drilldown component**

Create `bullforge/services/dashboard_service/static/components/drilldown.js`:

```javascript
// components/drilldown.js
// Drill-down page frame: back button, title, breadcrumbs, content slot.
//
// Public API:
//   var dd = DrillDown.create({
//     mountEl: host,
//     title: 'Model #42',
//     backHref: '#models/leaderboard',
//     breadcrumbs: [{label, href?}, ...],
//   });
//   dd.setTitle(newTitle);
//   dd.getContentSlot();  // returns <div> for caller to populate
(function (global) {
  function _escape(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
    });
  }

  function create(opts) {
    if (!opts || !opts.mountEl) throw new Error('DrillDown.create requires mountEl');
    var mountEl = opts.mountEl;
    var title = opts.title || '';
    var backHref = opts.backHref || '#';
    var breadcrumbs = opts.breadcrumbs || [];

    mountEl.innerHTML = '';

    var header = document.createElement('div');
    header.className = 'dd-header';
    header.style.display = 'flex';
    header.style.alignItems = 'center';
    header.style.gap = '1em';
    header.style.marginBottom = '1em';

    var back = document.createElement('a');
    back.className = 'dd-back';
    back.href = backHref;
    back.textContent = '← Back';
    back.style.color = '#00b4d8';
    back.style.textDecoration = 'none';
    header.appendChild(back);

    var titleEl = document.createElement('h2');
    titleEl.className = 'dd-title';
    titleEl.style.margin = '0';
    titleEl.textContent = title;
    header.appendChild(titleEl);

    mountEl.appendChild(header);

    var breadcrumbsEl = document.createElement('div');
    breadcrumbsEl.className = 'dd-breadcrumbs';
    breadcrumbsEl.style.color = '#888';
    breadcrumbsEl.style.fontSize = '0.85rem';
    breadcrumbsEl.style.marginBottom = '1em';
    breadcrumbs.forEach(function (b, i) {
      if (i > 0) {
        var sep = document.createElement('span');
        sep.textContent = ' › ';
        breadcrumbsEl.appendChild(sep);
      }
      if (b.href) {
        var a = document.createElement('a');
        a.href = b.href;
        a.textContent = b.label;
        a.style.color = '#888';
        breadcrumbsEl.appendChild(a);
      } else {
        var span = document.createElement('span');
        span.textContent = b.label;
        span.style.color = '#e0e0e0';
        breadcrumbsEl.appendChild(span);
      }
    });
    mountEl.appendChild(breadcrumbsEl);

    var content = document.createElement('div');
    content.className = 'dd-content';
    mountEl.appendChild(content);

    return {
      setTitle: function (newTitle) {
        title = newTitle;
        titleEl.textContent = newTitle;
      },
      getContentSlot: function () { return content; },
    };
  }

  global.DrillDown = {create: create};
})(window);
```

- [ ] **Step 3: Run test harness**

In a browser, open `http://localhost:18084/static/test/drilldown.html`.

Expected: All PASS; title `PASS — DrillDown tests`.

- [ ] **Step 4: Commit**

```bash
git add bullforge/services/dashboard_service/static/components/drilldown.js \
        bullforge/services/dashboard_service/static/test/drilldown.html
git commit -m "feat(dashboard): add DrillDown frame with back/title/breadcrumbs"
```

---

## Task 10: Include components from index.html

**Files:**
- Modify: `bullforge/services/dashboard_service/static/index.html`

- [ ] **Step 1: Load components before dashboard.js**

In `bullforge/services/dashboard_service/static/index.html`, locate the block (around line 227):

```html
    <script src="/static/dashboard.js"></script>
    <link rel="stylesheet" href="/static/backtests.css">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
    <script src="/static/backtests.js?v=2"></script>
```

Replace it with (Chart.js loaded BEFORE charts.js because charts.js depends on it):

```html
    <link rel="stylesheet" href="/static/backtests.css">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
    <script src="/static/components/router.js"></script>
    <script src="/static/components/timewindow.js"></script>
    <script src="/static/components/refresh.js"></script>
    <script src="/static/components/charts.js"></script>
    <script src="/static/components/leaderboard.js"></script>
    <script src="/static/components/drilldown.js"></script>
    <script src="/static/dashboard.js"></script>
    <script src="/static/backtests.js?v=2"></script>
```

- [ ] **Step 2: Reload the dashboard manually**

With the override mount set up, reload `http://localhost:18084/` in a browser with DevTools console open.

Expected:
- Existing tabs (Overview, Trading, Models, ...) render exactly as before.
- Console shows no errors from the new component scripts.
- `window.Router`, `window.TimeWindow`, `window.Refresh`, `window.Charts`, `window.Leaderboard`, `window.DrillDown` are all defined (`typeof window.Router === 'object'`, etc.).
- All existing functionality (tab switching, refresh every 30s, backtests table) still works.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/index.html
git commit -m "feat(dashboard): load foundation components before dashboard.js"
```

---

## Task 11: End-to-end smoke test + verification

**Files:** none modified — verification only.

- [ ] **Step 1: Full backend test suite**

Run: `cd bullforge && python -m pytest tests/test_time_window.py tests/test_dashboard_api.py tests/test_dashboard_stubs.py tests/test_dashboard_backtests_api.py tests/test_dashboard_db.py -v`

Expected: All tests PASS. No regressions in the existing dashboard API or DB tests.

- [ ] **Step 2: Full broader backend suite sanity check**

Run: `cd bullforge && python -m pytest tests/ -q --ignore=tests/integration 2>&1 | tail -5`

Expected: All unit tests PASS (integration tests excluded because they require running TimescaleDB; those continue to pass when DB is available).

- [ ] **Step 3: Each in-browser component harness passes**

Open in a browser, one at a time:
- `http://localhost:18084/static/test/router.html`
- `http://localhost:18084/static/test/timewindow.html`
- `http://localhost:18084/static/test/refresh.html`
- `http://localhost:18084/static/test/charts.html`
- `http://localhost:18084/static/test/leaderboard.html`
- `http://localhost:18084/static/test/drilldown.html`
- `http://localhost:18084/static/test/fmttime.html` (existing — regression check)

Expected: Every title reads `PASS — ... tests`. No red FAIL lines. No DevTools console errors.

- [ ] **Step 4: Manual dashboard regression check**

Open `http://localhost:18084/` and click through each tab (Overview, Trading, Models, Trades, Orders, Halts, Symbols, Freshness, Config, Backtests).

Expected:
- Every tab renders without error.
- Numbers and tables populate as before.
- TZ badge appears in header.
- Refresh still fires every 30 seconds (watch `last-updated`).
- No DevTools console errors from `components/*.js`.
- Stub endpoints return 501 when probed:
  - `curl http://localhost:18084/api/models/1/calibration` → status 501, body `{"error":"not_implemented","enabled_after_plan":"plan_2"}`
  - `curl http://localhost:18084/api/trading/equity` → status 501, body `{"error":"not_implemented","enabled_after_plan":"plan_3"}`
  - `curl http://localhost:18084/api/backtests/1/folds` → status 501, body `{"error":"not_implemented","enabled_after_plan":"plan_4"}`

- [ ] **Step 5: Time-window parameter works on existing endpoint**

Run:
```bash
curl -s 'http://localhost:18084/api/metrics/history?category=trading&from=2026-04-01T00:00:00Z&to=2026-04-17T00:00:00Z' | head -c 200
```

Expected: HTTP 200, JSON array (possibly empty). Not 400, not 500.

Run:
```bash
curl -s -o /dev/null -w '%{http_code}\n' 'http://localhost:18084/api/metrics/history?from=garbage'
```

Expected: `400`.

- [ ] **Step 6: Commit verification notes (if any manual fixes were needed)**

If Steps 1-5 all pass with no fixes, skip this step.

If any fixes were required, add them under a new commit:

```bash
git add <modified files>
git commit -m "fix(dashboard): address smoke-test findings in foundation"
```

---

## Self-Review Summary

**Spec coverage check** (against [2026-04-17-dashboard-expansion-design.md](../specs/2026-04-17-dashboard-expansion-design.md) — Plan 1 scope):

| Spec item | Task |
|-----------|------|
| Router component | Task 4 |
| Time-window picker | Task 5 |
| Refresh control | Task 6 |
| Chart wrappers (line, bar, histogram, scatter, sparkline, heatmap) | Task 7 |
| Leaderboard primitive | Task 8 |
| Drill-down frame | Task 9 |
| `?from/?to` support on existing `/api/metrics/*` | Tasks 1-2 (helper + wiring) |
| All 21 Plan 2-4 endpoint stubs returning HTTP 501 | Task 3 |
| Component test harnesses extending `fmttime.html` pattern | Tasks 4-9 |
| Plan 1 success criteria: no user-visible tab changes, existing behavior unchanged | Task 11 manual regression |
| Forward-compatibility with backtest run/fold scoping dependency | Plan 1 scope excludes tables the dependency touches; note on first page |

**Placeholder scan:** No TBD/TODO. Every step has concrete code, file paths, and expected output.

**Type consistency:** All components expose `window.<Name>` globals with consistent `create(opts)` entry points. `onChange`/`onRefresh` callbacks have stable shapes. `storageKey`/`mountEl` naming consistent across timewindow.js and refresh.js. DB method `query_metric_snapshots` preserves positional signature; new args are keyword-only.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-17-dashboard-foundation.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
