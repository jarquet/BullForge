# Backtest Multi-Delete Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add checkbox multi-select + Delete to the dashboard Backtests list. Deleting a run removes its `backtest_runs` row (cascading folds/trades/equity) and its non-cascaded `backtest.*` sandbox-schema rows keyed by `backtest_run_id`. Running runs must be cancelled before delete.

**Architecture:** Single shared server helper `_delete_run_impl` runs one transaction per run: `DELETE FROM backtest.{orders,predictions,model_registry} WHERE backtest_run_id=$1`, then `DELETE FROM backtest_runs WHERE id=$1 AND status IN ('completed','failed','cancelled')`. Two aiohttp routes call it: `DELETE /api/backtests/{id}` (primitive) and `POST /api/backtests/delete` (bulk with per-id result buckets). Frontend adds a checkbox column with Shift+click range and a fixed action bar; dispatches the primitive endpoint in parallel when ≤10 are selected and the bulk endpoint when >10.

**Tech Stack:** Python 3.12, aiohttp, asyncpg, Postgres/TimescaleDB, vanilla JS/CSS. No DB migration.

**Spec:** [docs/superpowers/specs/2026-04-19-backtest-multi-delete-design.md](../specs/2026-04-19-backtest-multi-delete-design.md)

---

## Pre-Task: Worktree

The execution skill creates `.worktrees/backtest-multi-delete` branched from master as `feat/backtest-multi-delete`. All paths below are relative to that worktree.

---

## File Map

| Path | Role |
|---|---|
| `bullforge/services/dashboard_service/backtests_api.py` | Add `_delete_run_impl` helper, `DELETE /api/backtests/{id}`, `POST /api/backtests/delete`. Register routes with correct ordering (literal `/delete` before `{id}`). |
| `bullforge/tests/test_backtest_delete.py` | New file — real-DB tests for helper + both routes. Lives outside `integration/` so the function-scoped `db` fixture (same pattern as `test_backtest_jobs_db.py`) composes cleanly with `aiohttp_client`. |
| `bullforge/services/dashboard_service/static/backtests.js` | Checkbox column + selection state + Shift+click + header select-all + action bar + confirm + delete dispatch. |
| `bullforge/services/dashboard_service/static/backtests.css` | Styles for checkbox column, disabled-checkbox cursor, action bar. |

---

## Task 1: Backend helper `_delete_run_impl` + real-DB tests

**Files:**
- Create: `bullforge/tests/test_backtest_delete.py`
- Modify: `bullforge/services/dashboard_service/backtests_api.py` (add helper, does NOT register routes yet)

- [ ] **Step 1: Write the failing helper tests**

Create `bullforge/tests/test_backtest_delete.py` with EXACTLY:

```python
"""Real-DB tests for the multi-delete backtest helper and routes.

Uses a function-scoped `db` fixture (same pattern as
`test_backtest_jobs_db.py`) so it composes cleanly with `aiohttp_client`.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from shared.config import Settings
from shared.db import Database

from services.dashboard_service.backtests_api import _delete_run_impl


UTC = timezone.utc
pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def db():
    """Function-scoped real DB.

    Cleanup is scoped to rows this file creates (name='del-test' for runs,
    name='linked-job' for jobs, note='test promote' for audit) so running
    these tests against a dev database does not wipe unrelated data. We do
    NOT use TRUNCATE CASCADE here because runtime_config_audit is
    FK-linked to backtest_runs and CASCADE would wipe the whole audit log.
    """
    d = Database(Settings())
    await d.connect()

    async def _cleanup(conn):
        await conn.execute(
            "DELETE FROM backtest.orders WHERE backtest_run_id IN "
            "(SELECT id FROM backtest_runs WHERE name = 'del-test')")
        await conn.execute(
            "DELETE FROM backtest.predictions WHERE backtest_run_id IN "
            "(SELECT id FROM backtest_runs WHERE name = 'del-test')")
        await conn.execute(
            "DELETE FROM backtest.model_registry WHERE backtest_run_id IN "
            "(SELECT id FROM backtest_runs WHERE name = 'del-test')")
        await conn.execute(
            "DELETE FROM backtest_jobs WHERE name = 'linked-job'")
        # ON DELETE CASCADE removes folds/trades/equity; SET NULL on
        # backtest_jobs.run_id and runtime_config_audit.source_backtest_run_id
        # are handled by FK definitions.
        await conn.execute(
            "DELETE FROM backtest_runs WHERE name = 'del-test'")
        await conn.execute(
            "DELETE FROM runtime_config_audit WHERE note = 'test promote'")

    async with d.pool.acquire() as conn:
        await _cleanup(conn)
    yield d
    async with d.pool.acquire() as conn:
        await _cleanup(conn)
    await d.disconnect()


async def _insert_run(db, status: str = "completed") -> int:
    """Insert a backtest_runs row with the given status; return id."""
    async with db.pool.acquire() as conn:
        return await conn.fetchval(
            """
            INSERT INTO backtest_runs (name, symbols, timeframe, horizons,
                range_start, range_end, train_days, test_days, embargo_min,
                mode, config_json, status)
            VALUES ('del-test', ARRAY['AAPL'], '1m', ARRAY[5],
                NOW(), NOW() + INTERVAL '1 day', 30, 5, 30,
                'rolling', '{}'::jsonb, $1)
            RETURNING id
            """,
            status,
        )


async def _insert_cascaded_artifacts(db, run_id: int) -> int:
    """Insert one fold + one trade + one equity row. Returns fold_id."""
    async with db.pool.acquire() as conn:
        fold_id = await conn.fetchval(
            """
            INSERT INTO backtest_folds (run_id, fold_index,
                train_start, train_end, test_start, test_end)
            VALUES ($1, 0, NOW(), NOW() + INTERVAL '1 hour',
                    NOW() + INTERVAL '1 hour', NOW() + INTERVAL '2 hours')
            RETURNING id
            """,
            run_id,
        )
        await conn.execute(
            """
            INSERT INTO backtest_trades (run_id, fold_id, symbol, side,
                entry_time, entry_price, quantity, signal_strength)
            VALUES ($1, $2, 'AAPL', 'long', NOW(), 100.0, 1.0, 0.7)
            """,
            run_id, fold_id,
        )
        await conn.execute(
            """
            INSERT INTO backtest_equity (run_id, fold_id, timestamp, equity, cash)
            VALUES ($1, $2, NOW(), 10000.0, 10000.0)
            """,
            run_id, fold_id,
        )
    return fold_id


async def _insert_sandbox_artifacts(db, run_id: int) -> None:
    """Insert one row each into backtest.{model_registry,predictions,orders}.

    Columns match the real schemas (see migrations 005/006/007 for the
    public counterparts, which backtest.* tables copy via LIKE INCLUDING ALL
    in migration 014, and 018 for the added backtest_run_id/fold_index).
    """
    async with db.pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO backtest.model_registry (
                symbol, timeframe, horizon_minutes, model_type,
                artifact_path, train_samples, test_samples, is_active,
                backtest_run_id, fold_index
            )
            VALUES ('AAPL', '1m', 5, 'gradient_boost',
                    '/tmp/fake.joblib', 1, 1, FALSE, $1, 0)
            """,
            run_id,
        )
        await conn.execute(
            """
            INSERT INTO backtest.predictions (
                symbol, timestamp, timeframe, horizon_minutes, model_id,
                predicted_direction, confidence,
                backtest_run_id, fold_index
            )
            VALUES ('AAPL', NOW(), '1m', 5, 1,
                    1, 0.7, $1, 0)
            """,
            run_id,
        )
        await conn.execute(
            """
            INSERT INTO backtest.orders (
                symbol, side, quantity, order_type, submitted_at,
                signal_strength, backtest_run_id, fold_index
            )
            VALUES ('AAPL', 'buy', 1.0, 'market', NOW(), 0.7, $1, 0)
            """,
            run_id,
        )


async def _count_artifacts(db, run_id: int) -> dict:
    """Return counts of all artifact tables scoped to this run_id."""
    async with db.pool.acquire() as conn:
        return {
            "run": await conn.fetchval(
                "SELECT COUNT(*) FROM backtest_runs WHERE id = $1", run_id),
            "folds": await conn.fetchval(
                "SELECT COUNT(*) FROM backtest_folds WHERE run_id = $1", run_id),
            "trades": await conn.fetchval(
                "SELECT COUNT(*) FROM backtest_trades WHERE run_id = $1", run_id),
            "equity": await conn.fetchval(
                "SELECT COUNT(*) FROM backtest_equity WHERE run_id = $1", run_id),
            "sandbox_models": await conn.fetchval(
                "SELECT COUNT(*) FROM backtest.model_registry "
                "WHERE backtest_run_id = $1", run_id),
            "sandbox_predictions": await conn.fetchval(
                "SELECT COUNT(*) FROM backtest.predictions "
                "WHERE backtest_run_id = $1", run_id),
            "sandbox_orders": await conn.fetchval(
                "SELECT COUNT(*) FROM backtest.orders "
                "WHERE backtest_run_id = $1", run_id),
        }


@pytest.mark.asyncio
async def test_delete_run_impl_deletes_completed_run_and_all_artifacts(db):
    run_id = await _insert_run(db, status="completed")
    await _insert_cascaded_artifacts(db, run_id)
    await _insert_sandbox_artifacts(db, run_id)

    before = await _count_artifacts(db, run_id)
    assert before == {
        "run": 1, "folds": 1, "trades": 1, "equity": 1,
        "sandbox_models": 1, "sandbox_predictions": 1, "sandbox_orders": 1,
    }

    result = await _delete_run_impl(db, run_id)
    assert result == {"status": "deleted", "run_id": run_id}

    after = await _count_artifacts(db, run_id)
    assert after == {
        "run": 0, "folds": 0, "trades": 0, "equity": 0,
        "sandbox_models": 0, "sandbox_predictions": 0, "sandbox_orders": 0,
    }


@pytest.mark.asyncio
async def test_delete_run_impl_refuses_running_run(db):
    run_id = await _insert_run(db, status="running")
    await _insert_cascaded_artifacts(db, run_id)
    await _insert_sandbox_artifacts(db, run_id)

    result = await _delete_run_impl(db, run_id)
    assert result == {
        "status": "still_running",
        "run_id": run_id,
        "current_status": "running",
    }

    # Nothing was deleted — sandbox rows must still be present.
    counts = await _count_artifacts(db, run_id)
    assert counts == {
        "run": 1, "folds": 1, "trades": 1, "equity": 1,
        "sandbox_models": 1, "sandbox_predictions": 1, "sandbox_orders": 1,
    }


@pytest.mark.asyncio
async def test_delete_run_impl_returns_not_found_for_missing_id(db):
    # Pick an id that definitely doesn't exist.
    async with db.pool.acquire() as conn:
        max_id = await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM backtest_runs")
    missing_id = max_id + 100_000

    result = await _delete_run_impl(db, missing_id)
    assert result == {"status": "not_found", "run_id": missing_id}


@pytest.mark.asyncio
async def test_delete_run_impl_nulls_backtest_jobs_run_id(db):
    run_id = await _insert_run(db, status="completed")
    async with db.pool.acquire() as conn:
        job_id = await conn.fetchval(
            """
            INSERT INTO backtest_jobs (name, config_json, status, run_id,
                worker_pool)
            VALUES ('linked-job', '{}'::jsonb, 'completed', $1, 'cpu')
            RETURNING id
            """,
            run_id,
        )

    result = await _delete_run_impl(db, run_id)
    assert result["status"] == "deleted"

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT run_id, status FROM backtest_jobs WHERE id = $1", job_id)
    assert row["run_id"] is None
    assert row["status"] == "completed"  # job row itself survives


@pytest.mark.asyncio
async def test_delete_run_impl_nulls_runtime_config_audit_link(db):
    run_id = await _insert_run(db, status="completed")
    async with db.pool.acquire() as conn:
        audit_id = await conn.fetchval(
            """
            INSERT INTO runtime_config_audit (key, new_value,
                source_backtest_run_id, note)
            VALUES ('execution_min_confidence', '0.65', $1, 'test promote')
            RETURNING id
            """,
            run_id,
        )

    result = await _delete_run_impl(db, run_id)
    assert result["status"] == "deleted"

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT source_backtest_run_id, new_value "
            "FROM runtime_config_audit WHERE id = $1",
            audit_id,
        )
    assert row["source_backtest_run_id"] is None
    assert row["new_value"] == "0.65"  # audit row survives with history intact
```

- [ ] **Step 2: Run the tests and verify they fail**

```bash
cd <repo>/.worktrees/backtest-multi-delete/bullforge
pytest tests/test_backtest_delete.py -v
```

Expected: ImportError — `_delete_run_impl` does not yet exist in `backtests_api.py`. All 5 tests collected-but-error.

- [ ] **Step 3: Add the helper to `backtests_api.py`**

Insert this function in `bullforge/services/dashboard_service/backtests_api.py` IMMEDIATELY AFTER the `_iso` helper at line 55-56 (so it sits with the other top-level helpers, above `_cancel_job`):

```python
async def _delete_run_impl(db, run_id: int) -> dict:
    """Delete a terminal backtest run and its non-cascaded sandbox artifacts.

    Returns one of:
      {"status": "deleted",       "run_id": run_id}
      {"status": "not_found",     "run_id": run_id}
      {"status": "still_running", "run_id": run_id,
                                  "current_status": <str>}

    One transaction per run. If the final status-guarded DELETE hits zero
    rows (run is running), the sandbox deletes roll back.
    """
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM backtest.orders WHERE backtest_run_id = $1",
                run_id,
            )
            await conn.execute(
                "DELETE FROM backtest.predictions WHERE backtest_run_id = $1",
                run_id,
            )
            await conn.execute(
                "DELETE FROM backtest.model_registry WHERE backtest_run_id = $1",
                run_id,
            )
            row = await conn.fetchrow(
                """
                DELETE FROM backtest_runs
                WHERE id = $1
                  AND status IN ('completed', 'failed', 'cancelled')
                RETURNING id
                """,
                run_id,
            )
    if row is not None:
        return {"status": "deleted", "run_id": run_id}

    # Distinguish not-found from still-running. Only runs when the guarded
    # DELETE above returned zero rows, so the extra SELECT is rare.
    async with db.pool.acquire() as conn:
        status = await conn.fetchval(
            "SELECT status FROM backtest_runs WHERE id = $1", run_id,
        )
    if status is None:
        return {"status": "not_found", "run_id": run_id}
    return {
        "status": "still_running",
        "run_id": run_id,
        "current_status": status,
    }
```

- [ ] **Step 4: Run the tests and verify they pass**

```bash
cd <repo>/.worktrees/backtest-multi-delete/bullforge
pytest tests/test_backtest_delete.py -v
```

Expected: all 5 tests pass.

- [ ] **Step 5: Commit**

```bash
cd <repo>/.worktrees/backtest-multi-delete
git add bullforge/services/dashboard_service/backtests_api.py \
        bullforge/tests/test_backtest_delete.py
git commit -m "feat(dashboard): add _delete_run_impl helper for backtest run deletion"
```

---

## Task 2: `DELETE /api/backtests/{id}` route + tests

**Files:**
- Modify: `bullforge/services/dashboard_service/backtests_api.py` (add handler + register route)
- Modify: `bullforge/tests/test_backtest_delete.py` (add route tests)

- [ ] **Step 1: Write failing route tests**

Append to the END of `bullforge/tests/test_backtest_delete.py`:

```python
# ---------------------------------------------------------------------------
# DELETE /api/backtests/{id} route tests (real-DB via aiohttp client).
# ---------------------------------------------------------------------------

import pytest_asyncio
from aiohttp import web

from services.dashboard_service.backtests_api import register_backtests_routes
from services.backtest_service.jobs_db import BacktestJobsDB
from services.backtest_service.results_db import BacktestResultsDB


@pytest_asyncio.fixture
async def delete_client(aiohttp_client, db):
    app = web.Application()
    app["db"] = db
    app["results_db"] = BacktestResultsDB(db)
    app["jobs_db"] = BacktestJobsDB(db)
    register_backtests_routes(app)
    return await aiohttp_client(app)


@pytest.mark.asyncio
async def test_delete_route_200_on_terminal_run(delete_client, db):
    run_id = await _insert_run(db, status="completed")
    await _insert_sandbox_artifacts(db, run_id)

    resp = await delete_client.delete(f"/api/backtests/{run_id}")
    assert resp.status == 200
    body = await resp.json()
    assert body == {"status": "deleted", "run_id": run_id}

    counts = await _count_artifacts(db, run_id)
    assert counts["run"] == 0
    assert counts["sandbox_models"] == 0


@pytest.mark.asyncio
async def test_delete_route_409_on_running_run(delete_client, db):
    run_id = await _insert_run(db, status="running")

    resp = await delete_client.delete(f"/api/backtests/{run_id}")
    assert resp.status == 409
    body = await resp.json()
    assert body["run_id"] == run_id
    assert body["current_status"] == "running"

    # Still there.
    counts = await _count_artifacts(db, run_id)
    assert counts["run"] == 1


@pytest.mark.asyncio
async def test_delete_route_404_on_missing_id(delete_client, db):
    async with db.pool.acquire() as conn:
        max_id = await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM backtest_runs")
    missing = max_id + 100_000

    resp = await delete_client.delete(f"/api/backtests/{missing}")
    assert resp.status == 404
    body = await resp.json()
    assert body["run_id"] == missing


@pytest.mark.asyncio
async def test_delete_route_400_on_non_integer_id(delete_client):
    resp = await delete_client.delete("/api/backtests/not-an-int")
    assert resp.status == 400
```

- [ ] **Step 2: Run and verify tests fail**

```bash
cd <repo>/.worktrees/backtest-multi-delete/bullforge
pytest tests/test_backtest_delete.py -v -k "delete_route"
```

Expected: 4 tests fail with 404 or 405 (route not yet registered).

- [ ] **Step 3: Add the route handler**

Insert this function in `bullforge/services/dashboard_service/backtests_api.py` IMMEDIATELY AFTER `_delete_run_impl` (i.e., right before the existing `_cancel_job` at line ~59 of the pre-change file):

```python
async def _delete_run(request: web.Request) -> web.Response:
    """DELETE /api/backtests/{id}."""
    try:
        run_id = int(request.match_info["id"])
    except (KeyError, ValueError):
        return web.json_response(
            {"error": "run_id must be an integer"}, status=400,
        )
    db = request.app["db"]
    result = await _delete_run_impl(db, run_id)
    status = result["status"]
    if status == "deleted":
        return web.json_response(result, status=200)
    if status == "not_found":
        return web.json_response(
            {"error": "not found", "run_id": run_id}, status=404,
        )
    # still_running
    return web.json_response(
        {
            "error": "run is still running",
            "run_id": run_id,
            "current_status": result["current_status"],
        },
        status=409,
    )
```

- [ ] **Step 4: Register the route**

In `register_backtests_routes` in `backtests_api.py` (currently at lines 129-138), add the new `DELETE` route. Place it AFTER the existing `add_get("/api/backtests/{id}", _detail)` so registration order stays consistent:

```python
def register_backtests_routes(app: web.Application) -> None:
    app.router.add_get("/api/backtests", _list)
    app.router.add_post("/api/backtests", _create)
    app.router.add_get("/api/backtests/{id}", _detail)
    app.router.add_delete("/api/backtests/{id}", _delete_run)
    app.router.add_get("/api/backtests/{id}/folds", _folds)
    app.router.add_get("/api/backtests/{id}/equity", _equity)
    app.router.add_get("/api/backtests/{id}/trades", _trades)
    app.router.add_post("/api/backtests/{id}/cancel", _cancel_run)
    app.router.add_post("/api/backtest-jobs/{job_id}/cancel", _cancel_job)
```

- [ ] **Step 5: Run the tests and verify they pass**

```bash
cd <repo>/.worktrees/backtest-multi-delete/bullforge
pytest tests/test_backtest_delete.py -v
```

Expected: all 9 tests pass (5 helper + 4 route).

- [ ] **Step 6: Commit**

```bash
cd <repo>/.worktrees/backtest-multi-delete
git add bullforge/services/dashboard_service/backtests_api.py \
        bullforge/tests/test_backtest_delete.py
git commit -m "feat(dashboard): add DELETE /api/backtests/{id} route"
```

---

## Task 3: `POST /api/backtests/delete` bulk route + tests

**Files:**
- Modify: `bullforge/services/dashboard_service/backtests_api.py`
- Modify: `bullforge/tests/test_backtest_delete.py`

- [ ] **Step 1: Write failing bulk-route tests**

Append to the END of `bullforge/tests/test_backtest_delete.py`:

```python
# ---------------------------------------------------------------------------
# POST /api/backtests/delete (bulk) route tests.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bulk_delete_mixed_outcomes(delete_client, db):
    done_id = await _insert_run(db, status="completed")
    failed_id = await _insert_run(db, status="failed")
    running_id = await _insert_run(db, status="running")
    async with db.pool.acquire() as conn:
        max_id = await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM backtest_runs")
    missing_id = max_id + 100_000

    resp = await delete_client.post(
        "/api/backtests/delete",
        json={"ids": [done_id, failed_id, running_id, missing_id]},
    )
    assert resp.status == 200
    body = await resp.json()
    assert set(body["deleted"]) == {done_id, failed_id}
    assert body["not_found"] == [missing_id]
    assert body["still_running"] == [
        {"run_id": running_id, "current_status": "running"}
    ]


@pytest.mark.asyncio
async def test_bulk_delete_dedupes_ids(delete_client, db):
    run_id = await _insert_run(db, status="completed")

    resp = await delete_client.post(
        "/api/backtests/delete",
        json={"ids": [run_id, run_id, run_id]},
    )
    assert resp.status == 200
    body = await resp.json()
    # Deduped: appears exactly once in deleted, nowhere else.
    assert body["deleted"] == [run_id]
    assert body["not_found"] == []
    assert body["still_running"] == []


@pytest.mark.asyncio
async def test_bulk_delete_empty_ids_rejected(delete_client):
    resp = await delete_client.post("/api/backtests/delete", json={"ids": []})
    assert resp.status == 400


@pytest.mark.asyncio
async def test_bulk_delete_too_many_ids_rejected(delete_client):
    resp = await delete_client.post(
        "/api/backtests/delete", json={"ids": list(range(1, 502))},
    )
    assert resp.status == 400


@pytest.mark.asyncio
async def test_bulk_delete_non_integer_ids_rejected(delete_client):
    resp = await delete_client.post(
        "/api/backtests/delete", json={"ids": [1, "two", 3]},
    )
    assert resp.status == 400


@pytest.mark.asyncio
async def test_bulk_delete_bad_json_rejected(delete_client):
    resp = await delete_client.post(
        "/api/backtests/delete",
        data="not json at all",
        headers={"content-type": "application/json"},
    )
    assert resp.status == 400


@pytest.mark.asyncio
async def test_bulk_delete_missing_ids_key_rejected(delete_client):
    resp = await delete_client.post("/api/backtests/delete", json={"wrong": [1]})
    assert resp.status == 400
```

- [ ] **Step 2: Run and verify they fail**

```bash
cd <repo>/.worktrees/backtest-multi-delete/bullforge
pytest tests/test_backtest_delete.py -v -k "bulk_delete"
```

Expected: 7 tests fail with 404 (route not registered).

- [ ] **Step 3: Add the bulk route handler**

Insert in `bullforge/services/dashboard_service/backtests_api.py` IMMEDIATELY AFTER `_delete_run` (the handler from Task 2):

```python
_BULK_DELETE_MAX = 500


async def _bulk_delete_runs(request: web.Request) -> web.Response:
    """POST /api/backtests/delete  — body: {"ids": [int, ...]}.

    Iterates _delete_run_impl per id, grouping results into buckets. Each
    id's deletion is independent; one still-running or missing id does not
    block the others.
    """
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "bad json"}, status=400)
    if not isinstance(body, dict):
        return web.json_response({"error": "body must be an object"}, status=400)

    ids = body.get("ids")
    if not isinstance(ids, list) or not ids:
        return web.json_response(
            {"error": "ids must be a non-empty list"}, status=400,
        )
    if not all(isinstance(i, int) and not isinstance(i, bool) for i in ids):
        return web.json_response(
            {"error": "every id must be an integer"}, status=400,
        )
    if len(ids) > _BULK_DELETE_MAX:
        return web.json_response(
            {"error": f"too many ids (max {_BULK_DELETE_MAX})"}, status=400,
        )

    # Dedupe while preserving first-occurrence order.
    seen = set()
    unique_ids: list[int] = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            unique_ids.append(i)

    db = request.app["db"]
    deleted: list[int] = []
    not_found: list[int] = []
    still_running: list[dict] = []
    for run_id in unique_ids:
        res = await _delete_run_impl(db, run_id)
        if res["status"] == "deleted":
            deleted.append(run_id)
        elif res["status"] == "not_found":
            not_found.append(run_id)
        else:  # still_running
            still_running.append(
                {"run_id": run_id, "current_status": res["current_status"]},
            )
    return web.json_response({
        "deleted": deleted,
        "not_found": not_found,
        "still_running": still_running,
    })
```

- [ ] **Step 4: Register the bulk route before `{id}` routes**

aiohttp matches routes in registration order. The literal `/delete` path must be registered BEFORE `/api/backtests/{id}` so aiohttp doesn't try to parse `"delete"` as an id on unrelated matchers. Update `register_backtests_routes` in `backtests_api.py` to the following final form:

```python
def register_backtests_routes(app: web.Application) -> None:
    app.router.add_get("/api/backtests", _list)
    app.router.add_post("/api/backtests", _create)
    # Literal-path routes BEFORE parameterized /{id} routes.
    app.router.add_post("/api/backtests/delete", _bulk_delete_runs)
    app.router.add_get("/api/backtests/{id}", _detail)
    app.router.add_delete("/api/backtests/{id}", _delete_run)
    app.router.add_get("/api/backtests/{id}/folds", _folds)
    app.router.add_get("/api/backtests/{id}/equity", _equity)
    app.router.add_get("/api/backtests/{id}/trades", _trades)
    app.router.add_post("/api/backtests/{id}/cancel", _cancel_run)
    app.router.add_post("/api/backtest-jobs/{job_id}/cancel", _cancel_job)
```

- [ ] **Step 5: Run the full test file**

```bash
cd <repo>/.worktrees/backtest-multi-delete/bullforge
pytest tests/test_backtest_delete.py -v
```

Expected: all 16 tests pass (5 helper + 4 single-route + 7 bulk).

- [ ] **Step 6: Commit**

```bash
cd <repo>/.worktrees/backtest-multi-delete
git add bullforge/services/dashboard_service/backtests_api.py \
        bullforge/tests/test_backtest_delete.py
git commit -m "feat(dashboard): add POST /api/backtests/delete bulk route"
```

---

## Task 4: Frontend — checkbox column + selection state (no range, no bar)

**Files:**
- Modify: `bullforge/services/dashboard_service/static/backtests.js`
- Modify: `bullforge/services/dashboard_service/static/backtests.css`

This task introduces the column and disabled rules but NOT range-select or the action bar. A user seeing the page mid-deploy should see a working checkbox column with no deletion affordance yet. That keeps the JS change small and reviewable.

- [ ] **Step 1: Add the checkbox column's `<th>` and update the table header**

In `backtests.js` inside `renderOverview` (currently lines 75-114), update the `<thead>` so the first `<th>` is a checkbox cell and the `colspan` in the loading row matches. Replace the existing `<table>…</table>` block with:

```js
        <table class="bt-table">
          <thead><tr>
            <th class="bt-sel-col"><input type="checkbox" id="bt-select-all" title="Select all on this page"></th>
            <th data-sort="name">Name</th>
            <th>Symbols</th>
            <th>Range</th>
            <th data-sort="total_pnl">P&amp;L</th>
            <th data-sort="sharpe">Sharpe</th>
            <th data-sort="max_dd">Max DD</th>
            <th>#Trades</th>
            <th>Hit %</th>
            <th>Status</th>
            <th>Tags</th>
            <th data-sort="finished_at">Finished</th>
            <th>Runtime</th>
          </tr></thead>
          <tbody id="bt-rows"><tr><td colspan="13">Loading…</td></tr></tbody>
        </table>
```

(The colspan increments from 12 → 13 because we added a column.)

- [ ] **Step 2: Add module-scoped selection state**

Near the top of the IIFE in `backtests.js` (right after `const state = { ... }` around line 45), add:

```js
  // Multi-select state for the delete feature.
  // Keys are of the form "run:{id}". Jobs are never added (disabled).
  const selection = new Set();
  let lastCheckedBox = null;  // most recently clicked checkbox (for Shift-range, Task 5)

  function selectionKey(kind, id) { return kind + ':' + id; }
```

- [ ] **Step 3: Update `renderRows` to prepend a checkbox cell per row**

In `backtests.js` find `renderRows` (currently lines 364-422). Inside the `.map(r => { ... })` callback, just before the `return \`<tr class="clickable" ...>\`` statement, compute the checkbox HTML, and update the `<tr>` template to include the new cell FIRST.

Update the `renderRows` function's row template (the `return` statement) to read:

```js
      const selKey = selectionKey(r.kind, r.id);
      const isDisabled = r.kind === 'job'
        || (r.kind === 'run' && r.status === 'running');
      const isChecked = !isDisabled && selection.has(selKey);
      const boxTitle = isDisabled
        ? 'Cancel this backtest first'
        : 'Select for delete';
      const checkboxHtml =
        '<input type="checkbox" class="bt-row-select"' +
        ' data-sel-key="' + selKey + '"' +
        ' data-kind="' + r.kind + '"' +
        ' data-id="' + r.id + '"' +
        ' data-status="' + (r.status || '') + '"' +
        (isDisabled ? ' disabled' : '') +
        (isChecked ? ' checked' : '') +
        ' title="' + boxTitle + '"' +
        ' onclick="event.stopPropagation()"' +
        '>';
      return `<tr class="clickable" onclick="location.hash='${target}'">
        <td class="bt-sel-col" onclick="event.stopPropagation()">${checkboxHtml}</td>
        <td>${r.name}</td><td>${symsDisplay}</td><td>${range}</td>
        <td>${pnl}</td><td>${sharpe}</td><td>${dd}</td>
        <td>${ntr}</td><td>${hit}</td>
        <td>${statusCell} ${cancelBtn}</td>
        <td>${tagChips}</td>
        <td>${fmtTime(r.finished_at)}</td>
        <td>${runtime}</td>
      </tr>`;
```

Also update the empty-state `<tr>` at the top of `renderRows` (where it reads `colspan="12"`):

```js
      tbody.innerHTML =
        '<tr><td colspan="13">No backtests yet. Click New Backtest to start one.</td></tr>';
```

And the error row near the bottom of `reload()` (currently `colspan="10"` around line 152) — update to `colspan="13"` for consistency:

```js
      document.getElementById('bt-rows').innerHTML =
        `<tr><td colspan="13" class="bt-error">Failed to load: ${e.message}</td></tr>`;
```

- [ ] **Step 4: Wire per-row checkbox clicks and prune stale keys**

Still in `renderRows`, just after the existing `tbody.__btCancelDelegated` block (~line 409-421), add another delegated handler for the row-select checkboxes. Replace the existing cancel-delegation block with this extended block:

```js
    if (!tbody.__btCancelDelegated) {
      tbody.addEventListener('click', function (ev) {
        const btn = ev.target && ev.target.closest('.bt-cancel-btn');
        if (btn) {
          ev.stopPropagation();
          cancelBacktest({
            kind: btn.dataset.cancelKind,
            id: parseInt(btn.dataset.cancelId, 10),
            name: btn.dataset.cancelName || '',
          });
          return;
        }
        const box = ev.target && ev.target.matches('.bt-row-select') ? ev.target : null;
        if (box && !box.disabled) {
          const key = box.dataset.selKey;
          if (box.checked) selection.add(key);
          else selection.delete(key);
          lastCheckedBox = box;
          updateActionBar();  // defined in Task 6; safe-called as a no-op if absent
        }
      });
      tbody.__btCancelDelegated = true;
    }

    // Prune selection keys whose rows are no longer rendered. This keeps
    // the selection set bounded and handles rows that vanish between polls
    // (e.g., deleted by another tab).
    const visibleKeys = new Set(rows.map(r => selectionKey(r.kind, r.id)));
    for (const key of Array.from(selection)) {
      if (!visibleKeys.has(key)) selection.delete(key);
    }
```

- [ ] **Step 5: Stub `updateActionBar`**

At the TOP of the IIFE (near the `selection` declaration), add a stub that later tasks will replace with the real implementation. This prevents `ReferenceError` in this task:

```js
  // Stub until Task 6 wires the action bar. Real implementation updates
  // the 'Delete selected (N)' button visibility and count.
  function updateActionBar() { /* populated in Task 6 */ }
```

- [ ] **Step 6: Add CSS for the checkbox column**

Append to `bullforge/services/dashboard_service/static/backtests.css`:

```css
.bt-table th.bt-sel-col,
.bt-table td.bt-sel-col {
  width: 28px;
  text-align: center;
  cursor: default;
}
.bt-table th.bt-sel-col { cursor: default; }
.bt-table .bt-row-select:disabled {
  cursor: not-allowed;
  opacity: 0.4;
}
.bt-table .bt-row-select { cursor: pointer; }
```

- [ ] **Step 7: Manually smoke-test in a browser**

```bash
cd <repo>/.worktrees/backtest-multi-delete/bullforge
docker compose up -d --remove-orphans dashboard_service
```

Open the dashboard, navigate to Backtests. Verify:
1. A checkbox column appears as the leftmost column.
2. Checkboxes on queued/running rows (jobs) are disabled and show the "Cancel this backtest first" tooltip.
3. Checkboxes on running backtest_runs rows (status='running') are also disabled.
4. Clicking a checkbox on a terminal run toggles it without navigating to the detail page.
5. Waiting 10s for the poll keeps the checkbox checked.
6. No JS console errors.

If you don't have a running run handy, create one quickly from the UI and observe the disabled state while it's running.

- [ ] **Step 8: Commit**

```bash
cd <repo>/.worktrees/backtest-multi-delete
git add bullforge/services/dashboard_service/static/backtests.js \
        bullforge/services/dashboard_service/static/backtests.css
git commit -m "feat(dashboard): add checkbox column + selection state to backtests list"
```

---

## Task 5: Frontend — Shift+click range + header "select all"

**Files:**
- Modify: `bullforge/services/dashboard_service/static/backtests.js`

- [ ] **Step 1: Implement Shift+click range selection**

Replace the checkbox handler block inside `renderRows` (the block that starts with `const box = ev.target && ev.target.matches('.bt-row-select')`) with:

```js
        const box = ev.target && ev.target.matches('.bt-row-select') ? ev.target : null;
        if (box && !box.disabled) {
          let handledAsRange = false;
          if (ev.shiftKey && lastCheckedBox && lastCheckedBox !== box) {
            // Range select between lastCheckedBox and box (inclusive),
            // skipping disabled boxes. Target state = this box's new state.
            const all = Array.from(tbody.querySelectorAll('.bt-row-select'))
              .filter(b => !b.disabled);
            const a = all.indexOf(lastCheckedBox);
            const b = all.indexOf(box);
            if (a !== -1 && b !== -1) {
              const [lo, hi] = a < b ? [a, b] : [b, a];
              const targetChecked = box.checked;
              for (let i = lo; i <= hi; i++) {
                const bx = all[i];
                bx.checked = targetChecked;
                const k = bx.dataset.selKey;
                if (targetChecked) selection.add(k);
                else selection.delete(k);
              }
              handledAsRange = true;
            }
            // If lastCheckedBox was stale (not in current list), fall through.
          }
          if (!handledAsRange) {
            const key = box.dataset.selKey;
            if (box.checked) selection.add(key);
            else selection.delete(key);
          }
          lastCheckedBox = box;
          updateActionBar();
        }
```

- [ ] **Step 2: Wire the header `#bt-select-all` checkbox**

In `bindOverviewEvents` in `backtests.js` (currently lines 116-140), append the header-checkbox handler at the end of the function, just before the closing brace of `bindOverviewEvents`:

```js
    const selectAll = document.getElementById('bt-select-all');
    if (selectAll) {
      selectAll.onclick = function (ev) {
        ev.stopPropagation();
        const checked = selectAll.checked;
        const boxes = document.querySelectorAll(
          '#bt-rows .bt-row-select:not(:disabled)'
        );
        boxes.forEach(b => {
          b.checked = checked;
          const key = b.dataset.selKey;
          if (checked) selection.add(key);
          else selection.delete(key);
        });
        lastCheckedBox = null;
        updateActionBar();
      };
    }
```

- [ ] **Step 3: Keep the header checkbox in sync on row re-render**

At the very END of `renderRows`, after the selection-pruning loop, append:

```js
    // Keep header select-all in sync: checked iff every selectable row is selected.
    const headerBox = document.getElementById('bt-select-all');
    if (headerBox) {
      const selectableBoxes = tbody.querySelectorAll('.bt-row-select:not(:disabled)');
      const total = selectableBoxes.length;
      const selected = Array.from(selectableBoxes).filter(b => b.checked).length;
      headerBox.checked = total > 0 && selected === total;
      headerBox.indeterminate = selected > 0 && selected < total;
    }
```

- [ ] **Step 4: Manually smoke-test**

```bash
cd <repo>/.worktrees/backtest-multi-delete/bullforge
docker compose up -d --remove-orphans dashboard_service
```

In the Backtests list verify:
1. Click first row's checkbox, then Shift+click fifth → boxes 1-5 are all checked.
2. Shift+click third → boxes 5 through 3 are unchecked (reflecting third's new state).
3. Header checkbox selects all selectable rows; unchecks them when toggled off.
4. Header checkbox shows indeterminate state when only some rows are selected.
5. Disabled rows (running / jobs) are NEVER toggled by either action.

- [ ] **Step 5: Commit**

```bash
cd <repo>/.worktrees/backtest-multi-delete
git add bullforge/services/dashboard_service/static/backtests.js
git commit -m "feat(dashboard): shift-click range + header select-all for backtests"
```

---

## Task 6: Frontend — action bar + confirm + delete dispatch

**Files:**
- Modify: `bullforge/services/dashboard_service/static/backtests.js`
- Modify: `bullforge/services/dashboard_service/static/backtests.css`

- [ ] **Step 1: Add action-bar HTML to `renderOverview`**

In `backtests.js` inside `renderOverview`, insert an action-bar `<div>` AFTER `.bt-toolbar` and BEFORE `<table>`. Replace the existing `.bt-overview` block template so it reads (only the new bar line is added — keep the rest verbatim):

```js
    root().innerHTML = `
      <div class="bt-overview">
        <div class="bt-toolbar">
          <select id="bt-status">
            <option value="all">all</option>
            <option value="queued">queued</option>
            <option value="running">running</option>
            <option value="completed">completed</option>
            <option value="failed">failed</option>
            <option value="cancelled">Cancelled</option>
          </select>
          <input id="bt-search" placeholder="search name…" />
          <button id="bt-new">New Backtest</button>
          <button id="bt-refresh">Refresh</button>
        </div>
        <div id="bt-action-bar" class="bt-action-bar" hidden>
          <button id="bt-delete-selected" class="bt-delete-btn" type="button">Delete selected (<span id="bt-sel-count">0</span>)</button>
          <a id="bt-clear-selection" class="bt-clear-link" href="#">Clear</a>
          <span id="bt-action-summary" class="bt-action-summary"></span>
        </div>
        <table class="bt-table">
          <thead><tr>
            <th class="bt-sel-col"><input type="checkbox" id="bt-select-all" title="Select all on this page"></th>
            <th data-sort="name">Name</th>
            <th>Symbols</th>
            <th>Range</th>
            <th data-sort="total_pnl">P&amp;L</th>
            <th data-sort="sharpe">Sharpe</th>
            <th data-sort="max_dd">Max DD</th>
            <th>#Trades</th>
            <th>Hit %</th>
            <th>Status</th>
            <th>Tags</th>
            <th data-sort="finished_at">Finished</th>
            <th>Runtime</th>
          </tr></thead>
          <tbody id="bt-rows"><tr><td colspan="13">Loading…</td></tr></tbody>
        </table>
      </div>
    `;
```

- [ ] **Step 2: Replace the stub `updateActionBar` with the real one**

Replace the `function updateActionBar() { /* populated in Task 6 */ }` stub added in Task 4 with:

```js
  function updateActionBar() {
    const bar = document.getElementById('bt-action-bar');
    const count = document.getElementById('bt-sel-count');
    if (!bar || !count) return;  // list view not mounted
    const n = selection.size;
    count.textContent = n;
    bar.hidden = n === 0;
  }
```

- [ ] **Step 3: Wire the Clear link and the Delete button**

At the end of `bindOverviewEvents` (after the `selectAll` block added in Task 5), append:

```js
    const clearLink = document.getElementById('bt-clear-selection');
    if (clearLink) {
      clearLink.onclick = function (ev) {
        ev.preventDefault();
        clearSelection();
      };
    }
    const deleteBtn = document.getElementById('bt-delete-selected');
    if (deleteBtn) {
      deleteBtn.onclick = function () { deleteSelected(); };
    }
```

Add these two helper functions BEFORE `async function reload()` (or anywhere at module scope — pick one consistent spot):

```js
  function clearSelection() {
    selection.clear();
    lastCheckedBox = null;
    document.querySelectorAll('#bt-rows .bt-row-select').forEach(b => {
      b.checked = false;
    });
    const header = document.getElementById('bt-select-all');
    if (header) { header.checked = false; header.indeterminate = false; }
    const summary = document.getElementById('bt-action-summary');
    if (summary) summary.textContent = '';
    updateActionBar();
  }

  function selectedRunIds() {
    // selection keys are "run:{id}"; jobs never enter the set.
    return Array.from(selection)
      .filter(k => k.startsWith('run:'))
      .map(k => parseInt(k.slice(4), 10))
      .filter(Number.isFinite);
  }
```

- [ ] **Step 4: Implement `deleteSelected` with the 10-item threshold**

Add this function next to `clearSelection` / `selectedRunIds`:

```js
  const BULK_DELETE_THRESHOLD = 10;

  async function deleteSelected() {
    const ids = selectedRunIds();
    if (ids.length === 0) return;
    if (!confirm('Delete ' + ids.length + ' backtest(s)? This cannot be undone.')) return;

    let deleted = 0;
    let notFound = 0;
    let stillRunning = 0;

    if (ids.length <= BULK_DELETE_THRESHOLD) {
      const results = await Promise.allSettled(
        ids.map(id => fetch('/api/backtests/' + id, {method: 'DELETE'}))
      );
      for (const r of results) {
        if (r.status !== 'fulfilled') { continue; }
        const res = r.value;
        if (res.status === 200) deleted++;
        else if (res.status === 404) notFound++;
        else if (res.status === 409) stillRunning++;
      }
    } else {
      try {
        const res = await fetch('/api/backtests/delete', {
          method: 'POST',
          headers: {'content-type': 'application/json'},
          body: JSON.stringify({ids: ids}),
        });
        if (res.ok) {
          const body = await res.json();
          deleted = (body.deleted || []).length;
          notFound = (body.not_found || []).length;
          stillRunning = (body.still_running || []).length;
        } else {
          alert('Bulk delete failed: HTTP ' + res.status);
          return;
        }
      } catch (e) {
        alert('Bulk delete failed: ' + (e.message || 'network error'));
        return;
      }
    }

    const summary = document.getElementById('bt-action-summary');
    if (summary) {
      const parts = ['Deleted ' + deleted];
      if (stillRunning) parts.push(stillRunning + ' still running');
      if (notFound)     parts.push(notFound + ' not found');
      summary.textContent = parts.join(' · ');
    }
    clearSelection();
    reload();
  }
```

- [ ] **Step 5: Style the action bar**

Append to `bullforge/services/dashboard_service/static/backtests.css`:

```css
.bt-action-bar {
  display: flex;
  gap: 12px;
  align-items: center;
  padding: 6px 8px;
  margin-bottom: 8px;
  background: #1a1a1a;
  border: 1px solid #333;
  border-radius: 3px;
}
.bt-action-bar[hidden] { display: none; }
.bt-delete-btn {
  background: #b94b4b;
  color: white;
  border: none;
  border-radius: 3px;
  padding: 0.3em 0.8em;
  cursor: pointer;
  font-size: 0.9em;
}
.bt-delete-btn:hover { background: #d25959; }
.bt-clear-link {
  color: var(--text-muted, #888);
  font-size: 0.85em;
}
.bt-action-summary {
  color: var(--text-muted, #888);
  font-size: 0.85em;
  margin-left: auto;
}
```

- [ ] **Step 6: Manually smoke-test the full flow**

```bash
cd <repo>/.worktrees/backtest-multi-delete/bullforge
docker compose up -d --remove-orphans dashboard_service
```

Test cases from the spec's manual plan:

1. Select 3 completed runs, click `Delete selected (3)`, confirm → rows vanish; summary reads `Deleted 3`.
2. Select 1-5 rows using Shift+click range → counter matches; Delete removes all.
3. Check a running run is NOT selectable (checkbox disabled with tooltip).
4. Select 12+ runs: open the browser network tab, click Delete → observe ONE `POST /api/backtests/delete` request (not 12 `DELETE`s).
5. Select 10 runs exactly: observe 10 parallel `DELETE /api/backtests/{id}` calls.
6. Cancel the confirm dialog → selection preserved, action bar still shows.
7. Click Clear → selection empties, action bar hides.

- [ ] **Step 7: Commit**

```bash
cd <repo>/.worktrees/backtest-multi-delete
git add bullforge/services/dashboard_service/static/backtests.js \
        bullforge/services/dashboard_service/static/backtests.css
git commit -m "feat(dashboard): action bar + bulk/parallel delete dispatch for backtests"
```

---

## Task 7: Final verification

**Files:** none modified; this task runs the full test suite and deploy cycle to confirm nothing regressed.

- [ ] **Step 1: Run all backtest-related tests**

```bash
cd <repo>/.worktrees/backtest-multi-delete/bullforge
pytest tests/test_backtest_delete.py \
       tests/test_dashboard_backtests_api.py \
       tests/test_dashboard_backtests_extra_api.py \
       tests/test_backtest_jobs_db.py \
       tests/test_backtest_results_db.py -v
```

Expected: all existing and new tests pass.

- [ ] **Step 2: Redeploy the dashboard container with `--remove-orphans`**

Per the standing rule, always pass `--remove-orphans` when restarting compose services so any renamed or removed services don't leave orphaned containers.

```bash
cd <repo>/.worktrees/backtest-multi-delete/bullforge
docker compose up -d --remove-orphans dashboard_service
docker compose logs --tail 40 dashboard_service
```

Expected: dashboard service comes up healthy; no tracebacks in logs.

- [ ] **Step 3: Final manual smoke test on the live dashboard**

1. Backtests tab loads.
2. Select 2 completed runs, delete → both vanish; summary reads `Deleted 2`.
3. DB check: the run rows are gone AND their sandbox rows are gone.

```bash
docker compose exec timescaledb psql -U bullforge -d bullforge -c \
  "SELECT COUNT(*) AS rows FROM backtest_runs;
   SELECT COUNT(*) AS sandbox_orders FROM backtest.orders;
   SELECT COUNT(*) AS sandbox_preds FROM backtest.predictions;
   SELECT COUNT(*) AS sandbox_models FROM backtest.model_registry;"
```

Eyeball that counts look reasonable (no orphan sandbox rows for runs you just deleted). If you're unsure, delete one more known run and rerun the counts — each should drop.

- [ ] **Step 4: No code commit needed; update only if issues surface**

If any bug appears, fix it via small TDD additions to `test_backtest_delete.py` and matching code patches, then re-run Step 1. Commit each fix as its own `fix(...)` commit.

---

## Done

When Task 7 completes cleanly, the feature is ready for merge. Handoff to the finishing-a-development-branch skill at that point.
