"""Real-DB tests for the multi-delete backtest helper and routes.

Uses a function-scoped `db` fixture (same pattern as
`test_backtest_jobs_db.py`) so it composes cleanly with `aiohttp_client`.
"""
from __future__ import annotations

import pytest
import pytest_asyncio
from aiohttp import web

from shared.config import Settings
from shared.db import Database

from services.backtest_service.jobs_db import BacktestJobsDB
from services.backtest_service.results_db import BacktestResultsDB
from services.dashboard_service.backtests_api import _delete_run_impl
from services.dashboard_service.backtests_api import register_backtests_routes


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


async def test_delete_run_impl_returns_not_found_for_missing_id(db):
    # Pick an id that definitely doesn't exist.
    async with db.pool.acquire() as conn:
        max_id = await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM backtest_runs")
    missing_id = max_id + 100_000

    result = await _delete_run_impl(db, missing_id)
    assert result == {"status": "not_found", "run_id": missing_id}


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


# ---------------------------------------------------------------------------
# DELETE /api/backtests/{id} route tests (real-DB via aiohttp client).
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def delete_client(aiohttp_client, db):
    app = web.Application()
    app["db"] = db
    app["results_db"] = BacktestResultsDB(db)
    app["jobs_db"] = BacktestJobsDB(db)
    register_backtests_routes(app)
    return await aiohttp_client(app)


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


async def test_delete_route_404_on_missing_id(delete_client, db):
    async with db.pool.acquire() as conn:
        max_id = await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM backtest_runs")
    missing = max_id + 100_000

    resp = await delete_client.delete(f"/api/backtests/{missing}")
    assert resp.status == 404
    body = await resp.json()
    assert body["run_id"] == missing


async def test_delete_route_400_on_non_integer_id(delete_client):
    resp = await delete_client.delete("/api/backtests/not-an-int")
    assert resp.status == 400
