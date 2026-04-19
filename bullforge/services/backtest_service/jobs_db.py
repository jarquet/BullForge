from __future__ import annotations

import json
from typing import Optional

from shared.db import Database
from services.backtest_service.worker_pool import cpu_or_gpu


class BacktestJobsDB:
    """Thin async wrapper around the backtest_jobs table."""

    def __init__(self, db: Database):
        self._db = db

    async def insert_job(
        self, *, name: str, config: dict,
        custom_name: Optional[str] = None,
        worker_pool: Optional[str] = None,
    ) -> int:
        pool = worker_pool or cpu_or_gpu(config.get("model_type"))
        async with self._db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO backtest_jobs (name, custom_name, config_json, worker_pool)
                VALUES ($1, $2, $3, $4)
                RETURNING id
                """,
                name, custom_name, json.dumps(config), pool,
            )
        return row["id"]

    async def claim_next(self, *, worker_pool: str) -> Optional[dict]:
        async with self._db.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT id, name, custom_name, config_json
                    FROM backtest_jobs
                    WHERE status = 'queued' AND worker_pool = $1
                    ORDER BY created_at
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """,
                    worker_pool,
                )
                if row is None:
                    return None
                await conn.execute(
                    """
                    UPDATE backtest_jobs
                    SET status = 'running', started_at = NOW()
                    WHERE id = $1
                    """,
                    row["id"],
                )
                return {
                    "id": row["id"],
                    "name": row["name"],
                    "custom_name": row["custom_name"],
                    "config": json.loads(row["config_json"]),
                    "status": "running",
                    "started_at": "set",
                }

    async def cancel(self, job_id: int) -> str:
        """Flip a job to 'cancelled' iff currently queued or running.

        CAS semantics: the `WHERE status IN (...)` clause prevents a race
        with a worker's mark_completed/mark_failed running at the same
        instant. Whichever UPDATE runs first wins; the other matches zero
        rows and becomes a no-op.

        Returns:
            'cancelled'         — the row actually flipped to cancelled
            'already_terminal'  — row was completed/failed/cancelled, or
                                  no such job_id exists
        """
        async with self._db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE backtest_jobs
                SET status = 'cancelled', finished_at = NOW()
                WHERE id = $1 AND status IN ('queued', 'running')
                RETURNING id
                """,
                job_id,
            )
        return "cancelled" if row else "already_terminal"

    async def is_cancelled(self, job_id: int) -> bool:
        """Fast PK lookup used by the engine's in-run cancellation poll."""
        async with self._db.pool.acquire() as conn:
            status = await conn.fetchval(
                "SELECT status FROM backtest_jobs WHERE id = $1", job_id,
            )
        return status == "cancelled"

    async def link_run(self, *, job_id: int, run_id: int) -> None:
        """Attach a run_id to a job as soon as the run row exists.

        Called mid-flight so the dashboard list can de-duplicate: once a job
        has a run_id, the run row is the authoritative UI surface.
        """
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE backtest_jobs SET run_id = $2 WHERE id = $1",
                job_id, run_id,
            )

    async def mark_completed(self, *, job_id: int, run_id: int) -> None:
        """No-op if the row is already terminal (e.g., cancelled mid-run)."""
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE backtest_jobs
                SET status = 'completed', run_id = $2, finished_at = NOW()
                WHERE id = $1 AND status IN ('queued', 'running')
                """,
                job_id, run_id,
            )

    async def mark_failed(self, *, job_id: int, error: str) -> None:
        """No-op if the row is already terminal (e.g., cancelled mid-run)."""
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE backtest_jobs
                SET status = 'failed', error = $2, finished_at = NOW()
                WHERE id = $1 AND status IN ('queued', 'running')
                """,
                job_id, error,
            )

    async def get(self, job_id: int) -> Optional[dict]:
        async with self._db.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM backtest_jobs WHERE id = $1", job_id,
            )
        return dict(row) if row else None

    async def list_pending(self) -> list[dict]:
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, name, custom_name, config_json, status, created_at,
                       started_at, finished_at, run_id, error
                FROM backtest_jobs
                WHERE status IN ('queued','running')
                  AND run_id IS NULL
                ORDER BY created_at DESC
                """
            )
        return [dict(r) for r in rows]
