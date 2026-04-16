from __future__ import annotations

import json
from typing import Optional

from shared.db import Database


class BacktestJobsDB:
    """Thin async wrapper around the backtest_jobs table."""

    def __init__(self, db: Database):
        self._db = db

    async def insert_job(
        self, *, name: str, config: dict,
        custom_name: Optional[str] = None,
    ) -> int:
        async with self._db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO backtest_jobs (name, custom_name, config_json)
                VALUES ($1, $2, $3)
                RETURNING id
                """,
                name, custom_name, json.dumps(config),
            )
        return row["id"]

    async def claim_next(self) -> Optional[dict]:
        async with self._db.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT id, name, custom_name, config_json
                    FROM backtest_jobs
                    WHERE status = 'queued'
                    ORDER BY created_at
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """
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
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE backtest_jobs
                SET status = 'completed', run_id = $2, finished_at = NOW()
                WHERE id = $1
                """,
                job_id, run_id,
            )

    async def mark_failed(self, *, job_id: int, error: str) -> None:
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE backtest_jobs
                SET status = 'failed', error = $2, finished_at = NOW()
                WHERE id = $1
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
