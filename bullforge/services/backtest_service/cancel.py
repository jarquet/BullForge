"""One-shot cancel entrypoint.

Usage:
    docker compose exec backtest-worker-cpu python -m services.backtest_service.cancel <job_id>

Prints 'cancelled' on success (exit 0) or 'already_terminal' if the job was
no longer queued/running (exit 1). Exits 2 on argv error.
"""
from __future__ import annotations

import asyncio
import logging
import sys

from shared.config import Settings
from shared.db import Database
from services.backtest_service.jobs_db import BacktestJobsDB


async def _main(job_id: int) -> int:
    settings = Settings()
    db = Database(settings)
    await db.connect()
    try:
        result = await BacktestJobsDB(db).cancel(job_id)
        print(result)
        return 0 if result == "cancelled" else 1
    finally:
        await db.disconnect()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(
            "usage: python -m services.backtest_service.cancel <job_id>",
            file=sys.stderr,
        )
        sys.exit(2)
    try:
        job_id_arg = int(sys.argv[1])
    except ValueError:
        print("job_id must be an integer", file=sys.stderr)
        sys.exit(2)
    logging.basicConfig(level=logging.INFO)
    sys.exit(asyncio.run(_main(job_id_arg)))
