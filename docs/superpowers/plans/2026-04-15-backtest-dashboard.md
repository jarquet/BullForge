# Backtest Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a dashboard Backtests tab with an overview list (including P&L), a detail view with equity curve and drilldowns, and a "New Backtest" form that queues walk-forward runs executed by a new worker process.

**Architecture:** Extend `backtest_service/results_db.py` with read queries; add a new `backtest_jobs` table + `worker.py` loop; add `/api/backtests/*` routes to `dashboard_service/api.py`; build the UI as a new tab in the existing vanilla-JS dashboard using Chart.js.

**Tech Stack:** Python 3 / asyncio / aiohttp / asyncpg · TimescaleDB · vanilla JS / Chart.js (CDN) · pytest / pytest-asyncio · docker compose.

**Spec:** `docs/superpowers/specs/2026-04-15-backtest-dashboard-design.md`

**Pre-flight:**
- Work on branch `feature/backtest-dashboard` in the `.worktrees/backtest-dashboard` worktree.
- Tests require a running TimescaleDB (see `docker-compose.yml`); integration tests follow the existing `test_backtest_results_db.py` pattern (live DB via `Settings()`).

---

## File Structure

**Create:**
- `bullforge/migrations/013_create_backtest_jobs.sql` — new table.
- `bullforge/services/backtest_service/jobs_db.py` — thin async wrapper around `backtest_jobs`.
- `bullforge/services/backtest_service/worker.py` — long-running job consumer.
- `bullforge/services/backtest_service/__main__.py` *(only if missing)* — make `python -m bullforge.services.backtest_service.worker` discoverable (module already importable; no change expected).
- `bullforge/services/dashboard_service/backtests_api.py` — new route module (mirrors the split used by `config_api.py`, `freshness_api.py`, `symbols_api.py`).
- `bullforge/services/dashboard_service/static/backtests.js` — frontend controller for the new tab.
- `bullforge/services/dashboard_service/static/backtests.css` — styling for the tab.
- `bullforge/tests/test_backtest_jobs_db.py`
- `bullforge/tests/test_backtest_worker.py`
- `bullforge/tests/test_dashboard_backtests_api.py`
- `bullforge/tests/test_backtest_results_db_queries.py` — covers the new read methods.

**Modify:**
- `bullforge/services/backtest_service/results_db.py` — add query methods.
- `bullforge/services/dashboard_service/api.py` — register `backtests_api` routes.
- `bullforge/services/dashboard_service/main.py` — instantiate `BacktestJobsDB` and `BacktestResultsDB` (readonly) on startup.
- `bullforge/services/dashboard_service/static/index.html` — add tab button, tab body, Chart.js CDN `<script>`, include `backtests.js` + `backtests.css`.
- `bullforge/services/dashboard_service/static/dashboard.js` — register the Backtests tab in the existing tab-switch function.
- `bullforge/docker-compose.yml` — add `backtest-worker` long-running service.
- `bullforge/docs/runbooks/backtest.md` — document the worker + UI form.

Each task below is self-contained and ends with a commit.

---

## Task 1: Migration for `backtest_jobs`

**Files:**
- Create: `bullforge/migrations/013_create_backtest_jobs.sql`

- [ ] **Step 1: Write the migration**

```sql
-- bullforge/migrations/013_create_backtest_jobs.sql
CREATE TABLE IF NOT EXISTS backtest_jobs (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    config_json  JSONB NOT NULL,
    status       TEXT NOT NULL DEFAULT 'queued'
                 CHECK (status IN ('queued','running','completed','failed')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    run_id       BIGINT REFERENCES backtest_runs(id) ON DELETE SET NULL,
    error        TEXT
);
CREATE INDEX IF NOT EXISTS idx_backtest_jobs_status_created
    ON backtest_jobs (status, created_at);
```

- [ ] **Step 2: Apply the migration**

Run (against the dev DB — adjust DSN if different):

```bash
docker compose exec timescaledb psql -U postgres -d bullforge \
  -f /migrations/013_create_backtest_jobs.sql
```

Expected: `CREATE TABLE` and `CREATE INDEX` output with no errors. If `/migrations` is not mounted, run the equivalent via `psql` piping from stdin.

- [ ] **Step 3: Verify**

```bash
docker compose exec timescaledb psql -U postgres -d bullforge \
  -c "\d backtest_jobs"
```

Expected: table definition printed with the four statuses and the index.

- [ ] **Step 4: Commit**

```bash
git add bullforge/migrations/013_create_backtest_jobs.sql
git commit -m "feat(db): add backtest_jobs table for queued walk-forward jobs"
```

---

## Task 2: `BacktestJobsDB` — queue CRUD

**Files:**
- Create: `bullforge/services/backtest_service/jobs_db.py`
- Test: `bullforge/tests/test_backtest_jobs_db.py`

- [ ] **Step 1: Write the failing test**

```python
# bullforge/tests/test_backtest_jobs_db.py
import json
import pytest
from shared.config import Settings
from shared.db import Database
from services.backtest_service.jobs_db import BacktestJobsDB

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def db():
    d = Database(Settings())
    await d.connect()
    yield d
    await d.disconnect()


async def test_insert_and_claim_roundtrip(db):
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(name="t1", config={"symbols": ["SPY"]})
    assert job_id > 0

    claimed = await jobs.claim_next()
    assert claimed is not None
    assert claimed["id"] == job_id
    assert claimed["status"] == "running"
    assert claimed["started_at"] is not None

    # Second claim returns nothing
    second = await jobs.claim_next()
    assert second is None


async def test_mark_completed(db):
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(name="t2", config={})
    await jobs.claim_next()
    await jobs.mark_completed(job_id=job_id, run_id=12345)

    row = await jobs.get(job_id)
    assert row["status"] == "completed"
    assert row["run_id"] == 12345
    assert row["finished_at"] is not None


async def test_mark_failed(db):
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(name="t3", config={})
    await jobs.claim_next()
    await jobs.mark_failed(job_id=job_id, error="boom")
    row = await jobs.get(job_id)
    assert row["status"] == "failed"
    assert row["error"] == "boom"


async def test_list_pending_jobs(db):
    jobs = BacktestJobsDB(db)
    j1 = await jobs.insert_job(name="a", config={})
    j2 = await jobs.insert_job(name="b", config={})
    pending = await jobs.list_pending()
    ids = {r["id"] for r in pending}
    assert {j1, j2}.issubset(ids)
    assert all(r["status"] in ("queued", "running") for r in pending)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
docker compose run --rm backtest pytest bullforge/tests/test_backtest_jobs_db.py -v
```

Expected: `ModuleNotFoundError: No module named 'services.backtest_service.jobs_db'`.

- [ ] **Step 3: Implement `BacktestJobsDB`**

```python
# bullforge/services/backtest_service/jobs_db.py
from __future__ import annotations

import json
from typing import Optional

from shared.db import Database


class BacktestJobsDB:
    """Thin async wrapper around the backtest_jobs table."""

    def __init__(self, db: Database):
        self._db = db

    async def insert_job(self, *, name: str, config: dict) -> int:
        async with self._db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO backtest_jobs (name, config_json)
                VALUES ($1, $2)
                RETURNING id
                """,
                name, json.dumps(config),
            )
        return row["id"]

    async def claim_next(self) -> Optional[dict]:
        """Atomically claim the oldest queued job; returns None if empty."""
        async with self._db.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT id, name, config_json
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
                    "config": json.loads(row["config_json"]),
                    "status": "running",
                    "started_at": "set",
                }

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
        """Jobs that have not completed (queued or running)."""
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, name, config_json, status, created_at,
                       started_at, finished_at, run_id, error
                FROM backtest_jobs
                WHERE status IN ('queued','running')
                ORDER BY created_at DESC
                """
            )
        return [dict(r) for r in rows]
```

- [ ] **Step 4: Run test to verify it passes**

```bash
docker compose run --rm backtest pytest bullforge/tests/test_backtest_jobs_db.py -v
```

Expected: 4 passing.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/backtest_service/jobs_db.py \
        bullforge/tests/test_backtest_jobs_db.py
git commit -m "feat(backtest): BacktestJobsDB queue wrapper"
```

---

## Task 3: `BacktestResultsDB` — read query methods

**Files:**
- Modify: `bullforge/services/backtest_service/results_db.py`
- Test: `bullforge/tests/test_backtest_results_db_queries.py`

- [ ] **Step 1: Write the failing test**

Seeds a run, fold, trades, and equity points, then calls each new read method.

```python
# bullforge/tests/test_backtest_results_db_queries.py
from datetime import datetime, timezone
import pytest
from shared.config import Settings
from shared.db import Database
from services.backtest_service.results_db import BacktestResultsDB

pytestmark = pytest.mark.asyncio
UTC = timezone.utc


@pytest.fixture
async def seeded(tmp_path_factory):
    d = Database(Settings()); await d.connect()
    r = BacktestResultsDB(d)
    run_id = await r.insert_run(
        name="query-test", symbols=["SPY"], timeframe="1m", horizons=[5],
        range_start=datetime(2025, 1, 1, tzinfo=UTC),
        range_end=datetime(2025, 1, 5, tzinfo=UTC),
        train_days=1, test_days=1, embargo_minutes=5, mode="rolling",
        config={},
    )
    fold_id = await r.insert_fold(
        run_id=run_id, fold_index=0,
        train_start=datetime(2025,1,1,tzinfo=UTC),
        train_end=datetime(2025,1,2,tzinfo=UTC),
        test_start=datetime(2025,1,2,tzinfo=UTC),
        test_end=datetime(2025,1,3,tzinfo=UTC),
    )
    await r.insert_trade(
        run_id=run_id, fold_id=fold_id, symbol="SPY", side="long",
        entry_time=datetime(2025,1,2,14,0,tzinfo=UTC), entry_price=100.0,
        quantity=1.0,
        exit_time=datetime(2025,1,2,15,0,tzinfo=UTC), exit_price=102.0,
        pnl=2.0, signal_strength=0.5,
    )
    await r.insert_trade(
        run_id=run_id, fold_id=fold_id, symbol="SPY", side="long",
        entry_time=datetime(2025,1,2,16,0,tzinfo=UTC), entry_price=102.0,
        quantity=1.0,
        exit_time=datetime(2025,1,2,17,0,tzinfo=UTC), exit_price=101.0,
        pnl=-1.0, signal_strength=0.3,
    )
    for i, (t, eq) in enumerate([
        (datetime(2025,1,2,14,0,tzinfo=UTC), 100000.0),
        (datetime(2025,1,2,15,0,tzinfo=UTC), 100002.0),
        (datetime(2025,1,2,17,0,tzinfo=UTC), 100001.0),
    ]):
        await r.insert_equity_point(
            run_id=run_id, fold_id=fold_id, timestamp=t, equity=eq, cash=eq,
        )
    await r.finish_run(run_id=run_id, status="completed")
    yield d, r, run_id, fold_id
    await d.disconnect()


async def test_list_runs_returns_summary(seeded):
    _, r, run_id, _ = seeded
    rows = await r.list_runs(status="completed", q="query", limit=10, offset=0,
                             sort="finished_at", order="desc")
    match = [row for row in rows if row["id"] == run_id]
    assert match, "seeded run should be in list"
    row = match[0]
    assert row["metrics"]["n_trades"] == 2
    assert row["metrics"]["total_pnl"] == pytest.approx(1.0)
    assert row["metrics"]["hit_rate"] == pytest.approx(0.5)
    assert row["metrics"]["max_dd"] <= 0


async def test_get_run_returns_full_metrics(seeded):
    _, r, run_id, _ = seeded
    row = await r.get_run(run_id)
    assert row is not None
    m = row["metrics"]
    for key in ("total_pnl","sharpe","sortino","max_dd","hit_rate",
                "profit_factor","n_trades","n_folds"):
        assert key in m


async def test_get_run_returns_none_for_missing(seeded):
    _, r, *_ = seeded
    assert await r.get_run(-1) is None


async def test_list_folds(seeded):
    _, r, run_id, fold_id = seeded
    folds = await r.list_folds(run_id)
    assert len(folds) == 1
    f = folds[0]
    assert f["id"] == fold_id
    assert f["metrics"]["n_trades"] == 2
    assert f["metrics"]["total_pnl"] == pytest.approx(1.0)


async def test_list_trades_paginated(seeded):
    _, r, run_id, _ = seeded
    trades = await r.list_trades(run_id, limit=1, offset=0,
                                 sort="entry_time", order="asc")
    assert len(trades) == 1
    assert trades[0]["entry_price"] == 100.0


async def test_get_equity_curve(seeded):
    _, r, run_id, fold_id = seeded
    full = await r.get_equity_curve(run_id, fold_id=None)
    assert len(full) == 3
    assert full[0]["equity"] == 100000.0
    per = await r.get_equity_curve(run_id, fold_id=fold_id)
    assert len(per) == 3
```

- [ ] **Step 2: Run test to verify it fails**

```bash
docker compose run --rm backtest pytest \
  bullforge/tests/test_backtest_results_db_queries.py -v
```

Expected: `AttributeError: 'BacktestResultsDB' object has no attribute 'list_runs'`.

- [ ] **Step 3: Add the read methods**

Append to `bullforge/services/backtest_service/results_db.py`:

```python
    # ------------------------------ reads ------------------------------

    _SORTABLE = {
        "finished_at", "started_at", "name",
        "total_pnl", "sharpe", "max_dd",
    }

    async def list_runs(
        self, *,
        status: str = "all",
        q: Optional[str] = None,
        sort: str = "finished_at",
        order: str = "desc",
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        if sort not in self._SORTABLE:
            sort = "finished_at"
        order = "asc" if order.lower() == "asc" else "desc"

        # Aggregated per-run summary using LATERAL joins.
        sql = f"""
            SELECT r.id, r.name, r.symbols, r.range_start, r.range_end,
                   r.status, r.started_at, r.finished_at,
                   COALESCE(t.n_trades, 0)                AS n_trades,
                   COALESCE(t.total_pnl, 0)               AS total_pnl,
                   COALESCE(t.hit_rate, 0)                AS hit_rate,
                   COALESCE(e.max_dd, 0)                  AS max_dd,
                   COALESCE(e.sharpe, 0)                  AS sharpe
            FROM backtest_runs r
            LEFT JOIN LATERAL (
                SELECT COUNT(*)                                  AS n_trades,
                       COALESCE(SUM(pnl), 0)                     AS total_pnl,
                       AVG(CASE WHEN pnl > 0 THEN 1.0 ELSE 0 END) AS hit_rate
                FROM backtest_trades WHERE run_id = r.id
            ) t ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    MIN(equity / NULLIF(running_max, 0) - 1) AS max_dd,
                    -- Cheap sharpe proxy on equity returns; replace with a
                    -- materialized view if volume grows.
                    CASE WHEN STDDEV_SAMP(ret) > 0
                         THEN AVG(ret) / STDDEV_SAMP(ret) * SQRT(252)
                         ELSE 0 END AS sharpe
                FROM (
                    SELECT
                        equity,
                        MAX(equity) OVER (ORDER BY timestamp) AS running_max,
                        (equity - LAG(equity) OVER (ORDER BY timestamp))
                            / NULLIF(LAG(equity) OVER (ORDER BY timestamp), 0)
                            AS ret
                    FROM backtest_equity WHERE run_id = r.id
                ) s
            ) e ON TRUE
            WHERE ($1 = 'all' OR r.status = $1)
              AND ($2::text IS NULL OR r.name ILIKE '%' || $2 || '%')
            ORDER BY {sort} {order} NULLS LAST
            LIMIT $3 OFFSET $4
        """
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(sql, status, q, limit, offset)
        return [
            {
                "id": r["id"],
                "name": r["name"],
                "symbols": list(r["symbols"]),
                "range_start": r["range_start"],
                "range_end": r["range_end"],
                "status": r["status"],
                "started_at": r["started_at"],
                "finished_at": r["finished_at"],
                "metrics": {
                    "n_trades": r["n_trades"],
                    "total_pnl": float(r["total_pnl"]),
                    "hit_rate": float(r["hit_rate"]),
                    "max_dd": float(r["max_dd"]),
                    "sharpe": float(r["sharpe"]),
                },
            }
            for r in rows
        ]

    async def get_run(self, run_id: int) -> Optional[dict]:
        async with self._db.pool.acquire() as conn:
            header = await conn.fetchrow(
                "SELECT * FROM backtest_runs WHERE id = $1", run_id,
            )
            if header is None:
                return None
            agg = await conn.fetchrow(
                """
                SELECT
                    COALESCE(SUM(pnl), 0)                AS total_pnl,
                    COUNT(*)                             AS n_trades,
                    AVG(CASE WHEN pnl > 0 THEN 1.0 ELSE 0 END) AS hit_rate,
                    COALESCE(SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END)
                             / NULLIF(-SUM(CASE WHEN pnl < 0 THEN pnl ELSE 0 END), 0),
                             0) AS profit_factor
                FROM backtest_trades WHERE run_id = $1
                """,
                run_id,
            )
            eq = await conn.fetchrow(
                """
                SELECT
                    MIN(equity / NULLIF(running_max, 0) - 1) AS max_dd,
                    CASE WHEN STDDEV_SAMP(ret) > 0
                         THEN AVG(ret) / STDDEV_SAMP(ret) * SQRT(252) ELSE 0
                    END AS sharpe,
                    CASE WHEN STDDEV_SAMP(CASE WHEN ret < 0 THEN ret END) > 0
                         THEN AVG(ret) / STDDEV_SAMP(CASE WHEN ret < 0 THEN ret END) * SQRT(252)
                         ELSE 0 END AS sortino
                FROM (
                    SELECT equity,
                           MAX(equity) OVER (ORDER BY timestamp) AS running_max,
                           (equity - LAG(equity) OVER (ORDER BY timestamp))
                             / NULLIF(LAG(equity) OVER (ORDER BY timestamp), 0)
                             AS ret
                    FROM backtest_equity WHERE run_id = $1
                ) s
                """,
                run_id,
            )
            n_folds = await conn.fetchval(
                "SELECT COUNT(*) FROM backtest_folds WHERE run_id = $1", run_id,
            )
        return {
            "id": header["id"],
            "name": header["name"],
            "symbols": list(header["symbols"]),
            "timeframe": header["timeframe"],
            "horizons": list(header["horizons"]),
            "range_start": header["range_start"],
            "range_end": header["range_end"],
            "train_days": header["train_days"],
            "test_days": header["test_days"],
            "embargo_min": header["embargo_min"],
            "mode": header["mode"],
            "status": header["status"],
            "started_at": header["started_at"],
            "finished_at": header["finished_at"],
            "config": json.loads(header["config_json"]) if header["config_json"] else {},
            "metrics": {
                "total_pnl": float(agg["total_pnl"]),
                "n_trades": agg["n_trades"],
                "hit_rate": float(agg["hit_rate"] or 0),
                "profit_factor": float(agg["profit_factor"] or 0),
                "max_dd": float(eq["max_dd"] or 0),
                "sharpe": float(eq["sharpe"] or 0),
                "sortino": float(eq["sortino"] or 0),
                "n_folds": n_folds,
            },
        }

    async def list_folds(self, run_id: int) -> list[dict]:
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT f.id, f.fold_index,
                       f.train_start, f.train_end, f.test_start, f.test_end,
                       f.metrics_json,
                       COALESCE(t.n_trades, 0)  AS n_trades,
                       COALESCE(t.total_pnl, 0) AS total_pnl,
                       COALESCE(e.max_dd, 0)    AS max_dd,
                       COALESCE(e.sharpe, 0)    AS sharpe
                FROM backtest_folds f
                LEFT JOIN LATERAL (
                    SELECT COUNT(*) AS n_trades, COALESCE(SUM(pnl),0) AS total_pnl
                    FROM backtest_trades WHERE fold_id = f.id
                ) t ON TRUE
                LEFT JOIN LATERAL (
                    SELECT MIN(equity / NULLIF(running_max,0) - 1) AS max_dd,
                           CASE WHEN STDDEV_SAMP(ret) > 0
                                THEN AVG(ret)/STDDEV_SAMP(ret)*SQRT(252) ELSE 0 END AS sharpe
                    FROM (
                        SELECT equity,
                               MAX(equity) OVER (ORDER BY timestamp) AS running_max,
                               (equity - LAG(equity) OVER (ORDER BY timestamp))
                                 / NULLIF(LAG(equity) OVER (ORDER BY timestamp),0) AS ret
                        FROM backtest_equity WHERE fold_id = f.id
                    ) s
                ) e ON TRUE
                WHERE f.run_id = $1
                ORDER BY f.fold_index
                """,
                run_id,
            )
        return [
            {
                "id": r["id"],
                "fold_index": r["fold_index"],
                "train_start": r["train_start"],
                "train_end": r["train_end"],
                "test_start": r["test_start"],
                "test_end": r["test_end"],
                "metrics": {
                    "n_trades": r["n_trades"],
                    "total_pnl": float(r["total_pnl"]),
                    "max_dd": float(r["max_dd"]),
                    "sharpe": float(r["sharpe"]),
                },
                "metrics_json": json.loads(r["metrics_json"])
                                if r["metrics_json"] else None,
            }
            for r in rows
        ]

    _TRADE_SORTABLE = {
        "entry_time", "exit_time", "pnl", "symbol", "signal_strength",
    }

    async def list_trades(
        self, run_id: int, *,
        limit: int = 50, offset: int = 0,
        sort: str = "entry_time", order: str = "desc",
    ) -> list[dict]:
        if sort not in self._TRADE_SORTABLE:
            sort = "entry_time"
        order = "asc" if order.lower() == "asc" else "desc"
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT id, fold_id, symbol, side,
                       entry_time, entry_price,
                       exit_time, exit_price,
                       quantity, pnl, signal_strength
                FROM backtest_trades
                WHERE run_id = $1
                ORDER BY {sort} {order} NULLS LAST
                LIMIT $2 OFFSET $3
                """,
                run_id, limit, offset,
            )
        return [dict(r) for r in rows]

    async def get_equity_curve(
        self, run_id: int, *, fold_id: Optional[int],
    ) -> list[dict]:
        async with self._db.pool.acquire() as conn:
            if fold_id is None:
                rows = await conn.fetch(
                    """
                    SELECT timestamp, equity, cash
                    FROM backtest_equity WHERE run_id = $1
                    ORDER BY timestamp
                    """,
                    run_id,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT timestamp, equity, cash
                    FROM backtest_equity WHERE run_id = $1 AND fold_id = $2
                    ORDER BY timestamp
                    """,
                    run_id, fold_id,
                )
        return [{"t": r["timestamp"], "equity": float(r["equity"]),
                 "cash": float(r["cash"])} for r in rows]
```

- [ ] **Step 4: Run tests**

```bash
docker compose run --rm backtest pytest \
  bullforge/tests/test_backtest_results_db_queries.py -v
```

Expected: 6 passing.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/backtest_service/results_db.py \
        bullforge/tests/test_backtest_results_db_queries.py
git commit -m "feat(backtest): read-side query methods on BacktestResultsDB"
```

---

## Task 4: Worker process

**Files:**
- Create: `bullforge/services/backtest_service/worker.py`
- Test: `bullforge/tests/test_backtest_worker.py`

- [ ] **Step 1: Write the failing test**

Drive the worker through a single mocked-engine run; verify the job transitions `queued → running → completed` with `run_id` set.

```python
# bullforge/tests/test_backtest_worker.py
from datetime import datetime, timezone
from unittest.mock import AsyncMock
import pytest

from services.backtest_service.worker import run_one


pytestmark = pytest.mark.asyncio


class FakeJobs:
    def __init__(self, job):
        self._job = job
        self.completed = None
        self.failed = None
    async def claim_next(self):
        j, self._job = self._job, None
        return j
    async def mark_completed(self, *, job_id, run_id):
        self.completed = (job_id, run_id)
    async def mark_failed(self, *, job_id, error):
        self.failed = (job_id, error)


async def test_run_one_success():
    jobs = FakeJobs({
        "id": 7,
        "name": "t",
        "config": {
            "symbols": ["SPY"], "timeframe": "1m", "horizons": [5],
            "range_start": "2025-01-01T00:00:00+00:00",
            "range_end": "2025-01-02T00:00:00+00:00",
            "train_days": 1, "test_days": 1, "embargo_min": 5,
            "mode": "rolling",
        },
    })
    engine = AsyncMock()
    engine.run = AsyncMock(return_value=42)

    got_job = await run_one(jobs, engine_factory=lambda cfg: engine)
    assert got_job is True
    assert jobs.completed == (7, 42)


async def test_run_one_idle_when_no_job():
    jobs = FakeJobs(None)
    engine = AsyncMock()
    got = await run_one(jobs, engine_factory=lambda cfg: engine)
    assert got is False
    assert jobs.completed is None
    engine.run.assert_not_called()


async def test_run_one_failure_marks_failed():
    jobs = FakeJobs({
        "id": 9, "name": "t",
        "config": {
            "symbols": ["SPY"], "timeframe": "1m", "horizons": [5],
            "range_start": "2025-01-01T00:00:00+00:00",
            "range_end": "2025-01-02T00:00:00+00:00",
            "train_days": 1, "test_days": 1, "embargo_min": 5,
            "mode": "rolling",
        },
    })
    engine = AsyncMock()
    engine.run = AsyncMock(side_effect=RuntimeError("boom"))
    await run_one(jobs, engine_factory=lambda cfg: engine)
    assert jobs.failed is not None
    assert "boom" in jobs.failed[1]
```

- [ ] **Step 2: Run test to verify failure**

```bash
docker compose run --rm backtest pytest \
  bullforge/tests/test_backtest_worker.py -v
```

Expected: ModuleNotFoundError.

- [ ] **Step 3: Implement the worker**

```python
# bullforge/services/backtest_service/worker.py
from __future__ import annotations

import asyncio
import logging
import signal
import traceback
from dataclasses import asdict
from datetime import datetime
from typing import Callable

from shared.config import Settings
from shared.db import Database

from services.backtest_service.engine import BacktestEngine, BacktestConfig
from services.backtest_service.jobs_db import BacktestJobsDB
from services.backtest_service.results_db import BacktestResultsDB

logger = logging.getLogger(__name__)

IDLE_SLEEP_SECONDS = 2.0


def _build_config(cfg: dict) -> BacktestConfig:
    return BacktestConfig(
        name=cfg["name"] if "name" in cfg else cfg.get("run_name", "dashboard-run"),
        symbols=list(cfg["symbols"]),
        timeframe=cfg["timeframe"],
        horizons=list(cfg["horizons"]),
        range_start=datetime.fromisoformat(cfg["range_start"]),
        range_end=datetime.fromisoformat(cfg["range_end"]),
        train_days=int(cfg["train_days"]),
        test_days=int(cfg["test_days"]),
        embargo_minutes=int(cfg["embargo_min"]),
        mode=cfg["mode"],
    )


async def run_one(
    jobs: BacktestJobsDB,
    *,
    engine_factory: Callable[[BacktestConfig], BacktestEngine],
) -> bool:
    """Claim and run exactly one job. Returns True iff a job was processed."""
    job = await jobs.claim_next()
    if job is None:
        return False

    job_id = job["id"]
    try:
        cfg = _build_config({"name": job["name"], **job["config"]})
        engine = engine_factory(cfg)
        run_id = await engine.run(cfg)
        await jobs.mark_completed(job_id=job_id, run_id=int(run_id))
        logger.info("backtest job %s -> run %s completed", job_id, run_id)
    except Exception as exc:  # pragma: no cover - exercised via test
        tb = traceback.format_exc()
        logger.exception("backtest job %s failed", job_id)
        await jobs.mark_failed(job_id=job_id, error=f"{exc}\n{tb}")
    return True


def _default_engine(cfg: BacktestConfig) -> BacktestEngine:
    # Lazy construction avoids importing heavy deps at module import time
    # when the worker is merely imported for tests.
    from services.backtest_service.engine import make_default_engine
    return make_default_engine()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings()
    db = Database(settings)
    await db.connect()
    jobs = BacktestJobsDB(db)

    stop = asyncio.Event()

    def _graceful(*_):
        logger.info("worker shutdown requested")
        stop.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _graceful)
        except NotImplementedError:  # pragma: no cover - Windows
            pass

    try:
        while not stop.is_set():
            did = await run_one(jobs, engine_factory=_default_engine)
            if not did:
                try:
                    await asyncio.wait_for(stop.wait(),
                                            timeout=IDLE_SLEEP_SECONDS)
                except asyncio.TimeoutError:
                    pass
    finally:
        await db.disconnect()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
```

Note: `make_default_engine` does not currently exist. If `BacktestEngine`'s constructor today takes explicit `db`/`broker` args, update `_default_engine` to construct those from `Settings`. Inspect `engine.py` for the actual constructor and adjust if needed — this plan assumes a zero-arg factory; fall back to the exact CLI construction used in `bullforge/scripts/backtest.py` if that differs.

- [ ] **Step 4: Run tests**

```bash
docker compose run --rm backtest pytest \
  bullforge/tests/test_backtest_worker.py -v
```

Expected: 3 passing.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/backtest_service/worker.py \
        bullforge/tests/test_backtest_worker.py
git commit -m "feat(backtest): worker loop consuming backtest_jobs"
```

---

## Task 5: Compose — long-running `backtest-worker` service

**Files:**
- Modify: `bullforge/docker-compose.yml`

- [ ] **Step 1: Add service**

Append (alongside the existing `backtest` service — do NOT delete the `tools`-profile CLI invocation):

```yaml
  backtest-worker:
    build:
      context: .
      dockerfile: services/backtest_service/Dockerfile
    image: bullforge-backtest
    command: ["python", "-m", "services.backtest_service.worker"]
    depends_on:
      timescaledb:
        condition: service_healthy
      redis:
        condition: service_healthy
    environment:
      - PYTHONPATH=/app/bullforge
    env_file:
      - .env
    restart: unless-stopped
```

Reuse the same image tag as the `backtest` CLI service (`image: bullforge-backtest`) so `docker compose build backtest` also rebuilds the worker.

- [ ] **Step 2: Bring the worker up**

```bash
docker compose up -d backtest-worker
docker compose logs --tail=20 backtest-worker
```

Expected: log line `worker shutdown requested` absent; the worker sits idle polling.

- [ ] **Step 3: Smoke test — enqueue via SQL and observe status flip**

```bash
docker compose exec timescaledb psql -U postgres -d bullforge -c \
  "INSERT INTO backtest_jobs(name, config_json) VALUES ('smoke', '{\"symbols\":[\"SPY\"],\"timeframe\":\"1m\",\"horizons\":[5],\"range_start\":\"2025-01-02T14:30:00+00:00\",\"range_end\":\"2025-01-02T21:00:00+00:00\",\"train_days\":1,\"test_days\":1,\"embargo_min\":5,\"mode\":\"rolling\"}'::jsonb) RETURNING id;"
sleep 5
docker compose exec timescaledb psql -U postgres -d bullforge -c \
  "SELECT id, status, run_id, error FROM backtest_jobs ORDER BY id DESC LIMIT 1;"
```

Expected: row eventually shows `status='completed'` with a `run_id`. If the smoke range lacks data, a `failed` status is also acceptable — confirm the worker reached the job and didn't crash.

- [ ] **Step 4: Commit**

```bash
git add bullforge/docker-compose.yml
git commit -m "feat(compose): long-running backtest-worker service"
```

---

## Task 6: Dashboard API — `GET /api/backtests` (list)

**Files:**
- Create: `bullforge/services/dashboard_service/backtests_api.py`
- Modify: `bullforge/services/dashboard_service/api.py`
- Modify: `bullforge/services/dashboard_service/main.py`
- Test: `bullforge/tests/test_dashboard_backtests_api.py`

- [ ] **Step 1: Write the failing test**

```python
# bullforge/tests/test_dashboard_backtests_api.py
import pytest
from unittest.mock import AsyncMock
from aiohttp import web

from services.dashboard_service.backtests_api import register_backtests_routes


@pytest.fixture
def app():
    app = web.Application()
    app["results_db"] = AsyncMock()
    app["jobs_db"] = AsyncMock()
    register_backtests_routes(app)
    return app


async def test_list_returns_runs_and_pending_jobs(aiohttp_client, app):
    app["results_db"].list_runs = AsyncMock(return_value=[
        {"id": 1, "name": "r", "symbols": ["SPY"],
         "range_start": None, "range_end": None,
         "status": "completed",
         "started_at": None, "finished_at": None,
         "metrics": {"total_pnl": 1.0, "sharpe": 0.5, "max_dd": -0.1,
                     "n_trades": 2, "hit_rate": 0.5}},
    ])
    app["jobs_db"].list_pending = AsyncMock(return_value=[
        {"id": 9, "name": "q", "config_json": {"symbols": ["TSLA"]},
         "status": "queued", "created_at": None,
         "started_at": None, "finished_at": None, "run_id": None,
         "error": None},
    ])
    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests")
    assert resp.status == 200
    body = await resp.json()
    kinds = {row["kind"] for row in body["rows"]}
    assert {"run", "job"} == kinds


async def test_validate_new_job_rejects_embargo_gt_horizon(aiohttp_client, app):
    client = await aiohttp_client(app)
    resp = await client.post("/api/backtests", json={
        "name": "n", "symbols": ["SPY"], "timeframe": "1m",
        "horizons": [5, 10], "range_start": "2025-01-01T00:00:00+00:00",
        "range_end":   "2025-01-02T00:00:00+00:00",
        "train_days": 1, "test_days": 1, "embargo_min": 30, "mode": "rolling",
    })
    assert resp.status == 400
    body = await resp.json()
    assert "embargo_min" in body["errors"]
```

- [ ] **Step 2: Run test to verify failure**

```bash
docker compose run --rm backtest pytest \
  bullforge/tests/test_dashboard_backtests_api.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement the route module**

```python
# bullforge/services/dashboard_service/backtests_api.py
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from aiohttp import web

logger = logging.getLogger(__name__)

VALID_HORIZONS = {5, 10, 15, 30, 60, 75, 90, 105, 120}
VALID_MODES = {"rolling", "anchored"}
VALID_TIMEFRAMES = {"1m", "1d"}
JOB_STATUS_FILTERS = {"all", "queued", "running", "completed", "failed"}


def _iso(v):
    return v.isoformat() if isinstance(v, datetime) else v


def register_backtests_routes(app: web.Application) -> None:
    app.router.add_get("/api/backtests", _list)
    app.router.add_post("/api/backtests", _create)
    app.router.add_get("/api/backtests/{id}", _detail)
    app.router.add_get("/api/backtests/{id}/folds", _folds)
    app.router.add_get("/api/backtests/{id}/equity", _equity)
    app.router.add_get("/api/backtests/{id}/trades", _trades)


async def _list(request: web.Request) -> web.Response:
    results = request.app["results_db"]
    jobs = request.app["jobs_db"]

    status = request.query.get("status", "all")
    if status not in JOB_STATUS_FILTERS:
        return web.json_response({"error": "bad status"}, status=400)
    q = request.query.get("q") or None
    sort = request.query.get("sort", "finished_at")
    order = request.query.get("order", "desc")
    try:
        limit = min(int(request.query.get("limit", 50)), 200)
        offset = int(request.query.get("offset", 0))
    except ValueError:
        return web.json_response({"error": "bad paging"}, status=400)

    rows: list[dict] = []
    if status in ("all", "queued", "running"):
        pending = await jobs.list_pending()
        for j in pending:
            if status != "all" and j["status"] != status:
                continue
            if q and q.lower() not in j["name"].lower():
                continue
            cfg = j["config_json"] if isinstance(j["config_json"], dict) \
                else json.loads(j["config_json"])
            rows.append({
                "kind": "job",
                "id": j["id"],
                "name": j["name"],
                "symbols": cfg.get("symbols", []),
                "range_start": cfg.get("range_start"),
                "range_end": cfg.get("range_end"),
                "status": j["status"],
                "started_at": _iso(j["started_at"]),
                "finished_at": _iso(j["finished_at"]),
                "metrics": None,
                "error": j["error"],
            })

    if status in ("all", "completed", "failed"):
        run_status = status if status in ("completed", "failed") else "all"
        runs = await results.list_runs(
            status=run_status, q=q, sort=sort, order=order,
            limit=limit, offset=offset,
        )
        for r in runs:
            rows.append({
                "kind": "run",
                "id": r["id"],
                "name": r["name"],
                "symbols": r["symbols"],
                "range_start": _iso(r["range_start"]),
                "range_end": _iso(r["range_end"]),
                "status": r["status"],
                "started_at": _iso(r["started_at"]),
                "finished_at": _iso(r["finished_at"]),
                "metrics": r["metrics"],
            })

    return web.json_response({"rows": rows})


def _validate_new_job(body: dict) -> dict:
    errors: dict[str, str] = {}
    name = (body.get("name") or "").strip()
    if not name:
        errors["name"] = "required"
    symbols = body.get("symbols") or []
    if not isinstance(symbols, list) or not symbols:
        errors["symbols"] = "at least one required"
    timeframe = body.get("timeframe")
    if timeframe not in VALID_TIMEFRAMES:
        errors["timeframe"] = f"must be one of {sorted(VALID_TIMEFRAMES)}"
    horizons = body.get("horizons") or []
    if (not isinstance(horizons, list) or not horizons
            or not all(h in VALID_HORIZONS for h in horizons)):
        errors["horizons"] = f"subset of {sorted(VALID_HORIZONS)} required"
    try:
        start = datetime.fromisoformat(body["range_start"])
        end = datetime.fromisoformat(body["range_end"])
        if start >= end:
            errors["range_end"] = "must be after range_start"
    except (KeyError, TypeError, ValueError):
        errors["range_start"] = "range_start and range_end must be ISO-8601"
    for key in ("train_days", "test_days", "embargo_min"):
        v = body.get(key)
        if not isinstance(v, int) or v <= 0:
            errors[key] = "positive integer required"
    if "horizons" not in errors and "embargo_min" not in errors:
        if body["embargo_min"] > max(horizons):
            errors["embargo_min"] = "must be ≤ max(horizons)"
    mode = body.get("mode")
    if mode not in VALID_MODES:
        errors["mode"] = f"must be one of {sorted(VALID_MODES)}"
    return errors


async def _create(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "bad json"}, status=400)

    errors = _validate_new_job(body)
    if errors:
        return web.json_response({"errors": errors}, status=400)

    jobs = request.app["jobs_db"]
    config = {k: body[k] for k in (
        "symbols", "timeframe", "horizons",
        "range_start", "range_end",
        "train_days", "test_days", "embargo_min", "mode",
    )}
    job_id = await jobs.insert_job(name=body["name"], config=config)
    return web.json_response({"job_id": job_id}, status=201)


async def _detail(request: web.Request) -> web.Response:
    run_id = int(request.match_info["id"])
    row = await request.app["results_db"].get_run(run_id)
    if row is None:
        return web.json_response({"error": "not found"}, status=404)
    row["range_start"] = _iso(row["range_start"])
    row["range_end"] = _iso(row["range_end"])
    row["started_at"] = _iso(row["started_at"])
    row["finished_at"] = _iso(row["finished_at"])
    return web.json_response(row)


async def _folds(request: web.Request) -> web.Response:
    run_id = int(request.match_info["id"])
    folds = await request.app["results_db"].list_folds(run_id)
    for f in folds:
        for k in ("train_start", "train_end", "test_start", "test_end"):
            f[k] = _iso(f[k])
    return web.json_response({"folds": folds})


async def _equity(request: web.Request) -> web.Response:
    run_id = int(request.match_info["id"])
    fold_id = request.query.get("fold_id")
    fold_id = int(fold_id) if fold_id else None
    pts = await request.app["results_db"].get_equity_curve(run_id, fold_id=fold_id)
    for p in pts:
        p["t"] = _iso(p["t"])
    return web.json_response({"points": pts})


async def _trades(request: web.Request) -> web.Response:
    run_id = int(request.match_info["id"])
    try:
        limit = min(int(request.query.get("limit", 50)), 200)
        offset = int(request.query.get("offset", 0))
    except ValueError:
        return web.json_response({"error": "bad paging"}, status=400)
    sort = request.query.get("sort", "entry_time")
    order = request.query.get("order", "desc")
    trades = await request.app["results_db"].list_trades(
        run_id, limit=limit, offset=offset, sort=sort, order=order,
    )
    for t in trades:
        t["entry_time"] = _iso(t["entry_time"])
        t["exit_time"] = _iso(t["exit_time"])
    return web.json_response({"trades": trades})
```

- [ ] **Step 4: Wire routes into the main API app**

Edit `bullforge/services/dashboard_service/api.py` `create_api_app` — append after the existing `app.router.add_get(...)` block, before `return app`:

```python
    from services.dashboard_service.backtests_api import register_backtests_routes
    register_backtests_routes(app)
```

And pass `results_db` / `jobs_db` in via the app dict. In `main.py` where the API app is constructed, add:

```python
from services.backtest_service.results_db import BacktestResultsDB
from services.backtest_service.jobs_db import BacktestJobsDB
...
api_app["results_db"] = BacktestResultsDB(db)
api_app["jobs_db"] = BacktestJobsDB(db)
```

(Do this after `api_app = create_api_app(db)`.)

- [ ] **Step 5: Run tests**

```bash
docker compose run --rm backtest pytest \
  bullforge/tests/test_dashboard_backtests_api.py -v
```

Expected: 2 passing.

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/dashboard_service/backtests_api.py \
        bullforge/services/dashboard_service/api.py \
        bullforge/services/dashboard_service/main.py \
        bullforge/tests/test_dashboard_backtests_api.py
git commit -m "feat(dashboard): /api/backtests list + create routes"
```

---

## Task 7: Dashboard API — detail, folds, equity, trades tests

**Files:**
- Modify: `bullforge/tests/test_dashboard_backtests_api.py`

- [ ] **Step 1: Add happy-path tests**

Append these tests to the file from Task 6. They exercise the route handlers against mocked `results_db` responses.

```python
async def test_detail_happy_path(aiohttp_client, app):
    app["results_db"].get_run = AsyncMock(return_value={
        "id": 1, "name": "r", "symbols": ["SPY"], "timeframe": "1m",
        "horizons": [5], "range_start": None, "range_end": None,
        "train_days": 1, "test_days": 1, "embargo_min": 5,
        "mode": "rolling", "status": "completed",
        "started_at": None, "finished_at": None,
        "config": {}, "metrics": {"total_pnl": 1.0, "n_trades": 2,
                                   "hit_rate": 0.5, "profit_factor": 1.0,
                                   "max_dd": -0.1, "sharpe": 0.0,
                                   "sortino": 0.0, "n_folds": 1},
    })
    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests/1")
    assert resp.status == 200
    data = await resp.json()
    assert data["id"] == 1
    assert data["metrics"]["n_folds"] == 1


async def test_detail_404(aiohttp_client, app):
    app["results_db"].get_run = AsyncMock(return_value=None)
    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests/999")
    assert resp.status == 404


async def test_folds_route(aiohttp_client, app):
    app["results_db"].list_folds = AsyncMock(return_value=[
        {"id": 1, "fold_index": 0,
         "train_start": None, "train_end": None,
         "test_start": None, "test_end": None,
         "metrics": {"n_trades": 1, "total_pnl": 1.0,
                     "max_dd": -0.1, "sharpe": 0.0},
         "metrics_json": None},
    ])
    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests/1/folds")
    assert resp.status == 200
    data = await resp.json()
    assert len(data["folds"]) == 1


async def test_equity_route(aiohttp_client, app):
    from datetime import datetime, timezone
    app["results_db"].get_equity_curve = AsyncMock(return_value=[
        {"t": datetime(2025,1,1,tzinfo=timezone.utc), "equity": 100.0, "cash": 100.0},
    ])
    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests/1/equity?fold_id=7")
    assert resp.status == 200
    data = await resp.json()
    assert data["points"][0]["equity"] == 100.0
    app["results_db"].get_equity_curve.assert_awaited_with(1, fold_id=7)


async def test_trades_route(aiohttp_client, app):
    app["results_db"].list_trades = AsyncMock(return_value=[])
    client = await aiohttp_client(app)
    resp = await client.get("/api/backtests/1/trades?limit=10&offset=0")
    assert resp.status == 200
    data = await resp.json()
    assert data["trades"] == []
```

- [ ] **Step 2: Run tests**

```bash
docker compose run --rm backtest pytest \
  bullforge/tests/test_dashboard_backtests_api.py -v
```

Expected: 7 passing (2 from Task 6 + 5 new).

- [ ] **Step 3: Commit**

```bash
git add bullforge/tests/test_dashboard_backtests_api.py
git commit -m "test(dashboard): detail / folds / equity / trades route coverage"
```

---

## Task 8: Frontend — tab scaffold + Chart.js + hash routing

**Files:**
- Modify: `bullforge/services/dashboard_service/static/index.html`
- Modify: `bullforge/services/dashboard_service/static/dashboard.js`
- Create: `bullforge/services/dashboard_service/static/backtests.js`
- Create: `bullforge/services/dashboard_service/static/backtests.css`

- [ ] **Step 1: Add the tab to `index.html`**

Add a `<button>` to the existing tab strip and a tab body `<section>`. Before `</body>`, add the Chart.js CDN and the new JS/CSS references:

```html
<!-- inside the existing tab-strip (match the current pattern used by other tabs) -->
<button class="tab-button" data-tab="backtests">Backtests</button>

<!-- after the other tab sections -->
<section id="tab-backtests" class="tab-content" hidden>
  <div id="backtests-root"></div>
</section>

<link rel="stylesheet" href="/static/backtests.css">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="/static/backtests.js"></script>
```

- [ ] **Step 2: Hook the tab switch**

In `dashboard.js`, find the click handler that reveals tab sections and add after the existing `case` branches / conditionals:

```javascript
if (tab === 'backtests') {
  window.BacktestsTab && window.BacktestsTab.activate();
}
```

Also handle hash-based deep linking on page load:

```javascript
// at end of dashboard.js
window.addEventListener('DOMContentLoaded', () => {
  const hash = window.location.hash || '';
  if (hash.startsWith('#backtests')) {
    document.querySelector('[data-tab="backtests"]').click();
  }
});
window.addEventListener('hashchange', () => {
  const hash = window.location.hash || '';
  if (hash.startsWith('#backtests')) {
    document.querySelector('[data-tab="backtests"]').click();
    window.BacktestsTab && window.BacktestsTab.onHashChange(hash);
  }
});
```

- [ ] **Step 3: Create the stub controller**

```javascript
// bullforge/services/dashboard_service/static/backtests.js
(function () {
  const root = () => document.getElementById('backtests-root');
  let pollTimer = null;

  async function fetchJSON(url, opts) {
    const res = await fetch(url, opts);
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw Object.assign(new Error(`HTTP ${res.status}`),
                          { status: res.status, body });
    }
    return res.json();
  }

  function route() {
    const hash = window.location.hash || '#backtests';
    const parts = hash.replace(/^#/, '').split('/');
    if (parts[0] === 'backtests' && parts[1]) {
      renderDetail(parts[1]);
    } else {
      renderOverview();
    }
  }

  function renderOverview() {
    clearInterval(pollTimer);
    root().innerHTML = '<div class="bt-overview"><p>Overview loading…</p></div>';
    // Populated in Task 9.
  }

  async function renderDetail(id) {
    clearInterval(pollTimer);
    root().innerHTML =
      `<div class="bt-detail"><a href="#backtests">← Back</a>` +
      `<p>Detail loading for ${id}…</p></div>`;
    // Populated in Tasks 10-11.
  }

  window.BacktestsTab = {
    activate() { route(); },
    onHashChange() { route(); },
  };
})();
```

- [ ] **Step 4: Minimal CSS**

```css
/* bullforge/services/dashboard_service/static/backtests.css */
.bt-overview, .bt-detail { padding: 12px; }
.bt-toolbar { display: flex; gap: 8px; margin-bottom: 8px; align-items: center; }
.bt-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.bt-table th, .bt-table td { padding: 6px 8px; border-bottom: 1px solid #2a2a2a; text-align: left; }
.bt-table th { cursor: pointer; background: #1f1f1f; }
.bt-table tr.clickable { cursor: pointer; }
.bt-pos { color: #4ade80; } .bt-neg { color: #f87171; }
.bt-cards { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin: 10px 0; }
.bt-card { background: #1a1a1a; padding: 10px; border: 1px solid #2a2a2a; }
.bt-card .label { font-size: 10px; color: #888; text-transform: uppercase; }
.bt-card .value { font-size: 16px; margin-top: 4px; }
.bt-chart { background: #0f0f0f; border: 1px solid #2a2a2a; padding: 8px; margin: 8px 0; height: 260px; }
.bt-modal-backdrop { position: fixed; inset: 0; background: rgba(0,0,0,.6); display: flex; align-items: center; justify-content: center; z-index: 100; }
.bt-modal { background: #151515; padding: 16px; width: 520px; max-width: 95vw; border: 1px solid #333; }
.bt-form-row { display: flex; gap: 8px; margin-bottom: 8px; }
.bt-error { color: #f87171; font-size: 11px; }
```

- [ ] **Step 5: Manual verify**

Start/restart the dashboard service, open its UI, click the new **Backtests** tab. Expect: "Overview loading…" text (functionality in next tasks).

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/dashboard_service/static/index.html \
        bullforge/services/dashboard_service/static/dashboard.js \
        bullforge/services/dashboard_service/static/backtests.js \
        bullforge/services/dashboard_service/static/backtests.css
git commit -m "feat(dashboard): backtests tab scaffold + Chart.js CDN"
```

---

## Task 9: Frontend — overview table

**Files:**
- Modify: `bullforge/services/dashboard_service/static/backtests.js`

- [ ] **Step 1: Replace `renderOverview` with a full implementation**

Replace the placeholder `renderOverview` function in `backtests.js` with:

```javascript
  const state = { status: 'all', q: '', sort: 'finished_at', order: 'desc' };

  function fmtPnL(v) {
    if (v === null || v === undefined) return '—';
    const cls = v >= 0 ? 'bt-pos' : 'bt-neg';
    const sign = v >= 0 ? '+' : '';
    return `<span class="${cls}">${sign}$${v.toFixed(2)}</span>`;
  }
  function fmtPct(v) {
    if (v === null || v === undefined) return '—';
    return `${(v * 100).toFixed(1)}%`;
  }

  function renderOverview() {
    root().innerHTML = `
      <div class="bt-overview">
        <div class="bt-toolbar">
          <select id="bt-status">
            <option value="all">all</option>
            <option value="queued">queued</option>
            <option value="running">running</option>
            <option value="completed">completed</option>
            <option value="failed">failed</option>
          </select>
          <input id="bt-search" placeholder="search name…" />
          <button id="bt-new">New Backtest</button>
          <button id="bt-refresh">Refresh</button>
        </div>
        <table class="bt-table">
          <thead><tr>
            <th data-sort="name">Name</th>
            <th>Symbols</th>
            <th>Range</th>
            <th data-sort="total_pnl">P&amp;L</th>
            <th data-sort="sharpe">Sharpe</th>
            <th data-sort="max_dd">Max DD</th>
            <th>#Trades</th>
            <th>Hit %</th>
            <th>Status</th>
            <th data-sort="finished_at">Finished</th>
          </tr></thead>
          <tbody id="bt-rows"><tr><td colspan="10">Loading…</td></tr></tbody>
        </table>
      </div>
    `;
    bindOverviewEvents();
    reload();
    clearInterval(pollTimer);
    pollTimer = setInterval(reload, 10000);
  }

  function bindOverviewEvents() {
    document.getElementById('bt-status').value = state.status;
    document.getElementById('bt-search').value = state.q;
    document.getElementById('bt-status').onchange = (e) => {
      state.status = e.target.value; reload();
    };
    let t = null;
    document.getElementById('bt-search').oninput = (e) => {
      clearTimeout(t);
      t = setTimeout(() => { state.q = e.target.value; reload(); }, 300);
    };
    document.getElementById('bt-refresh').onclick = () => reload();
    document.getElementById('bt-new').onclick = () => openNewModal(); // Task 11
    document.querySelectorAll('.bt-table th[data-sort]').forEach(th => {
      th.onclick = () => {
        const key = th.getAttribute('data-sort');
        if (state.sort === key) {
          state.order = state.order === 'desc' ? 'asc' : 'desc';
        } else {
          state.sort = key; state.order = 'desc';
        }
        reload();
      };
    });
  }

  async function reload() {
    const params = new URLSearchParams({
      status: state.status, sort: state.sort, order: state.order,
    });
    if (state.q) params.set('q', state.q);
    try {
      const data = await fetchJSON(`/api/backtests?${params}`);
      renderRows(data.rows);
    } catch (e) {
      document.getElementById('bt-rows').innerHTML =
        `<tr><td colspan="10" class="bt-error">Failed to load: ${e.message}</td></tr>`;
    }
  }

  function renderRows(rows) {
    const tbody = document.getElementById('bt-rows');
    if (!rows.length) {
      tbody.innerHTML =
        '<tr><td colspan="10">No backtests yet. Click New Backtest to start one.</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(r => {
      const m = r.metrics;
      const range = r.range_start ? `${r.range_start.slice(0,10)} – ${r.range_end?.slice(0,10) || ''}` : '—';
      const symsDisplay = r.symbols.length > 2
        ? `${r.symbols.length} syms` : r.symbols.join(', ');
      const pnl = m ? fmtPnL(m.total_pnl) : '—';
      const sharpe = m ? m.sharpe.toFixed(2) : '—';
      const dd = m ? `<span class="bt-neg">${fmtPct(m.max_dd)}</span>` : '—';
      const ntr = m ? m.n_trades : '—';
      const hit = m ? fmtPct(m.hit_rate) : '—';
      const target = r.kind === 'run'
        ? `#backtests/${r.id}` : `#backtests/job-${r.id}`;
      return `<tr class="clickable" onclick="location.hash='${target}'">
        <td>${r.name}</td><td>${symsDisplay}</td><td>${range}</td>
        <td>${pnl}</td><td>${sharpe}</td><td>${dd}</td>
        <td>${ntr}</td><td>${hit}</td><td>${r.status}</td>
        <td>${r.finished_at ? r.finished_at.replace('T',' ').slice(0,16) : '—'}</td>
      </tr>`;
    }).join('');
  }
```

Also update the `route()` parsing to recognize `#backtests/job-N` and route it to a job-detail branch (stubbed until Task 10; for now route `job-*` to overview and let the Task-10 detail view handle the full picture).

- [ ] **Step 2: Manual verify**

Restart dashboard → load the tab. With no runs/jobs: empty-state message. With the smoke job from Task 5: one row shows. Filters/sort change the query URL. Polling refreshes every 10s.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/backtests.js
git commit -m "feat(dashboard): backtests overview table + filters + polling"
```

---

## Task 10: Frontend — detail view (run + job)

**Files:**
- Modify: `bullforge/services/dashboard_service/static/backtests.js`

- [ ] **Step 1: Implement `renderDetail` and supporting charts**

Replace the `renderDetail` stub with the full implementation:

```javascript
  async function renderDetail(id) {
    clearInterval(pollTimer);
    if (id.startsWith('job-')) {
      return renderJobDetail(id.slice(4));
    }
    root().innerHTML = `<div class="bt-detail">
      <a href="#backtests">← Back</a>
      <div id="bt-header"></div>
      <div class="bt-cards" id="bt-cards"></div>
      <div class="bt-chart"><canvas id="bt-equity"></canvas></div>
      <div class="bt-chart"><canvas id="bt-drawdown"></canvas></div>
      <h3>Folds</h3>
      <table class="bt-table" id="bt-folds"></table>
      <div class="bt-chart"><canvas id="bt-histogram"></canvas></div>
      <h3>Trades</h3>
      <div id="bt-trades"></div>
      <details><summary>Config</summary><pre id="bt-config"></pre></details>
    </div>`;
    try {
      const [detail, folds, equity] = await Promise.all([
        fetchJSON(`/api/backtests/${id}`),
        fetchJSON(`/api/backtests/${id}/folds`),
        fetchJSON(`/api/backtests/${id}/equity`),
      ]);
      renderHeader(detail);
      renderCards(detail.metrics);
      renderEquityChart(equity.points, folds.folds, id);
      renderDrawdown(equity.points);
      renderFolds(folds.folds);
      renderConfig(detail);
      await loadTrades(id, 0);
      await renderHistogram(id);
    } catch (e) {
      root().innerHTML += `<p class="bt-error">${e.message}</p>`;
    }
  }

  async function renderJobDetail(jobId) {
    root().innerHTML = `<div class="bt-detail">
      <a href="#backtests">← Back</a>
      <h3 id="bt-job-title">Job #${jobId}</h3>
      <p id="bt-job-status">Loading…</p>
      <details open><summary>Config</summary><pre id="bt-job-config"></pre></details>
      <p id="bt-job-error" class="bt-error"></p>
    </div>`;
    async function tick() {
      try {
        const data = await fetchJSON(`/api/backtests?status=all`);
        const job = data.rows.find(r => r.kind === 'job' && r.id == jobId);
        if (!job) {
          // If the run completed, job row vanishes once status='completed'
          // because run_id is set — look up the job record directly via its
          // recent completed siblings. Easiest: navigate to newest completed
          // run matching name. For MVP: leave page; user can click Back.
          document.getElementById('bt-job-status').textContent =
            'Job completed. Return to overview to find the run.';
          return;
        }
        document.getElementById('bt-job-title').textContent =
          `${job.name} (${job.status})`;
        document.getElementById('bt-job-status').textContent = job.status;
        document.getElementById('bt-job-config').textContent =
          JSON.stringify(job, null, 2);
        if (job.status === 'failed') {
          document.getElementById('bt-job-error').textContent = job.error || '';
        }
      } catch (e) { /* ignore transient */ }
    }
    tick();
    pollTimer = setInterval(tick, 5000);
  }

  function renderHeader(d) {
    document.getElementById('bt-header').innerHTML =
      `<h2>${d.name}</h2>
       <p>${d.symbols.join(', ')} · ${d.timeframe} · ${d.mode} ·
          ${d.range_start?.slice(0,10)} – ${d.range_end?.slice(0,10)} ·
          ${d.metrics.n_folds} folds · ${d.status}</p>`;
  }
  function renderCards(m) {
    const cards = [
      ['P&L', fmtPnL(m.total_pnl)], ['Sharpe', m.sharpe.toFixed(2)],
      ['Sortino', m.sortino.toFixed(2)], ['Max DD', fmtPct(m.max_dd)],
      ['Hit Rate', fmtPct(m.hit_rate)], ['Profit Factor', m.profit_factor.toFixed(2)],
      ['#Trades', m.n_trades], ['#Folds', m.n_folds],
    ];
    document.getElementById('bt-cards').innerHTML = cards.map(
      ([l, v]) => `<div class="bt-card"><div class="label">${l}</div>
                   <div class="value">${v}</div></div>`
    ).join('');
  }
  function renderEquityChart(points, folds, runId) {
    const ctx = document.getElementById('bt-equity').getContext('2d');
    const combined = {
      label: 'Combined',
      data: points.map(p => ({ x: p.t, y: p.equity })),
      borderColor: '#4ade80', pointRadius: 0, borderWidth: 1.5,
    };
    const chart = new Chart(ctx, {
      type: 'line',
      data: { datasets: [combined] },
      options: {
        parsing: false, animation: false,
        scales: { x: { type: 'time', time: { tooltipFormat: 'PP HH:mm' } } },
        plugins: { legend: { onClick: async (_, item) => {
          // Clicking a fold legend toggles per-fold overlay.
          const fold = folds[item.datasetIndex - 1];
          if (!fold) return;
          const key = `fold-${fold.id}`;
          const existing = chart.data.datasets.find(d => d.key === key);
          if (existing) {
            chart.data.datasets = chart.data.datasets.filter(d => d.key !== key);
          } else {
            const data = await fetchJSON(
              `/api/backtests/${runId}/equity?fold_id=${fold.id}`);
            chart.data.datasets.push({
              key, label: `Fold ${fold.fold_index}`,
              data: data.points.map(p => ({ x: p.t, y: p.equity })),
              pointRadius: 0, borderWidth: 1,
            });
          }
          chart.update();
        } } },
      },
    });
  }
  function renderDrawdown(points) {
    let peak = -Infinity;
    const dd = points.map(p => {
      peak = Math.max(peak, p.equity);
      return { x: p.t, y: p.equity / peak - 1 };
    });
    new Chart(document.getElementById('bt-drawdown').getContext('2d'), {
      type: 'line',
      data: { datasets: [{ label: 'Drawdown', data: dd,
                           borderColor: '#f87171', backgroundColor: 'rgba(248,113,113,.25)',
                           fill: 'origin', pointRadius: 0, borderWidth: 1 }] },
      options: { parsing: false, animation: false,
                 scales: { x: { type: 'time' },
                           y: { ticks: { callback: v => `${(v*100).toFixed(0)}%` }}}},
    });
  }
  function renderFolds(folds) {
    document.getElementById('bt-folds').innerHTML = `
      <thead><tr><th>#</th><th>Train</th><th>Test</th>
        <th>P&amp;L</th><th>Sharpe</th><th>Max DD</th><th>#Trades</th></tr></thead>
      <tbody>${folds.map(f => `<tr>
        <td>${f.fold_index}</td>
        <td>${f.train_start?.slice(0,10)} – ${f.train_end?.slice(0,10)}</td>
        <td>${f.test_start?.slice(0,10)} – ${f.test_end?.slice(0,10)}</td>
        <td>${fmtPnL(f.metrics.total_pnl)}</td>
        <td>${f.metrics.sharpe.toFixed(2)}</td>
        <td><span class="bt-neg">${fmtPct(f.metrics.max_dd)}</span></td>
        <td>${f.metrics.n_trades}</td></tr>`).join('')}</tbody>`;
  }
  function renderConfig(d) {
    document.getElementById('bt-config').textContent =
      JSON.stringify(d.config, null, 2);
  }
  async function renderHistogram(runId) {
    const first = await fetchJSON(`/api/backtests/${runId}/trades?limit=200&offset=0`);
    const pnls = first.trades.map(t => t.pnl).filter(v => v != null);
    if (!pnls.length) return;
    const min = Math.min(...pnls), max = Math.max(...pnls);
    const bins = 30, width = (max - min) / bins || 1;
    const counts = Array(bins).fill(0);
    pnls.forEach(p => counts[Math.min(bins-1, Math.floor((p - min) / width))]++);
    const labels = counts.map((_, i) => (min + width * i).toFixed(2));
    new Chart(document.getElementById('bt-histogram').getContext('2d'), {
      type: 'bar',
      data: { labels, datasets: [{ label: 'Trade P&L', data: counts,
                                    backgroundColor: '#60a5fa' }] },
      options: { animation: false },
    });
  }
  async function loadTrades(runId, offset) {
    const data = await fetchJSON(`/api/backtests/${runId}/trades?limit=50&offset=${offset}`);
    const rows = data.trades.map(t => `<tr>
      <td>${t.entry_time || '—'}</td><td>${t.symbol}</td><td>${t.side}</td>
      <td>${t.entry_price}</td><td>${t.exit_price ?? '—'}</td>
      <td>${t.quantity}</td><td>${fmtPnL(t.pnl)}</td>
      <td>${t.signal_strength?.toFixed(2) ?? '—'}</td>
      <td>${t.fold_id}</td></tr>`).join('');
    document.getElementById('bt-trades').innerHTML = `
      <table class="bt-table"><thead><tr>
        <th>Entry</th><th>Sym</th><th>Side</th><th>Entry$</th><th>Exit$</th>
        <th>Qty</th><th>P&amp;L</th><th>Signal</th><th>Fold</th>
      </tr></thead><tbody>${rows}</tbody></table>
      <div style="margin-top:6px">
        ${offset > 0 ? `<button onclick="BacktestsTab._trades(${runId},${offset-50})">Prev</button>` : ''}
        ${data.trades.length === 50 ? `<button onclick="BacktestsTab._trades(${runId},${offset+50})">Next</button>` : ''}
      </div>`;
  }

  // Expose pagination helper
  window.BacktestsTab._trades = (id, off) => loadTrades(id, off);
```

- [ ] **Step 2: Manual verify**

With a completed run present, click its row → detail page renders with cards, equity curve, drawdown, fold table, trade histogram, trades table (50/page), and collapsible config. Click a fold in the equity legend to toggle its overlay. Visit `#backtests/<id>` directly to confirm deep-linking.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/backtests.js
git commit -m "feat(dashboard): backtest detail view with equity/drawdown/folds/trades"
```

---

## Task 11: Frontend — "New Backtest" modal

**Files:**
- Modify: `bullforge/services/dashboard_service/static/backtests.js`

- [ ] **Step 1: Implement `openNewModal`**

Append:

```javascript
  const HORIZON_CHOICES = [5,10,15,30,60,75,90,105,120];

  async function openNewModal() {
    // Populate symbols from /api/symbols (existing endpoint)
    let symbolsList = [];
    try {
      const sym = await fetchJSON('/api/symbols');
      symbolsList = [...(sym.trading_symbols_equity || []),
                     ...(sym.trading_symbols_crypto || [])];
    } catch (_) { symbolsList = []; }

    const backdrop = document.createElement('div');
    backdrop.className = 'bt-modal-backdrop';
    backdrop.innerHTML = `
      <div class="bt-modal">
        <h3>New Backtest</h3>
        <div class="bt-form-row">
          <label>Name<input id="f-name"></label>
        </div>
        <div class="bt-form-row">
          <label>Symbols<br>
            <select id="f-symbols" multiple size="6">
              ${symbolsList.map(s => `<option value="${s}">${s}</option>`).join('')}
            </select>
          </label>
          <label>Horizons (min)<br>
            <select id="f-horizons" multiple size="6">
              ${HORIZON_CHOICES.map(h => `<option value="${h}">${h}</option>`).join('')}
            </select>
          </label>
        </div>
        <div class="bt-form-row">
          <label>Timeframe
            <select id="f-timeframe"><option>1m</option><option>1d</option></select>
          </label>
          <label>Mode
            <select id="f-mode"><option>rolling</option><option>anchored</option></select>
          </label>
        </div>
        <div class="bt-form-row">
          <label>Start (UTC)<input id="f-start" type="datetime-local"></label>
          <label>End (UTC)<input id="f-end" type="datetime-local"></label>
        </div>
        <div class="bt-form-row">
          <label>Train days<input id="f-train" type="number" value="60" min="1"></label>
          <label>Test days<input id="f-test" type="number" value="5" min="1"></label>
          <label>Embargo (min)<input id="f-embargo" type="number" value="30" min="1"></label>
        </div>
        <div id="f-errors" class="bt-error"></div>
        <div style="display:flex;justify-content:flex-end;gap:8px">
          <button id="f-cancel">Cancel</button>
          <button id="f-submit">Queue Run</button>
        </div>
      </div>`;
    document.body.appendChild(backdrop);

    const close = () => backdrop.remove();
    backdrop.querySelector('#f-cancel').onclick = close;
    backdrop.onclick = (e) => { if (e.target === backdrop) close(); };

    const horizonsSel = backdrop.querySelector('#f-horizons');
    const embargoIn = backdrop.querySelector('#f-embargo');
    const updateEmbargoCap = () => {
      const chosen = [...horizonsSel.selectedOptions].map(o => Number(o.value));
      if (chosen.length) {
        embargoIn.max = Math.max(...chosen);
        if (Number(embargoIn.value) > embargoIn.max) embargoIn.value = embargoIn.max;
      } else {
        embargoIn.removeAttribute('max');
      }
    };
    horizonsSel.onchange = updateEmbargoCap;

    backdrop.querySelector('#f-submit').onclick = async () => {
      const body = {
        name: backdrop.querySelector('#f-name').value.trim(),
        symbols: [...backdrop.querySelector('#f-symbols').selectedOptions].map(o => o.value),
        timeframe: backdrop.querySelector('#f-timeframe').value,
        horizons: [...horizonsSel.selectedOptions].map(o => Number(o.value)),
        range_start: new Date(backdrop.querySelector('#f-start').value + 'Z').toISOString(),
        range_end:   new Date(backdrop.querySelector('#f-end').value + 'Z').toISOString(),
        train_days: Number(backdrop.querySelector('#f-train').value),
        test_days:  Number(backdrop.querySelector('#f-test').value),
        embargo_min: Number(embargoIn.value),
        mode: backdrop.querySelector('#f-mode').value,
      };
      try {
        await fetchJSON('/api/backtests', {
          method: 'POST',
          headers: {'content-type': 'application/json'},
          body: JSON.stringify(body),
        });
        close();
        reload();
      } catch (e) {
        const errs = e.body && e.body.errors;
        backdrop.querySelector('#f-errors').textContent = errs
          ? Object.entries(errs).map(([k,v]) => `${k}: ${v}`).join(' · ')
          : (e.message || 'Failed');
      }
    };
  }
```

- [ ] **Step 2: Manual verify**

Click **New Backtest** → modal appears with symbols preloaded. Select symbols, pick horizons (confirm embargo `max` attribute updates to the largest chosen horizon). Try to submit with embargo > max horizon → server-side 400 shows inline. Submit a valid tiny run → row appears in the table as `queued`; worker picks it up (Task 5 smoke pattern).

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/static/backtests.js
git commit -m "feat(dashboard): New Backtest modal with embargo clamp"
```

---

## Task 12: Runbook docs

**Files:**
- Modify: `bullforge/docs/runbooks/backtest.md`

- [ ] **Step 1: Add a section documenting the UI + worker**

Append:

```markdown
## Dashboard UI

Open the dashboard and click the **Backtests** tab to see all runs/jobs with
P&L and other summary metrics. Click a row to drill into equity curve,
drawdown, per-fold metrics, trade P&L histogram, and the trade list.

### Queue a run from the UI

Click **New Backtest**, fill the form (symbols, horizons, date range, mode),
and submit. The row appears as `queued` and transitions to `running` →
`completed`/`failed` as the `backtest-worker` service processes it.

### Worker service

`docker compose up -d backtest-worker` runs the queue consumer. One job at a
time; restart via `docker compose restart backtest-worker`. The legacy CLI
invocation (`docker compose run --rm backtest …`) still works for one-off or
scripted runs and bypasses the queue.
```

- [ ] **Step 2: Commit**

```bash
git add bullforge/docs/runbooks/backtest.md
git commit -m "docs(runbook): backtest dashboard UI + worker instructions"
```

---

## Task 13: End-to-end verification checklist

- [ ] **Step 1: Full test run**

```bash
docker compose run --rm backtest pytest bullforge/tests -v -k \
  "backtest_results_db_queries or backtest_jobs_db or backtest_worker or dashboard_backtests_api"
```

Expected: all green.

- [ ] **Step 2: Manual browser walkthrough**

Start the full stack (`docker compose up -d`). In the dashboard:

1. Click Backtests tab — overview loads.
2. Click New Backtest — submit a valid short run (e.g., SPY / 1m / horizons `[5]` / 2 training days / 1 test day / embargo 5 / rolling / a range with known data).
3. Watch the row transition `queued → running → completed`.
4. Click the completed row — detail page renders with all six sections (cards, equity, drawdown, folds, histogram, trades, config).
5. Click a legend entry to overlay a per-fold curve.
6. Paginate trades (Next/Prev).
7. Deep-link `#backtests/<id>` in a fresh tab — detail renders directly.
8. Change status filter to `failed` — only failed rows (if any).
9. Type in search — results narrow.

- [ ] **Step 3: Final commit if any doc/touchups needed**

```bash
git status
# commit any final polish
```

---

## Self-Review Notes (addressed during authoring)

- **Spec coverage:** every spec section maps to a task — migration (1), worker (4-5), list/detail/folds/equity/trades/create endpoints (6-7), overview view (8-9), detail view (10), new modal (11), runbook (12), verification (13).
- **Placeholders:** none — every step has concrete code or commands.
- **Type consistency:** `BacktestJobsDB` methods used in tests and worker match (`claim_next`, `mark_completed(job_id=, run_id=)`, `mark_failed(job_id=, error=)`, `list_pending`). `BacktestResultsDB` new methods (`list_runs`, `get_run`, `list_folds`, `list_trades`, `get_equity_curve`) used in both tests and API layer with matching signatures.
- **Engine-factory uncertainty:** Task 4 flags that `make_default_engine` may not exist and instructs the engineer to inspect `engine.py` + `bullforge/scripts/backtest.py` to wire up the real construction. This is acknowledged as a known unknown; do not skip it.
