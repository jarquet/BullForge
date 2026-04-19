# Backtest Worker Pool Split + Throughput Tuning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route GPU-needing backtest jobs to a GPU-allocated container, keep CPU jobs off the GPU container, and saturate the DGX's 20 cores while a queue of backfill runs drains — via a multi-core-native default model plus two pipelined CPU workers.

**Architecture:** New `worker_pool` column on `backtest_jobs` classifies each job at insert time via `cpu_or_gpu(model_type)`. Two worker services (`backtest-worker-cpu` and `backtest-worker-gpu`) each read `BACKTEST_WORKER_POOL` from env and only claim their pool's jobs. Default model switches to `HistGradientBoostingClassifier` (OpenMP-parallel per fit). Container env caps `OMP_NUM_THREADS` / `OPENBLAS_NUM_THREADS` / `MKL_NUM_THREADS` per worker so N workers × K threads never exceeds total cores.

**Tech Stack:** Python 3.12, sklearn, asyncpg, Postgres/TimescaleDB, Docker Compose v2.

**Spec:** [docs/superpowers/specs/2026-04-18-backtest-worker-pool-and-throughput-design.md](../specs/2026-04-18-backtest-worker-pool-and-throughput-design.md)

---

## Pre-Task: Worktree

The execution skill creates `.worktrees/worker-pool` branched from master as `feat/backtest-worker-pool`. All task paths below are relative to that worktree.

---

## Task 1: Migration + `cpu_or_gpu` classifier

**Files:**
- Create: `bullforge/migrations/019_backtest_jobs_worker_pool.sql`
- Create: `bullforge/services/backtest_service/worker_pool.py`
- Create: `bullforge/tests/test_worker_pool.py`

- [ ] **Step 1: Write the failing classifier test**

Create `bullforge/tests/test_worker_pool.py` with:

```python
import pytest

from services.backtest_service.worker_pool import cpu_or_gpu


@pytest.mark.parametrize("model_type,expected", [
    (None, "cpu"),
    ("gradient_boost", "cpu"),
    ("hist_gradient_boost", "cpu"),
    ("logistic_regression", "cpu"),
    ("lightgbm", "cpu"),
    ("baseline", "cpu"),
    ("lstm", "gpu"),
    ("mlp", "gpu"),
    ("meta_entry", "gpu"),
    ("meta_exec", "gpu"),
    ("LSTM", "gpu"),            # case-insensitive
    ("unknown_future", "cpu"),  # unknown → default to cpu
    ("", "cpu"),
])
def test_cpu_or_gpu(model_type, expected):
    assert cpu_or_gpu(model_type) == expected
```

- [ ] **Step 2: Run to verify failure**

```bash
cd <repo>/.worktrees/worker-pool/bullforge
python -m pytest tests/test_worker_pool.py -v
```

Expected: `ModuleNotFoundError: No module named 'services.backtest_service.worker_pool'`.

- [ ] **Step 3: Implement the classifier**

Create `bullforge/services/backtest_service/worker_pool.py` with:

```python
"""Classifies a backtest job's required worker pool based on its model_type."""
from __future__ import annotations

_GPU_MODEL_TYPES = frozenset({"lstm", "mlp", "meta_entry", "meta_exec"})


def cpu_or_gpu(model_type: str | None) -> str:
    """Maps a model_type string to the worker pool it should run on.

    None / empty / unknown / CPU-native types (gradient_boost,
    hist_gradient_boost, logistic_regression, lightgbm, baseline) → 'cpu'.
    LSTM / MLP / meta-models → 'gpu'.
    """
    if model_type and model_type.lower() in _GPU_MODEL_TYPES:
        return "gpu"
    return "cpu"
```

- [ ] **Step 4: Run tests to verify pass**

```bash
python -m pytest tests/test_worker_pool.py -v
```

Expected: 13 passed.

- [ ] **Step 5: Write the migration**

Create `bullforge/migrations/019_backtest_jobs_worker_pool.sql` with:

```sql
-- 019_backtest_jobs_worker_pool.sql
--
-- Adds a `worker_pool` column to backtest_jobs so CPU-native jobs go to
-- backtest-worker-cpu containers and GPU-needing jobs (LSTM/MLP/meta)
-- go to backtest-worker-gpu containers. Existing rows backfill to 'cpu'
-- via the column default, matching today's behavior.
ALTER TABLE backtest_jobs
    ADD COLUMN IF NOT EXISTS worker_pool TEXT NOT NULL DEFAULT 'cpu'
        CHECK (worker_pool IN ('cpu', 'gpu'));

CREATE INDEX IF NOT EXISTS idx_backtest_jobs_pool_status_created
    ON backtest_jobs (worker_pool, status, created_at);
```

- [ ] **Step 6: Commit**

```bash
git add bullforge/migrations/019_backtest_jobs_worker_pool.sql \
        bullforge/services/backtest_service/worker_pool.py \
        bullforge/tests/test_worker_pool.py
git commit -m "feat(backtest): add worker_pool column + cpu_or_gpu classifier"
```

---

## Task 2: Thread `worker_pool` through `BacktestJobsDB`

**Files:**
- Modify: `bullforge/services/backtest_service/jobs_db.py` (methods `insert_job`, `claim_next`)
- Modify: `bullforge/tests/test_backtest_jobs_db.py` (update existing tests, add new)

- [ ] **Step 1: Write failing tests**

Append to the end of `bullforge/tests/test_backtest_jobs_db.py`:

```python
async def test_insert_job_defaults_to_cpu(db):
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(name="t", config={})
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT worker_pool FROM backtest_jobs WHERE id = $1", job_id,
        )
    assert row["worker_pool"] == "cpu"


async def test_insert_job_derives_pool_from_model_type(db):
    jobs = BacktestJobsDB(db)
    cpu_id = await jobs.insert_job(
        name="cpu_job", config={"model_type": "gradient_boost"},
    )
    gpu_id = await jobs.insert_job(
        name="gpu_job", config={"model_type": "lstm"},
    )
    async with db.pool.acquire() as conn:
        cpu_row = await conn.fetchrow(
            "SELECT worker_pool FROM backtest_jobs WHERE id = $1", cpu_id,
        )
        gpu_row = await conn.fetchrow(
            "SELECT worker_pool FROM backtest_jobs WHERE id = $1", gpu_id,
        )
    assert cpu_row["worker_pool"] == "cpu"
    assert gpu_row["worker_pool"] == "gpu"


async def test_insert_job_explicit_worker_pool_overrides_derivation(db):
    jobs = BacktestJobsDB(db)
    job_id = await jobs.insert_job(
        name="override", config={"model_type": "gradient_boost"},
        worker_pool="gpu",
    )
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT worker_pool FROM backtest_jobs WHERE id = $1", job_id,
        )
    assert row["worker_pool"] == "gpu"


async def test_claim_next_filters_by_pool(db):
    jobs = BacktestJobsDB(db)
    cpu_id = await jobs.insert_job(
        name="cpu_j", config={"model_type": "gradient_boost"},
    )
    gpu_id = await jobs.insert_job(
        name="gpu_j", config={"model_type": "lstm"},
    )

    claimed_cpu = await jobs.claim_next(worker_pool="cpu")
    assert claimed_cpu is not None and claimed_cpu["id"] == cpu_id

    claimed_gpu = await jobs.claim_next(worker_pool="gpu")
    assert claimed_gpu is not None and claimed_gpu["id"] == gpu_id

    # Nothing left in either pool.
    assert await jobs.claim_next(worker_pool="cpu") is None
    assert await jobs.claim_next(worker_pool="gpu") is None
```

Also update the four existing tests that call `claim_next()` with no argument. Change every call to `await jobs.claim_next()` into `await jobs.claim_next(worker_pool="cpu")`. The tests to update are:
- `test_insert_and_claim_roundtrip` (lines 48, 54)
- `test_mark_completed` (line 62)
- `test_mark_failed` (line 73)
- `test_link_run_excludes_from_pending` (line 94)

- [ ] **Step 2: Run to verify failure**

```bash
python -m pytest tests/test_backtest_jobs_db.py -v
```

Expected: existing tests ERROR (the `claim_next()` call is still arg-less) AND the four new tests FAIL (TypeError: unexpected `worker_pool`). Fix the existing tests first per Step 1's instruction; then the new tests FAIL alone.

- [ ] **Step 3: Update `insert_job`**

In `bullforge/services/backtest_service/jobs_db.py`, update imports:

```python
from __future__ import annotations

import json
from typing import Optional

from shared.db import Database
from services.backtest_service.worker_pool import cpu_or_gpu
```

Replace the entire `insert_job` method (currently at line 15) with:

```python
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
```

- [ ] **Step 4: Update `claim_next`**

Replace the `claim_next` method (currently at line 30) with:

```python
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
```

- [ ] **Step 5: Run all jobs-db tests**

```bash
python -m pytest tests/test_backtest_jobs_db.py -v
```

Expected: all tests pass — the original 5 (updated) plus the 4 new ones.

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/backtest_service/jobs_db.py \
        bullforge/tests/test_backtest_jobs_db.py
git commit -m "feat(backtest): worker_pool kwarg on insert_job + claim_next"
```

---

## Task 3: Thread `worker_pool` through the worker process

**Files:**
- Modify: `bullforge/services/backtest_service/worker.py`
- Modify: `bullforge/tests/test_backtest_worker.py` (update existing test calls)

- [ ] **Step 1: Check existing worker tests for `claim_next` call sites**

```bash
grep -n "claim_next" bullforge/tests/test_backtest_worker.py
```

Any calls there of form `.claim_next()` (no args) must also become `.claim_next(worker_pool="cpu")`. Fix inline now before changing the worker so the tests continue to work.

- [ ] **Step 2: Update `run_one` signature**

In `bullforge/services/backtest_service/worker.py`, replace the entire `run_one` function (starting at line 48) with:

```python
async def run_one(
    jobs: BacktestJobsDB,
    *,
    engine_factory: Callable[[BacktestConfig], BacktestEngine],
    worker_pool: str,
) -> bool:
    """Claim and run exactly one job from `worker_pool`. Returns True iff a job was processed."""
    job = await jobs.claim_next(worker_pool=worker_pool)
    if job is None:
        return False

    job_id = job["id"]
    try:
        cfg = _build_config({
            "name": job["name"],
            "custom_name": job.get("custom_name"),
            **job["config"],
        })
        engine = engine_factory(cfg)

        async def _link(run_id: int) -> None:
            await jobs.link_run(job_id=job_id, run_id=run_id)

        run_id = await engine.run(cfg, on_run_created=_link)
        await jobs.mark_completed(job_id=job_id, run_id=int(run_id))
        logger.info("backtest job %s -> run %s completed", job_id, run_id)
    except Exception as exc:
        tb = traceback.format_exc()
        logger.exception("backtest job %s failed", job_id)
        await jobs.mark_failed(job_id=job_id, error=f"{exc}\n{tb}")
    return True
```

- [ ] **Step 3: Update `main()` to read env var and pass the pool through**

Add `os` to the imports at the top:

```python
import os
```

(near the other stdlib imports — line 4 area).

In `main()`, add this block right after `settings = Settings()` (currently around line 85), before the `live_db = Database(settings)` line:

```python
    worker_pool = os.getenv("BACKTEST_WORKER_POOL", "cpu").lower()
    if worker_pool not in ("cpu", "gpu"):
        raise ValueError(
            f"BACKTEST_WORKER_POOL must be 'cpu' or 'gpu', got {worker_pool!r}"
        )
    logger.info("starting backtest worker for pool=%s", worker_pool)
```

Then find the `while not stop.is_set():` loop (line 120) and change the `run_one` call from:

```python
            did = await run_one(jobs, engine_factory=_engine_factory)
```

to:

```python
            did = await run_one(
                jobs, engine_factory=_engine_factory, worker_pool=worker_pool,
            )
```

- [ ] **Step 4: Run worker tests**

```bash
python -m pytest tests/test_backtest_worker.py -v
```

Expected: all pass. If any test calls `run_one` directly without a `worker_pool` kwarg, update the test to pass `worker_pool="cpu"`.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/backtest_service/worker.py bullforge/tests/test_backtest_worker.py
git commit -m "feat(backtest): worker reads BACKTEST_WORKER_POOL env, passes to claim_next"
```

---

## Task 4: Switch default CPU model to `HistGradientBoostingClassifier`

**Files:**
- Modify: `bullforge/services/model_service/model_factory.py`
- Test via existing `bullforge/tests/test_model_factory.py` (no changes needed — assertions are structural)

- [ ] **Step 1: Run the existing factory tests to confirm baseline**

```bash
python -m pytest tests/test_model_factory.py -v
```

Expected: all pass with the current `GradientBoostingClassifier`.

- [ ] **Step 2: Swap the import and constructor**

In `bullforge/services/model_service/model_factory.py`, change line 4 from:

```python
from sklearn.ensemble import GradientBoostingClassifier
```

to:

```python
from sklearn.ensemble import HistGradientBoostingClassifier
```

Replace the `"gradient_boost"` branch (lines 16-22) with:

```python
    if model_type == "gradient_boost":
        return Pipeline([
            ("scaler", StandardScaler()),
            ("classifier", HistGradientBoostingClassifier(
                max_iter=100, max_depth=3, learning_rate=0.1, random_state=42,
            )),
        ])
```

`max_iter` replaces `n_estimators`. All other hyperparameters carry over directly. `HistGradientBoostingClassifier` exposes the same `fit` / `predict` / `predict_proba` methods, so the existing pipeline, inference engine, and model-store code need no changes.

- [ ] **Step 3: Run the existing factory tests again**

```bash
python -m pytest tests/test_model_factory.py -v
```

Expected: all 10 tests still pass. The structural assertions (`isinstance(model, Pipeline)`, step names `scaler`/`classifier`, synthetic `fit` + `predict` + `predict_proba`) are algorithm-agnostic.

- [ ] **Step 4: Run the wider model tests**

```bash
python -m pytest tests/test_model_db.py tests/test_trainer.py tests/test_inference.py -v
```

Expected: all pass. No code references `GradientBoostingClassifier` by name outside `model_factory.py`.

- [ ] **Step 5: Commit**

```bash
git add bullforge/services/model_service/model_factory.py
git commit -m "feat(model): default gradient_boost uses HistGradientBoostingClassifier"
```

---

## Task 5: Split `backtest-worker` into CPU and GPU services in Compose

**Files:**
- Modify: `bullforge/docker-compose.yml`

- [ ] **Step 1: Find the existing `backtest-worker` service block**

```bash
grep -n "backtest-worker:" bullforge/docker-compose.yml
```

Open `bullforge/docker-compose.yml` and locate the full `backtest-worker:` block (starts at the line printed above, typically ends right before `volumes:` at the end of the file or before the next service).

- [ ] **Step 2: Replace `backtest-worker` with the two new services**

Delete the entire existing `backtest-worker:` block and insert in its place:

```yaml
  backtest-worker-cpu:
    image: bullforge-backtest
    build:
      context: .
      dockerfile: services/backtest_service/Dockerfile
    entrypoint: ["python", "-m", "services.backtest_service.worker"]
    env_file:
      - .env
    environment:
      TIMESCALEDB_HOST: timescaledb
      REDIS_HOST: redis
      BACKTEST_WORKER_POOL: cpu
      OMP_NUM_THREADS: "${CPU_THREADS:-10}"
      OPENBLAS_NUM_THREADS: "${CPU_THREADS:-10}"
      MKL_NUM_THREADS: "${CPU_THREADS:-10}"
    depends_on:
      timescaledb:
        condition: service_healthy
      redis:
        condition: service_healthy
    deploy:
      replicas: ${CPU_WORKERS:-2}
    restart: unless-stopped

  backtest-worker-gpu:
    image: bullforge-backtest
    build:
      context: .
      dockerfile: services/backtest_service/Dockerfile
    entrypoint: ["python", "-m", "services.backtest_service.worker"]
    env_file:
      - .env
    environment:
      TIMESCALEDB_HOST: timescaledb
      REDIS_HOST: redis
      BACKTEST_WORKER_POOL: gpu
      OMP_NUM_THREADS: "${GPU_THREADS:-4}"
      OPENBLAS_NUM_THREADS: "${GPU_THREADS:-4}"
      MKL_NUM_THREADS: "${GPU_THREADS:-4}"
    depends_on:
      timescaledb:
        condition: service_healthy
      redis:
        condition: service_healthy
    ipc: host
    ulimits:
      memlock: -1
      stack: 67108864
    deploy:
      replicas: ${GPU_WORKERS:-1}
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
    restart: unless-stopped
```

Indentation MUST match the surrounding services (2 spaces for the service name, 4 spaces for properties). If the file uses different indentation, match what's already there.

- [ ] **Step 3: Validate the compose file**

```bash
cd <repo>/.worktrees/worker-pool/bullforge
docker compose config > /tmp/compose-parsed.yml
grep -E "^\s+(backtest-worker-cpu|backtest-worker-gpu|backtest-worker):" /tmp/compose-parsed.yml
```

Expected output:
```
  backtest-worker-cpu:
  backtest-worker-gpu:
```
(No line with plain `backtest-worker:`.)

If `docker compose config` errors, fix the YAML indentation and try again.

- [ ] **Step 4: Commit**

```bash
git add bullforge/docker-compose.yml
git commit -m "ops(compose): split backtest-worker into cpu+gpu replica pools"
```

---

## Task 6: Document the new env vars in `.env.example`

**Files:**
- Modify: `bullforge/.env.example`

- [ ] **Step 1: Append the new section**

Add to the end of `bullforge/.env.example`:

```
# Backtest worker pool sizing
# CPU pool: two workers pipeline DB I/O behind training. Each fit uses
# CPU_THREADS cores via OpenMP/BLAS; CPU_WORKERS * CPU_THREADS should
# stay ≤ total cores on the host.
CPU_WORKERS=2
CPU_THREADS=10

# GPU pool: one worker (most hosts have one GPU); small CPU budget for
# data prep since fits run on GPU.
GPU_WORKERS=1
GPU_THREADS=4
```

- [ ] **Step 2: Commit**

```bash
git add bullforge/.env.example
git commit -m "docs(env): document CPU/GPU worker pool env vars"
```

---

## Task 7: Deploy to DGX and smoke-test

**Files:** none modified — deployment + verification only.

- [ ] **Step 1: Pull on the host**

On the DGX Spark:

```bash
cd ~/BullForge/bullforge
git pull
```

- [ ] **Step 2: Add pool sizing to the host's `.env`**

Append to `~/BullForge/bullforge/.env` (create if needed):

```
CPU_WORKERS=2
CPU_THREADS=10
GPU_WORKERS=1
GPU_THREADS=4
```

- [ ] **Step 3: Tear down the old worker, bring up the new ones**

```bash
docker compose down backtest-worker 2>/dev/null || true
docker compose up -d --build backtest-worker-cpu backtest-worker-gpu
```

Expected: two `backtest-worker-cpu` replicas and one `backtest-worker-gpu` replica running.

Verify:
```bash
docker compose ps | grep backtest-worker
```

- [ ] **Step 4: Submit a standard CPU backtest from the dashboard**

Any existing single-symbol backfill run. Watch logs:

```bash
docker compose logs -f backtest-worker-cpu
```

Expected:
- Log line `starting backtest worker for pool=cpu` from one of the two replicas.
- Training completes faster than before (HistGBC + 10 threads vs single-threaded GBC).
- The other cpu replica picks up the next queued job if you submit two in a row.

- [ ] **Step 5: Verify the GPU worker is idle but ready**

```bash
docker compose logs backtest-worker-gpu | tail -20
```

Expected: `starting backtest worker for pool=gpu`, then periodic idle polling. No errors. No CPU jobs claimed.

- [ ] **Step 6: Verify DB state**

```bash
docker compose exec timescaledb psql -U bullforge -d bullforge -c \
  "SELECT worker_pool, status, COUNT(*) FROM backtest_jobs GROUP BY 1, 2 ORDER BY 1, 2;"
```

Expected: `cpu | completed | N` for the test jobs. No `gpu | ...` unless a GPU job has been submitted.

- [ ] **Step 7: Report back**

- Commits on the branch
- Timing comparison (old worker vs new cpu pool) for the smoke-test backtest
- `docker compose ps` output showing 2 cpu + 1 gpu workers running
- Any anomalies

---
