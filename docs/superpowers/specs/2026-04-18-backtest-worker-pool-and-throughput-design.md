# Backtest Worker Pool Split + Throughput Tuning — Design

**Date:** 2026-04-18
**Status:** Approved (pending user spec review)
**Scope:** Python + SQL + docker-compose. Backend only. No dashboard changes.

## Goal

Maximize DGX Spark utilization while draining a queue of backfill runs, while keeping concurrent backtest runs safe. Route GPU-needing jobs (LSTM / MLP) to a GPU-allocated container; keep CPU jobs off the GPU container; saturate the CPU during queue drain by pairing a multi-core-native model with a small number of worker replicas that pipeline DB I/O behind training.

## Motivation

Two independent problems to solve together:

1. **GPU reservation is wasted.** The single `backtest-worker` compose service reserves 1 Nvidia GPU. Every job currently trains `gradient_boost` (CPU sklearn), so the GPU sits idle. Future LSTM / MLP work should go there, not to a CPU worker.
2. **A single backtest uses 1 CPU core.** `GradientBoostingClassifier` is single-threaded and the `for symbol: for horizon: await trainer.train(...)` loop in [engine.py:128-133](bullforge/services/backtest_service/engine.py) is serial. On a 20-core DGX Spark, a backfill queue draining one job at a time uses 5 % of the box. Increasing concurrent jobs is one answer, but the user's actual goal is throughput while draining — not parallel UX.

Concurrent backtest safety shipped yesterday (commits `4ae4bfe..b3e92aa`); this design assumes that work is in place.

## Non-Goals

- **Dashboard changes.** Job submission continues through the existing API. `worker_pool` is derived from the config's `model_type`, not chosen in the UI.
- **Runtime thread-count coordination.** Dynamically adjusting `OMP_NUM_THREADS` based on how many workers are busy is out of scope. Static per-container values are enough.
- **Parallelizing the `(symbol × horizon)` loop inside a fold.** Not needed once each individual `.fit()` is multi-core.
- **Wiring `model_type` from the backtest config through to `Trainer`.** The current `Trainer.train` defaults to its own model_type; piping a user-selected model through is a follow-up. For this change, `worker_pool` is derived at submission time from whatever `config.get("model_type")` is present (None → cpu). A later feature can surface model_type in the dashboard.

## Design

### 1. Schema — `backtest_jobs.worker_pool`

New migration `019_backtest_jobs_worker_pool.sql`:

```sql
ALTER TABLE backtest_jobs
  ADD COLUMN IF NOT EXISTS worker_pool TEXT NOT NULL DEFAULT 'cpu'
    CHECK (worker_pool IN ('cpu', 'gpu'));

CREATE INDEX IF NOT EXISTS idx_backtest_jobs_pool_status_created
  ON backtest_jobs (worker_pool, status, created_at);
```

Existing rows backfill to `'cpu'` via the column default. No application data wipe needed.

### 2. Pool classifier

New module `bullforge/services/backtest_service/worker_pool.py`:

```python
_GPU_MODEL_TYPES = frozenset({"lstm", "mlp", "meta_entry", "meta_exec"})

def cpu_or_gpu(model_type: str | None) -> str:
    """Maps a model_type string to the worker pool it should run on.
    None / unknown / CPU-native types (gradient_boost, hist_gradient_boost,
    lightgbm, baseline) → 'cpu'."""
    if model_type and model_type.lower() in _GPU_MODEL_TYPES:
        return "gpu"
    return "cpu"
```

Pure-function, unit-testable, zero dependencies.

### 3. Job queue changes (`jobs_db.py`)

Two method signatures change:

- `insert_job(name, config, custom_name=None, *, worker_pool=None)` — when `worker_pool` is None (the default for all current callers), derive via `cpu_or_gpu(config.get("model_type"))` and write the result to the new column.
- `claim_next(*, worker_pool: str)` — now **requires** a pool argument. The SQL WHERE clause adds `AND worker_pool = $1`. Each worker claims only its own pool's jobs.

Callers:
- [backtests_api.py:243](bullforge/services/dashboard_service/backtests_api.py#L243) (dashboard job submission) — continues without a `worker_pool` kwarg; derivation kicks in.
- [worker.py:54](bullforge/services/backtest_service/worker.py#L54) (`run_one`) — call site gets its pool value from the worker's own env and passes it through.

### 4. Worker config

`worker.py` reads a new env var at startup:

```python
WORKER_POOL = os.getenv("BACKTEST_WORKER_POOL", "cpu").lower()
if WORKER_POOL not in ("cpu", "gpu"):
    raise ValueError(f"BACKTEST_WORKER_POOL must be 'cpu' or 'gpu', got {WORKER_POOL!r}")
# ... in main loop:
did = await run_one(jobs, engine_factory=_engine_factory, worker_pool=WORKER_POOL)
```

Invalid values fail fast at startup. No runtime switching.

### 5. Model switch — `HistGradientBoostingClassifier`

`bullforge/services/model_service/model_factory.py` changes:

- Replace `from sklearn.ensemble import GradientBoostingClassifier` with `from sklearn.ensemble import HistGradientBoostingClassifier`.
- In the pipeline, change `GradientBoostingClassifier(n_estimators=100, max_depth=3, learning_rate=0.1, random_state=42)` to `HistGradientBoostingClassifier(max_iter=100, max_depth=3, learning_rate=0.1, random_state=42)`. (Same semantic hyperparameters; `HistGradientBoostingClassifier` uses `max_iter` instead of `n_estimators`.)
- The existing model_type key `"gradient_boost"` keeps the same name — callers don't change.

`HistGradientBoostingClassifier` uses OpenMP and saturates all threads available per `.fit()` call. Thread count is controlled via the `OMP_NUM_THREADS` environment variable in the container; numpy BLAS operations during data prep are bounded by `OPENBLAS_NUM_THREADS` and `MKL_NUM_THREADS`. All three are driven from a single `CPU_THREADS` / `GPU_THREADS` compose variable so no backend can silently steal more cores than its pool's budget.

### 6. Docker Compose split

Remove the single `backtest-worker` service. Add two services that share the same image:

```yaml
backtest-worker-cpu:
  image: bullforge-backtest
  build:
    context: .
    dockerfile: services/backtest_service/Dockerfile
  entrypoint: ["python", "-m", "services.backtest_service.worker"]
  env_file: [.env]
  environment:
    TIMESCALEDB_HOST: timescaledb
    REDIS_HOST: redis
    BACKTEST_WORKER_POOL: cpu
    OMP_NUM_THREADS: "${CPU_THREADS:-10}"
    OPENBLAS_NUM_THREADS: "${CPU_THREADS:-10}"
    MKL_NUM_THREADS: "${CPU_THREADS:-10}"
  depends_on:
    timescaledb: { condition: service_healthy }
    redis:       { condition: service_healthy }
  deploy:
    replicas: ${CPU_WORKERS:-2}
  restart: unless-stopped

backtest-worker-gpu:
  image: bullforge-backtest
  build:
    context: .
    dockerfile: services/backtest_service/Dockerfile
  entrypoint: ["python", "-m", "services.backtest_service.worker"]
  env_file: [.env]
  environment:
    TIMESCALEDB_HOST: timescaledb
    REDIS_HOST: redis
    BACKTEST_WORKER_POOL: gpu
    OMP_NUM_THREADS: "${GPU_THREADS:-4}"
    OPENBLAS_NUM_THREADS: "${GPU_THREADS:-4}"
    MKL_NUM_THREADS: "${GPU_THREADS:-4}"
  depends_on:
    timescaledb: { condition: service_healthy }
    redis:       { condition: service_healthy }
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

Defaults (can be overridden in `.env`):

| Env var | Default | Rationale |
|---|---|---|
| `CPU_WORKERS` | `2` | Pipelines DB I/O behind training. While worker A blocks on `dataset_builder.build`, worker B trains. |
| `CPU_THREADS` | `10` | 20 cores ÷ 2 workers. Drives `OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS` so every numeric backend (HistGBC's OpenMP; numpy BLAS ops) respects the per-worker budget. Brief over-subscription when both train simultaneously is acceptable. |
| `GPU_WORKERS` | `1` | User's DGX Spark has 1 GPU. |
| `GPU_THREADS` | `4` | GPU worker offloads most compute to GPU; small CPU budget for data prep. Drives the same trio of thread-count env vars. |

### 7. Data flow

```
dashboard submission
  → backtests_api creates job config (may have model_type)
  → jobs.insert_job(...)
      → if worker_pool arg is None, derive from config.model_type
      → INSERT row with worker_pool = 'cpu' or 'gpu'

backtest-worker-cpu (BACKTEST_WORKER_POOL=cpu)
  → jobs.claim_next(worker_pool='cpu')
  → claims only cpu-pool jobs

backtest-worker-gpu (BACKTEST_WORKER_POOL=gpu)
  → jobs.claim_next(worker_pool='gpu')
  → claims only gpu-pool jobs

Both workers run the same serial (symbol × horizon) loop; HistGBC's
internal OpenMP parallelism uses OMP_NUM_THREADS cores per fit.
```

### 8. Error Handling

- **Invalid `BACKTEST_WORKER_POOL` env value** — worker raises at startup; container crashes and docker-compose restarts it. Loud and fast.
- **GPU job arrives with only CPU workers running (or vice versa)** — job stays `queued` forever until a worker of the right pool picks it up. This is the correct failure mode; an operator sees it in the dashboard's "pending" list and either launches the other pool or requeues the job.
- **Migration idempotency** — `ADD COLUMN IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` make it safe to re-run.

### 9. Testing

**Unit (no DB):**
- `cpu_or_gpu(None)` → `"cpu"`
- `cpu_or_gpu("gradient_boost")` → `"cpu"` 
- `cpu_or_gpu("lstm")` → `"gpu"`
- `cpu_or_gpu("MLP")` (case) → `"gpu"`
- `cpu_or_gpu("unknown_future_model")` → `"cpu"` (default-safe)

**DB integration (real Postgres):**
- `insert_job(config={"model_type": "gradient_boost"})` writes `worker_pool='cpu'`.
- `insert_job(config={"model_type": "lstm"})` writes `worker_pool='gpu'`.
- `insert_job(config={})` writes `worker_pool='cpu'`.
- `insert_job(worker_pool='gpu', config={...})` honors the explicit override.
- Given one cpu job and one gpu job queued, `claim_next(worker_pool='cpu')` returns only the cpu job (status now `running`). `claim_next(worker_pool='gpu')` returns only the gpu job. A second `claim_next('cpu')` returns None (no more cpu jobs).

**Regression:**
- Existing `test_backtest_jobs_db.py` tests pass after `claim_next` calls are updated to pass `worker_pool="cpu"`.
- Existing `test_model_factory.py` tests pass against `HistGradientBoostingClassifier`. Note that any test asserting the exact string `"GradientBoostingClassifier"` in repr or type name must be updated; the pipeline still produces a working classifier with the same public API.

**Benchmark (manual, not committed):**
- Before/after timing of a single-symbol, single-horizon, single-fold backtest on the DGX. Expect multi-second speedup from HistGBC over GradientBoosting on typical feature-row counts.

### 10. Migration / Rollout

1. Merge code + migration 019 to master.
2. Host-specific `.env` on the DGX Spark gets:
   ```
   CPU_WORKERS=2
   GPU_WORKERS=1
   CPU_THREADS=10
   GPU_THREADS=4
   ```
3. `docker compose down backtest-worker` (old service) + `docker compose up -d --build backtest-worker-cpu backtest-worker-gpu`.
4. Watch the dashboard: queued jobs should drain through `backtest-worker-cpu`; the GPU worker stays idle until an LSTM job is submitted.
5. Smoke test: submit one cpu backtest, confirm it completes faster than before (HistGBC + 10 cores).

Rollback: `docker compose down backtest-worker-cpu backtest-worker-gpu`, restore previous `docker-compose.yml` (or revert the merge), `docker compose up -d --build backtest-worker`. The `worker_pool` column is harmless if unused.

## Risks & Trade-offs

- **Solo-backtest CPU utilization is 50%.** One CPU worker × 10 threads = half the DGX's 20 cores. When a queue is draining, the second worker covers the I/O gap; when a single backtest runs alone, the other 10 cores sit idle. Acceptable per user's clarification — throughput on queue drain is the goal, not solo latency. If solo becomes a pain point, the follow-up is dynamic thread coordination (not in scope).
- **Brief over-subscription window.** When both CPU workers train at the same time, peak demand = 20 threads = full box. Linux CFS handles it, but there's a minor context-switch cost. Tuning `CPU_WORKERS=1 / CPU_OMP_THREADS=20` is available via `.env` if the user observes contention.
- **Model behavior change.** `HistGradientBoostingClassifier` is a different algorithm than `GradientBoostingClassifier` (histogram-based vs exact). Numerical outputs will differ. User explicitly accepted this: "I'm not yet attached to the current application's outcomes."
- **GPU jobs stranded if GPU worker is down.** If `backtest-worker-gpu` fails to start (e.g., GPU driver issue), GPU-pool jobs queue indefinitely. Compose's `restart: unless-stopped` and docker's health-check catch most cases; ops-visible in the dashboard pending list.

## Files Changed

| File | Purpose |
|---|---|
| `bullforge/migrations/019_backtest_jobs_worker_pool.sql` | New column + index |
| `bullforge/services/backtest_service/worker_pool.py` | Pool classifier |
| `bullforge/services/backtest_service/jobs_db.py` | `insert_job(worker_pool=None)`, `claim_next(worker_pool=...)` |
| `bullforge/services/backtest_service/worker.py` | Read `BACKTEST_WORKER_POOL` env; pass to `claim_next` |
| `bullforge/services/model_service/model_factory.py` | Swap in `HistGradientBoostingClassifier` |
| `bullforge/docker-compose.yml` | Delete `backtest-worker`; add `backtest-worker-cpu` + `backtest-worker-gpu` with env-driven replicas |
| `bullforge/.env.example` (if one exists) | Document `CPU_WORKERS` / `GPU_WORKERS` / `CPU_THREADS` / `GPU_THREADS` defaults |
| `bullforge/tests/test_worker_pool.py` | New unit + integration tests |
| `bullforge/tests/test_backtest_jobs_db.py` | Update `claim_next` calls to pass `worker_pool="cpu"` |
| `bullforge/tests/test_model_factory.py` | Adjust assertions if they hardcode class name |
