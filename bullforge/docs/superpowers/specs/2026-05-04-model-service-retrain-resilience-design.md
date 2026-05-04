# Model-service retrain resilience — design

**Status:** approved (auto-mode brainstorm)
**Date:** 2026-05-04
**Author:** Claude (Opus 4.7)
**Source:** post-mortem of the 2026-05-04 01:00 UTC retrain incident — operator handover note

Two intertwined defects in `services/model_service` combined to nullify live inference for ~18 hours after the nightly retrain on 2026-05-04. Bundled into one spec because they share a single root-cause incident and any fix to one without the other still leaves the service one bad artifact away from a repeat outage. Pattern matches the two-fixes-one-spec layout of `2026-05-04-exclusion-fallback-and-017-gap-design.md`.

## Incident summary

At 01:00 UTC the nightly retrain fired from inside the long-running `model_service` interpreter. A CPU worker hit a SIGSEGV inside libgomp (OpenMP) while running sklearn `HistGradientBoosting` binning on MSFT 1m 3m. The worker process died mid-write, leaving a partial pickle on disk; the parent's `ProcessPoolExecutor` then poisoned itself and returned `BrokenProcessPool` for every queued job (~150 CPU jobs failed). At the end of `RetrainScheduler.run_once`, `InferenceEngine.load_models()` ran, hit the corrupt pickle on `ModelStore.load(...)` (raised `EOFError` / `UnpicklingError` rather than `FileNotFoundError`), and the exception propagated. Because `self._models.clear()` runs **before** the load loop, the in-memory model dict was left empty. Inference silently produced zero predictions for ~18 hours until the operator restarted the service. The container never crashed; the health endpoint stayed green.

The pre-existing test `tests/test_meta_retrain_phase.py::test_run_once_invokes_meta_phase_per_symbol_direction` already emits `DeprecationWarning: This process (pid=1) is multi-threaded, use of fork() may lead to deadlocks in the child` — corroborating the root cause for Bug #2 from the test harness side.

## Bug #1 — `InferenceEngine.load_models` clears state before reload, narrows the exception window too far

### Problem

`services/model_service/inference.py:137-166` (`load_models`):

```python
async def load_models(self) -> None:
    active_models = await self._db.query_all_active_models(scope=self._scope)
    self._models.clear()              # ← clears BEFORE loading
    for meta in active_models:
        ...
        try:
            model = self._model_store.load(meta.artifact_path)
            self._models[key] = (model, meta)
        except FileNotFoundError:     # ← only catches this; pickle errors propagate
            logger.warning(...)
```

Two intertwined defects:

1. **Narrow except clause.** A corrupt artifact (truncated, non-pickle bytes, version skew) raises `EOFError`, `pickle.UnpicklingError`, `ModuleNotFoundError`, `OSError`, `RuntimeError` (torch.load on bad `.pt`), or others — none of them `FileNotFoundError`. The exception propagates out of `load_models` and aborts the loop.
2. **Eager clear.** `self._models.clear()` runs before any new model has been loaded. So a propagated exception (per #1) — or for that matter any transient error between line 138 and the first successful load — leaves the engine with an empty model dict and no recovery path until process restart.

### Goals

1. One bad artifact must log + skip and not poison the rest of the load.
2. If `load_models` raises midway through, the previous in-memory model set must remain intact (last-known-good behavior).
3. Match the pattern already used by `load_meta_models` (lines 225-243), which builds a `new = {}` dict and only assigns to `self._meta_models` at the end. Consistency between the two methods removes a foot-gun.
4. Don't change observable behavior on the happy path: same models loaded into the same dict-of-tuples key shape, same INFO log lines.

### Design

Mirror `load_meta_models`. Build a fresh local dict, broaden the `except`, swap atomically:

```python
async def load_models(self) -> None:
    active_models = await self._db.query_all_active_models(scope=self._scope)
    new: Dict[Tuple[str, str, int, str], Tuple[object, ModelMetadata]] = {}

    for meta in active_models:
        if meta.model_type in ("meta_entry", "meta_exec"):
            continue
        key = (meta.symbol, meta.timeframe, meta.horizon_minutes, meta.model_type)
        try:
            model = self._model_store.load(meta.artifact_path)
        except FileNotFoundError:
            logger.warning(
                "Artifact not found for model id=%d: %s",
                meta.id, meta.artifact_path,
            )
            continue
        except Exception:
            # Corrupt / truncated / version-skew artifact. Skipping one is
            # always correct — the alternative (propagating) clears every
            # other model with it, which is the 2026-05-04 incident.
            logger.exception(
                "Failed to load artifact for model id=%d (%s); skipping",
                meta.id, meta.artifact_path,
            )
            continue

        new[key] = (model, meta)
        logger.info(
            "Loaded model: %s %s %dm (%s, id=%d)",
            meta.symbol, meta.timeframe, meta.horizon_minutes,
            meta.model_type, meta.id,
        )

    self._models = new
    logger.info("Loaded %d models for inference", len(self._models))
```

Notes on the broadened except:

- `logger.exception(...)` (not `logger.warning`) so the traceback is captured — operators need the class + path to triage which artifact to delete on disk.
- We catch `Exception` rather than enumerating `(EOFError, UnpicklingError, OSError, RuntimeError, ModuleNotFoundError, ...)`. The full list is open-ended (every torch / sklearn / joblib version adds new failure modes), and the alternative — letting an unanticipated exception class wipe the dict — is exactly the failure mode we're fixing. Bare `except Exception:` is fine here because the unit of work is "load this one artifact"; we are not swallowing programmer-error tracebacks (those still log).
- We do **not** catch `BaseException`, so `KeyboardInterrupt` / `SystemExit` / `asyncio.CancelledError` still propagate normally.

### What we explicitly don't do

- Don't apply the same atomic-swap pattern to `load_meta_models` — it already does this. (Verify when reading; if it diverged, fix it in this commit.)
- Don't add retry-with-backoff. A bad artifact will be bad on retry; the fix is to skip + alert, not to spin.
- Don't reach into the operator's disk to delete the partial pickle. Out of scope; the operator will run a `find /app/models/artifacts -newer ... -size -1k -delete` once-off.

### Test plan (TDD, hermetic)

Two new tests in `tests/test_inference.py`, mirroring the existing `_make_model_metadata` / `_make_mock_model` fixtures:

1. **`test_load_models_skips_corrupt_artifact_keeps_good_models`:**
   - Use a real `tmp_path` (pytest fixture) and a real `ModelStore(tmp_path)`.
   - Save a real (`MagicMock`-pickleable: use a tiny `sklearn.dummy.DummyClassifier`-fit-on-dummy-data, or just `joblib.dump(object(), ...)`) good artifact.
   - Write garbage bytes to a second path with `.joblib` extension (`(tmp_path / "bad.joblib").write_bytes(b"\x00\x01not a pickle")`).
   - Mock `db.query_all_active_models` to return two `ModelMetadata` rows, one pointing at each path.
   - Assert: `await engine.load_models()` returns normally, `len(engine._models) == 1`, the good key is present, and a log record at ERROR level with "Failed to load artifact" was emitted (use `caplog`).

2. **`test_load_models_preserves_previous_state_on_db_failure`:**
   - First call succeeds — load one good artifact.
   - Second call: patch `db.query_all_active_models` to raise `ConnectionError`.
   - Assert: the call raises (propagates DB errors as before — out of scope for this fix), but `engine._models` still contains the previously-loaded model.
   - This test pins the atomic-swap behavior: it is OK for `load_models` to fail; it is not OK for it to leave the engine modelless.

3. **Optional polish — test_load_models_skips_corrupt_artifact_emits_exception_log:**
   - Reuse fixture #1; assert the log captured a traceback (`record.exc_info` is not None).
   - Skip if it complicates the scaffold; the ERROR-level assertion in #1 already pins the user-facing behavior.

### Files touched

- `bullforge/services/model_service/inference.py` — `load_models` rewritten (~25 lines diff).
- `bullforge/tests/test_inference.py` — two new tests + any imports.

## Bug #2 — `ProcessPoolExecutor` defaults to fork; combine with torch / lightgbm / sklearn / UCX in the parent and you get OpenMP segfaults

### Problem

`services/model_service/scheduler.py:113`:

```python
with ProcessPoolExecutor(max_workers=workers) as pool:
```

Default `mp_context` on Linux is `fork`. Forking from `model_service` — which has already imported torch (LSTM/GPU), lightgbm, sklearn, possibly UCX (NVIDIA HPCX present on the Spark/DGX containers), and started asyncio thread state — is the textbook recipe for OpenMP segfaults in child workers. CPython's own multiprocessing emits exactly this warning at runtime: `DeprecationWarning: This process (pid=1) is multi-threaded, use of fork() may lead to deadlocks in the child.` (already visible in our test output at `tests/test_meta_retrain_phase.py`.)

The manual `retrain_now` HTTP endpoint survives because it's invoked from a fresh interpreter with none of that state loaded. The nightly cycle does not have that luxury.

### Goals

1. Switch the executor's start method to `spawn`. Spawn workers start from a clean Python interpreter and re-import only what they need — no shared OpenMP runtime to corrupt.
2. Don't change anything else about the scheduler's behavior. Job dispatch, future result handling, error reporting all stay identical.
3. Verify spawn pickling actually works for the args we pass — primitives (`db_dsn`, `artifacts_dir`, `symbol`, `tf`, `h`, `mt`, two ints), a `datetime`, and a `dict`. (It will — `train_one_job` is a top-level module function with no closures, per `worker.py`'s docstring, and the args are all primitives + a dict.)
4. Don't introduce platform divergence — Windows already defaults to `spawn`, so explicitly setting it makes Linux match Windows rather than the other way around.

### Design

Two-line change at the top of `scheduler.py`:

```python
import multiprocessing  # added
from concurrent.futures import ProcessPoolExecutor
```

And inside `run_once`:

```python
with ProcessPoolExecutor(
    max_workers=workers,
    mp_context=multiprocessing.get_context("spawn"),
) as pool:
```

### What we explicitly don't do

- Don't `multiprocessing.set_start_method("spawn")` globally — that's process-wide state and would conflict with anything else in the codebase (or in tests) that relies on the default. Scoping to this one executor is cleaner and locally provable.
- Don't add `initializer=` to pre-import torch/lightgbm in workers — these workers run sklearn `HistGradientBoosting` only (LSTM trains in-process on the GPU), so initializer would only slow down dispatch.
- Don't change `retrain_cpu_workers` default. Tuning is a separate question.
- Don't add restart-on-failure for the scheduler. Out of scope; container-level supervision belongs at the docker / k8s layer.

### Test plan (TDD, hermetic)

The actual segfault is data-dependent and platform-dependent — we can't reproduce it in a unit test. What we can pin is "the constructor argument doesn't get dropped in a future refactor."

One new test in `tests/test_scheduler.py`, sibling to the existing `test_run_once_dispatches_cpu_then_gpu_phase`:

1. **`test_run_once_uses_spawn_context_for_process_pool`:**
   - Re-use the existing scheduler fixture.
   - `patch("services.model_service.scheduler.ProcessPoolExecutor")` so we capture the call args without actually spawning anything.
   - After `run_once` returns, assert `pool_cls.call_args.kwargs["mp_context"].get_start_method() == "spawn"`.
   - This is enough to catch a regression where someone removes the `mp_context=` argument.

The fork DeprecationWarning currently emitted by `tests/test_meta_retrain_phase.py` should disappear after the fix; a comment in the new test ("Fork start method causes OpenMP segfaults — see 2026-05-04 incident") points future readers at the why.

### Files touched

- `bullforge/services/model_service/scheduler.py` — two-line change.
- `bullforge/tests/test_scheduler.py` — one new test.

## Workflow

1. One worktree, one branch: `fix/model-service-retrain-resilience` (already created).
2. TDD for both bugs (failing test → minimal change → green). Bug #1 first because it's the higher-leverage fix — gates inference resilience independently of whether the retrain goes well.
3. Verification: `bin/test python -m pytest tests/ -v` from `bullforge/` — all green before claiming done. Baseline today (master @ `e3cba61`): `1010 passed, 1 skipped`.
4. One `superpowers:code-reviewer` pass at the end against this spec + plan.
5. Conventional Commits, **no Co-Authored-By trailer**, **`--no-ff` merge** — matching the 2026-05-03 / 2026-05-04 sessions' style.
6. Single commit per bug is fine; bundle into one merge.

## Out of scope

- Cleaning up partial artifact pickles on disk (operator concern).
- Restart-on-failure logic for the retrain scheduler.
- Tuning `retrain_cpu_workers` default.
- Investigating why MSFT 1m 3m specifically triggered the segfault — could be data-dependent, not necessary for the resilience fix.
- Anything Phase 2 / meta-cutover (gated on shadow-data accumulation through ~2026-05-14 — see `TODO.md`).
- Any change to `load_meta_models` beyond a sanity-check that it still mirrors the new `load_models` shape.
- Health-endpoint changes (the dashboard's "Models" tab already surfaces an empty inference dict — a separate UX question).
