# Model-service Retrain Resilience Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the nightly retrain from nullifying live inference. Two scoped fixes in `services/model_service`: (1) `InferenceEngine.load_models` survives bad artifacts via atomic-swap + broadened except, mirroring `load_meta_models`; (2) `RetrainScheduler.run_once` uses `mp_context="spawn"` for `ProcessPoolExecutor` so workers don't inherit the parent's torch/lightgbm/sklearn/UCX state.

**Architecture:**
1. Rewrite `load_models` to build a fresh local dict and only assign to `self._models` after the load loop completes — same pattern `load_meta_models` already uses (`inference.py:225-243`). Broaden `except FileNotFoundError` to `except Exception` for non-file-missing failures, log with traceback, skip the bad row.
2. Pass `mp_context=multiprocessing.get_context("spawn")` to `ProcessPoolExecutor`. Spawn workers start from a clean interpreter and re-import what they need.

**Tech Stack:** asyncio, sklearn, joblib, `concurrent.futures.ProcessPoolExecutor`, pytest-asyncio, `unittest.mock.patch`, pytest's `tmp_path` and `caplog` fixtures.

**Source spec:** `bullforge/docs/superpowers/specs/2026-05-04-model-service-retrain-resilience-design.md`

**Baseline (master @ `e3cba61`):** `1010 passed, 1 skipped` via `bin/test python -m pytest tests/ -v` from `bullforge/`.

**Out of scope:** disk cleanup of partial pickles, scheduler restart-on-failure, `retrain_cpu_workers` tuning, MSFT-specific data investigation, any change to `load_meta_models`.

---

## Task 1: TDD red — corrupt-artifact and atomic-swap tests for `load_models`

**Files:**
- Modify: `bullforge/tests/test_inference.py` (append two new tests at end of file)

- [ ] **Step 1: Add the two failing tests at the end of `test_inference.py`**

Append to `bullforge/tests/test_inference.py`:

```python
@pytest.mark.asyncio
async def test_load_models_skips_corrupt_artifact_keeps_good_models(
    tmp_path, caplog,
):
    """Regression for 2026-05-04 incident: a corrupt pickle on disk
    raises EOFError / UnpicklingError, not FileNotFoundError. The old
    load_models cleared self._models BEFORE the load loop and only
    caught FileNotFoundError, so a single corrupt artifact wiped the
    entire inference dict. The fix: build a new dict locally, broaden
    the except to log+skip the bad row, swap atomically at the end."""
    import dataclasses
    import joblib

    from services.model_service.model_store import ModelStore

    good_path = tmp_path / "gradient_boost_SPY_1m_5m_20260504T010000Z.joblib"
    # The model object only needs to be pickleable; this test never
    # calls .predict() on it. A plain dict round-trips cleanly through
    # joblib whereas MagicMock does not.
    joblib.dump({"dummy": "good model"}, good_path)

    bad_path = tmp_path / "gradient_boost_SPY_1m_10m_20260504T010000Z.joblib"
    bad_path.write_bytes(b"\x00\x01not a valid pickle stream\x00")

    # ModelMetadata is @dataclass(frozen=True), so use dataclasses.replace
    # to copy the fixture with the fields we need to override.
    base = _make_model_metadata("SPY", 5)
    good_meta = dataclasses.replace(base, artifact_path=str(good_path))
    bad_meta = dataclasses.replace(
        base, id=2, horizon_minutes=10, artifact_path=str(bad_path),
    )

    mock_db = AsyncMock()
    mock_db.query_all_active_models = AsyncMock(return_value=[good_meta, bad_meta])

    store = ModelStore(str(tmp_path))
    engine = InferenceEngine(db=mock_db, model_store=store)

    with caplog.at_level("ERROR"):
        await engine.load_models()

    assert len(engine._models) == 1, (
        "Corrupt artifact must not poison the load — only the bad row "
        "is skipped, the good model remains in the inference dict."
    )
    assert ("SPY", "1m", 5, "gradient_boost") in engine._models
    assert any(
        "Failed to load artifact" in rec.message
        for rec in caplog.records
    ), "Bad artifact should produce an ERROR-level log"


@pytest.mark.asyncio
async def test_load_models_preserves_state_when_db_query_fails(
    mock_db, mock_model_store,
):
    """If load_models raises midway through (e.g., DB outage between
    reading active_models and assigning the new dict), the previous
    in-memory model set must remain intact. The old code cleared
    self._models BEFORE awaiting the DB, so any propagated exception
    left the engine modelless. The atomic-swap rewrite removes that
    foot-gun."""
    engine = InferenceEngine(db=mock_db, model_store=mock_model_store)
    await engine.load_models()
    assert len(engine._models) == 2, "Precondition: initial load succeeded"

    mock_db.query_all_active_models = AsyncMock(
        side_effect=ConnectionError("simulated DB outage"),
    )

    with pytest.raises(ConnectionError):
        await engine.load_models()

    assert len(engine._models) == 2, (
        "Previous models must survive a failed reload — last-known-good"
    )
```

- [ ] **Step 2: Run the two new tests; verify they FAIL on master code**

Run:
```bash
cd bullforge
bin/test python -m pytest tests/test_inference.py::test_load_models_skips_corrupt_artifact_keeps_good_models tests/test_inference.py::test_load_models_preserves_state_when_db_query_fails -v
```

Expected: both FAIL.
- The corrupt-artifact test fails because `joblib.load` raises a non-`FileNotFoundError` exception that propagates out of `load_models`, so the `len(engine._models) == 1` assertion fires (it's 0).
- The preservation test fails because `self._models.clear()` runs before the DB query, leaving the dict empty after `ConnectionError`.

- [ ] **Step 3: Commit the failing tests**

```bash
git add bullforge/tests/test_inference.py
git commit -m "test(inference): cover corrupt-artifact + atomic-swap (failing)"
```

---

## Task 2: TDD green — rewrite `load_models` for atomic swap + broadened except

**Files:**
- Modify: `bullforge/services/model_service/inference.py:137-166`

- [ ] **Step 1: Rewrite `load_models`**

Replace the body of `load_models` (currently lines 137-166) with:

```python
    async def load_models(self) -> None:
        active_models = await self._db.query_all_active_models(scope=self._scope)
        # Atomic swap: build a fresh dict, assign to self._models only
        # after the loop. Mirrors load_meta_models below. Without this,
        # any exception propagating out of the loop (corrupt pickle,
        # torch.load on bad .pt, etc.) would leave self._models empty —
        # the 2026-05-04 incident.
        new: Dict[
            Tuple[str, str, int, str], Tuple[object, ModelMetadata]
        ] = {}

        for meta in active_models:
            # Meta artifacts (meta_entry / meta_exec) belong to _meta_models,
            # not the base inference dict. Without this skip, predict() would
            # iterate them too and crash with AttributeError because meta
            # estimators expose predict_proba but no predict — symptom was a
            # CalibratedEntryEstimator.predict() lookup blowing up the redis
            # pubsub listener for every 1m feature:updates event.
            if meta.model_type in ("meta_entry", "meta_exec"):
                continue
            try:
                model = self._model_store.load(meta.artifact_path)
            except FileNotFoundError:
                logger.warning(
                    "Artifact not found for model id=%d: %s",
                    meta.id, meta.artifact_path,
                )
                continue
            except Exception:
                # Corrupt / truncated / version-skew artifact. Skipping
                # one is always correct — the alternative (propagating)
                # clears every other model with it, which is the
                # 2026-05-04 incident. logger.exception captures the
                # traceback so operators can identify the bad file.
                logger.exception(
                    "Failed to load artifact for model id=%d (%s); skipping",
                    meta.id, meta.artifact_path,
                )
                continue

            key = (
                meta.symbol, meta.timeframe, meta.horizon_minutes,
                meta.model_type,
            )
            new[key] = (model, meta)
            logger.info(
                "Loaded model: %s %s %dm (%s, id=%d)",
                meta.symbol, meta.timeframe, meta.horizon_minutes,
                meta.model_type, meta.id,
            )

        self._models = new
        logger.info("Loaded %d models for inference", len(self._models))
```

- [ ] **Step 2: Run the two new tests; verify GREEN**

```bash
cd bullforge
bin/test python -m pytest tests/test_inference.py::test_load_models_skips_corrupt_artifact_keeps_good_models tests/test_inference.py::test_load_models_preserves_state_when_db_query_fails -v
```

Expected: both PASS.

- [ ] **Step 3: Run the full `test_inference.py` suite to confirm no regression in existing load tests**

```bash
cd bullforge
bin/test python -m pytest tests/test_inference.py -v
```

Expected: all tests pass — including the existing `test_load_models`, `test_load_models_empty`, `test_load_models_keeps_both_model_types_for_same_slot`, `test_load_models_skips_meta_artifacts`, `test_inference_engine_scoped_load_queries_with_scope`, `test_inference_engine_live_load_queries_without_scope`. The atomic-swap rewrite is observation-equivalent on the happy path.

- [ ] **Step 4: Commit the fix**

```bash
git add bullforge/services/model_service/inference.py
git commit -m "$(cat <<'EOF'
fix(inference): atomic swap + broadened except in load_models

Mirrors load_meta_models. Build a fresh dict locally and assign to
self._models only after the load loop completes; broaden except
FileNotFoundError to also log+skip on corrupt pickles, torch.load
errors, version skew, and other artifact failures. Without this,
the 2026-05-04 retrain incident left the engine modelless for ~18
hours after one bad MSFT artifact propagated past the narrow except.
EOF
)"
```

---

## Task 3: TDD red — assert `ProcessPoolExecutor` uses spawn context

**Files:**
- Modify: `bullforge/tests/test_scheduler.py` (append one new test at end of file)

- [ ] **Step 1: Add the failing test**

Append to `bullforge/tests/test_scheduler.py`:

```python
@pytest.mark.asyncio
async def test_run_once_uses_spawn_context_for_process_pool():
    """Regression for 2026-05-04 incident: forking from model_service
    (which has imported torch / lightgbm / sklearn / UCX in the parent)
    is the textbook OpenMP-segfault setup. Spawn workers start from a
    clean interpreter. Pin the constructor argument so a future refactor
    can't quietly drop it."""
    import multiprocessing

    runtime_config = AsyncMock()
    runtime_config.get = AsyncMock(return_value=1)
    runtime_config.get_all = AsyncMock(return_value={
        "retrain_cpu_workers": 1, "lstm_epochs": 1, "lstm_hidden_size": 8,
        "lstm_sequence_length": 5, "lstm_learning_rate": 0.01,
        "lstm_batch_size": 16,
    })
    runtime_config.set = AsyncMock()
    inference_engine = AsyncMock()
    redis_client = AsyncMock()

    sched = RetrainScheduler(
        db_dsn="postgres://fake", artifacts_dir="/tmp",
        runtime_config=runtime_config,
        inference_engine=inference_engine, redis_client=redis_client,
        symbols=["SPY"], timeframes=["1m"], horizons_by_tf={"1m": [5]},
        min_samples=10,
    )

    with patch("services.model_service.scheduler.ProcessPoolExecutor") as pool_cls:
        pool_instance = MagicMock()
        pool_cls.return_value.__enter__.return_value = pool_instance
        future = MagicMock()
        future.result.return_value = {
            "symbol": "SPY", "timeframe": "1m", "horizon": 5,
            "model_type": "gradient_boost", "status": "ok", "model_id": 1,
        }
        pool_instance.submit.return_value = future

        with patch.object(sched, "_train_lstm_in_process", new=AsyncMock(
            return_value={"status": "ok", "symbol": "SPY", "timeframe": "1m",
                          "horizon": 5, "model_type": "lstm"},
        )):
            await sched.run_once()

    assert pool_cls.call_count == 1, (
        "ProcessPoolExecutor must be invoked exactly once per cycle"
    )
    kwargs = pool_cls.call_args.kwargs
    assert "mp_context" in kwargs, (
        "ProcessPoolExecutor must be constructed with mp_context= "
        "to avoid the default fork start method on Linux. See "
        "docs/superpowers/specs/2026-05-04-model-service-retrain-resilience-design.md"
    )
    assert kwargs["mp_context"].get_start_method() == "spawn", (
        f"Expected spawn start method, got "
        f"{kwargs['mp_context'].get_start_method()!r}"
    )
```

- [ ] **Step 2: Run the new test; verify it FAILS on master code**

```bash
cd bullforge
bin/test python -m pytest tests/test_scheduler.py::test_run_once_uses_spawn_context_for_process_pool -v
```

Expected: FAIL on `assert "mp_context" in kwargs` — the current code calls `ProcessPoolExecutor(max_workers=workers)` with no `mp_context`.

- [ ] **Step 3: Commit the failing test**

```bash
git add bullforge/tests/test_scheduler.py
git commit -m "test(scheduler): pin spawn start method for ProcessPoolExecutor (failing)"
```

---

## Task 4: TDD green — pass `mp_context="spawn"` to `ProcessPoolExecutor`

**Files:**
- Modify: `bullforge/services/model_service/scheduler.py` (top imports + line 113)

- [ ] **Step 1: Add `import multiprocessing` near the existing stdlib imports**

In `bullforge/services/model_service/scheduler.py`, change the top of the file:

```python
from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Optional
```

(Add `import multiprocessing` between `import logging` and `from concurrent.futures import ProcessPoolExecutor`.)

- [ ] **Step 2: Pass `mp_context=` to `ProcessPoolExecutor`**

In `bullforge/services/model_service/scheduler.py`, replace the line currently at 113:

```python
        with ProcessPoolExecutor(max_workers=workers) as pool:
```

with:

```python
        # Spawn (not fork) so workers don't inherit the parent's torch /
        # lightgbm / sklearn / UCX state — fork from a multi-threaded
        # interpreter is the documented OpenMP-segfault setup, and was
        # the root cause of the 2026-05-04 retrain incident. Manual
        # retrain_now survived because that path runs in a fresh
        # interpreter; the nightly cycle does not have that luxury.
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=multiprocessing.get_context("spawn"),
        ) as pool:
```

- [ ] **Step 3: Run the new test; verify GREEN**

```bash
cd bullforge
bin/test python -m pytest tests/test_scheduler.py::test_run_once_uses_spawn_context_for_process_pool -v
```

Expected: PASS.

- [ ] **Step 4: Run the full `test_scheduler.py` suite to confirm no regression**

```bash
cd bullforge
bin/test python -m pytest tests/test_scheduler.py -v
```

Expected: all tests pass, including the existing `test_run_once_dispatches_cpu_then_gpu_phase` and `test_run_once_handles_worker_error` (both already use `patch("services.model_service.scheduler.ProcessPoolExecutor")` and don't inspect call kwargs, so they remain green).

- [ ] **Step 5: Run `test_meta_retrain_phase.py` to check the fork DeprecationWarning has gone**

```bash
cd bullforge
bin/test python -m pytest tests/test_meta_retrain_phase.py -v -W error::DeprecationWarning 2>&1 | tail -30
```

Expected: passes without raising the `multi-threaded ... fork() may lead to deadlocks` deprecation warning. (The warning is emitted by CPython's `multiprocessing` itself when `fork()` is called from a multi-threaded process; switching to spawn silences it because spawn doesn't fork.)

If the warning still fires from a different test path, that's a separate issue — note it but don't block on it for this fix. The pin is `pool_cls.call_args.kwargs["mp_context"]`, not the warning suppression.

- [ ] **Step 6: Commit the fix**

```bash
git add bullforge/services/model_service/scheduler.py
git commit -m "$(cat <<'EOF'
fix(scheduler): use spawn start method for ProcessPoolExecutor

Forking from model_service (which has imported torch / lightgbm /
sklearn / UCX in the parent) is the documented OpenMP-segfault
setup and was the root cause of the 2026-05-04 retrain incident.
Spawn workers start from a clean interpreter and re-import only
what train_one_job needs. train_one_job is already a top-level
function with primitive args, so spawn pickling works without
further changes.
EOF
)"
```

---

## Task 5: Full pytest sweep + verification

**Files:**
- None modified.

- [ ] **Step 1: Run the entire test suite via the docker harness**

```bash
cd bullforge
bin/test python -m pytest tests/ -v 2>&1 | tail -60
```

Expected: same green-count as the master baseline plus the three new tests, i.e. **1013 passed, 1 skipped** (1010 baseline + 2 new inference tests + 1 new scheduler test). No new failures.

If any test that was previously green is now red, do not proceed — diagnose first. The atomic-swap rewrite preserves observable behavior on the happy path; any regression there is a real bug in the rewrite.

- [ ] **Step 2: Confirm the four expected commits are on the branch**

```bash
git log --oneline master..HEAD
```

Expected (in order):
1. `docs(spec): model-service retrain resilience design`
2. `test(inference): cover corrupt-artifact + atomic-swap (failing)`
3. `fix(inference): atomic swap + broadened except in load_models`
4. `test(scheduler): pin spawn start method for ProcessPoolExecutor (failing)`
5. `fix(scheduler): use spawn start method for ProcessPoolExecutor`

(Five entries total — the spec commit is already on the branch from the brainstorm phase.)

- [ ] **Step 3: Hand off to code-review**

The branch is ready for `superpowers:code-reviewer` against this plan + the spec.
