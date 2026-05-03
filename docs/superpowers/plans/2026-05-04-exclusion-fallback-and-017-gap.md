# Dashboard exclusion-buffer fallback + 017 migration-slot doc — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pass `RuntimeConfig` equity-window buffer minutes through to `DatasetBuilder.build` inside the dashboard's permutation-importance fallback (closing the train/eval mismatch the A2 work introduced on the dashboard side), and document the intentionally empty `017_*` migration slot.

**Architecture:** Two surgical changes. (1) `services/dashboard_service/models_api.py::_handle_feature_importance` reads `equity_window_open_buffer_minutes` and `equity_window_close_buffer_minutes` from `request.app["runtime_config"]`, threads them into `DatasetBuilder.build(...)`, and surfaces them on the success-path JSON. Graceful fallback to `None`/`None` (filter disabled) on `runtime_config.get` failure. (2) `bullforge/migrations/README.md` gets a "Numbering history" subsection explaining why `017_*` is empty and instructing future contributors not to reclaim the slot.

**Tech Stack:** Python 3.11, aiohttp, asyncpg (mocked via `MagicMock` + `AsyncMock`), pytest with `asyncio_mode = auto`, sklearn, `unittest.mock.patch.object`. Working directory throughout: `bullforge/`.

**Spec:** [`docs/superpowers/specs/2026-05-04-exclusion-fallback-and-017-gap-design.md`](../specs/2026-05-04-exclusion-fallback-and-017-gap-design.md)

---

## File structure

| File | Action | Responsibility |
|------|--------|----------------|
| `bullforge/services/dashboard_service/models_api.py` | Modify (~10 lines around 280–284, plus response-shape addition) | Read buffers from runtime_config and thread them into the fallback rebuild; surface on JSON. |
| `bullforge/tests/test_dashboard_models_api.py` | Modify (3 new tests + supporting helpers) | Lock the kwargs, the error-path fallback, and the JSON response-shape addition. |
| `bullforge/migrations/README.md` | Modify (append one subsection) | Document the empty `017_*` slot. |

No new files. No schema changes. No runtime-config schema changes.

---

## Task 1: Worktree setup and verify baseline

**Files:**
- None (environment only)

- [ ] **Step 1: Create the worktree**

From the repo root:

```powershell
cd <repo>
git worktree add .claude/worktrees/exclusion-fallback-and-017-gap -b chore/exclusion-fallback-and-017-gap master
cd .claude/worktrees/exclusion-fallback-and-017-gap
```

Confirm with `git status` that you are on `chore/exclusion-fallback-and-017-gap` and the working tree is clean.

- [ ] **Step 2: Verify the baseline pytest sweep is green**

From the worktree's `bullforge/` subdirectory:

```bash
cd bullforge
bin/test python -m pytest tests/test_dashboard_models_api.py -v
```

Expected: all existing tests in this file pass. If any fail before you've changed anything, STOP and surface the failure — don't proceed.

- [ ] **Step 3: Verify the kwargs you're about to thread are real**

```bash
bin/test python -c "from services.model_service.dataset import DatasetBuilder; import inspect; print(inspect.signature(DatasetBuilder.build))"
```

Expected output contains `open_buffer_min: Optional[int] = None, close_buffer_min: Optional[int] = None`. If it doesn't, the spec assumption is wrong — STOP and re-check.

---

## Task 2: Failing test — buffers must reach `DatasetBuilder.build`

**Files:**
- Test: `bullforge/tests/test_dashboard_models_api.py` (append to end)

This test mocks `db.pool.acquire()` to return a model row that triggers the permutation-importance fallback (no `feature_importances_`), spies on `DatasetBuilder.build` to capture kwargs, and asserts the live buffer values reach it.

- [ ] **Step 1: Add the test (it will fail because the handler does not pass the kwargs yet)**

Append to `bullforge/tests/test_dashboard_models_api.py`:

```python
# --- Feature importance fallback: equity exclusion buffers --------------------
#
# `_handle_feature_importance` falls back to permutation importance for
# classifiers without `feature_importances_`. Inside that fallback it rebuilds
# the training dataset via DatasetBuilder, which (post-A2, 2026-05-03) accepts
# `open_buffer_min` / `close_buffer_min` to filter out rows whose forward-return
# target lands in the equity exclusion zone. The dashboard fallback must thread
# these through from RuntimeConfig so it sees the same population the live
# trainer would see today.


def _make_model_registry_row(model_type: str = "hist_gradient_boost"):
    """Mimic asyncpg's Record for the model_registry SELECT in
    _handle_feature_importance."""
    from datetime import datetime, timezone
    return {
        "artifact_path": "/tmp/does-not-matter/model.pkl",
        "feature_columns": '["f1","f2","f3"]',
        "model_type": model_type,
        "symbol": "AAPL",
        "timeframe": "1m",
        "horizon_minutes": 5,
        "trained_at": datetime(2026, 4, 30, 14, 0, 0, tzinfo=timezone.utc),
    }


def _wire_model_row_on(db, row):
    """Attach a `db.pool.acquire()` -> conn.fetchrow context-manager mock to
    the AsyncMock from the existing `mock_db_models` fixture."""
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=row)
    conn_cm = AsyncMock()
    conn_cm.__aenter__ = AsyncMock(return_value=conn)
    conn_cm.__aexit__ = AsyncMock(return_value=False)
    db.pool = MagicMock()
    db.pool.acquire = MagicMock(return_value=conn_cm)


@pytest.mark.asyncio
async def test_feature_importance_fallback_passes_runtime_buffers_to_builder(
    aiohttp_client, models_app_with_redis, mock_db_models,
):
    """The permutation-importance fallback must read both equity-window
    buffer keys from RuntimeConfig and thread them into DatasetBuilder.build,
    so the rebuilt training set respects the same exclusion-zone filter
    Trainer.train uses today.
    """
    from unittest.mock import patch
    from services.model_service.dataset import DatasetBuilder

    # Wire a model row whose model_type lacks feature_importances_ so the
    # handler hits the permutation branch.
    _wire_model_row_on(mock_db_models, _make_model_registry_row("hist_gradient_boost"))

    # RuntimeConfig surfaces distinct values so a key swap (open<->close) fails
    # the assertion rather than slipping through.
    async def _rc_get(key):
        return {
            "equity_window_open_buffer_minutes": 60,
            "equity_window_close_buffer_minutes": 45,
        }[key]

    runtime_config = MagicMock()
    runtime_config.get = AsyncMock(side_effect=_rc_get)
    models_app_with_redis["runtime_config"] = runtime_config

    captured = {}

    async def spy_build(self, *args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return None  # short-circuit to the available=False return path

    with patch.object(DatasetBuilder, "build", spy_build):
        client = await aiohttp_client(models_app_with_redis)
        resp = await client.get("/api/models/42/feature_importance")

    # Even though build returned None (handler responds with available=False),
    # the kwargs must have reached it.
    assert resp.status == 200
    assert captured["kwargs"].get("open_buffer_min") == 60
    assert captured["kwargs"].get("close_buffer_min") == 45
```

- [ ] **Step 2: Run it and verify it fails**

```bash
bin/test python -m pytest tests/test_dashboard_models_api.py::test_feature_importance_fallback_passes_runtime_buffers_to_builder -v
```

Expected: FAIL. The likely failure mode is `AssertionError` because `captured["kwargs"]` is empty (or missing the buffer keys) — the current handler calls `builder.build(..., as_of=row["trained_at"])` with no buffer kwargs.

- [ ] **Step 3: Commit the failing test**

```bash
git add bullforge/tests/test_dashboard_models_api.py
git commit -m "test(dashboard): assert exclusion buffers reach permutation-importance fallback"
```

---

## Task 3: Implement — thread RuntimeConfig buffers into the fallback

**Files:**
- Modify: `bullforge/services/dashboard_service/models_api.py:273-284`

- [ ] **Step 1: Edit the fallback block**

Replace the fallback block (currently around lines 273–284) so it reads both buffer keys from `runtime_config` and passes them into `DatasetBuilder.build`. Pattern mirrors the meta-shadow handler at lines 60–70 of the same file.

Before (current code):

```python
    else:
        # Fallback for HistGradientBoostingClassifier and others that don't
        # expose the attribute: permutation importance over the held-out slice
        # of the training dataset (same 80/20 split the trainer used).
        from services.model_service.dataset import DatasetBuilder, time_based_split
        from sklearn.inspection import permutation_importance

        builder = DatasetBuilder(db)
        built = await builder.build(
            row["symbol"], row["timeframe"], int(row["horizon_minutes"]),
            as_of=row["trained_at"],
        )
```

After:

```python
    else:
        # Fallback for HistGradientBoostingClassifier and others that don't
        # expose the attribute: permutation importance over the held-out slice
        # of the training dataset (same 80/20 split the trainer used).
        from services.model_service.dataset import DatasetBuilder, time_based_split
        from sklearn.inspection import permutation_importance

        # Live buffer values from RuntimeConfig — same exclusion-zone filter
        # Trainer.train applies today. None/None disables the filter, which is
        # the correct degraded behavior if Redis is unreachable (matches the
        # handler's pre-A2 contract).
        rc = request.app["runtime_config"]
        try:
            open_buf = await rc.get("equity_window_open_buffer_minutes")
            close_buf = await rc.get("equity_window_close_buffer_minutes")
        except Exception:
            logger.exception(
                "feature_importance: runtime_config.get failed; "
                "skipping exclusion filter for fallback rebuild",
            )
            open_buf = None
            close_buf = None

        builder = DatasetBuilder(db)
        built = await builder.build(
            row["symbol"], row["timeframe"], int(row["horizon_minutes"]),
            as_of=row["trained_at"],
            open_buffer_min=open_buf, close_buffer_min=close_buf,
        )
```

- [ ] **Step 2: Run the test to verify it now passes**

```bash
bin/test python -m pytest tests/test_dashboard_models_api.py::test_feature_importance_fallback_passes_runtime_buffers_to_builder -v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add bullforge/services/dashboard_service/models_api.py
git commit -m "feat(dashboard): thread RuntimeConfig equity buffers into permutation fallback"
```

---

## Task 4: Failing test — error path falls back to None/None

**Files:**
- Test: `bullforge/tests/test_dashboard_models_api.py` (append below the previous test)

This test asserts that when `runtime_config.get` raises, the handler still serves a 200 response with `open_buffer_min=None` and `close_buffer_min=None` in the kwargs (filter disabled), and an error is logged.

- [ ] **Step 1: Add the test**

Append to `bullforge/tests/test_dashboard_models_api.py`:

```python
@pytest.mark.asyncio
async def test_feature_importance_fallback_runtime_config_raises_falls_back_to_none(
    aiohttp_client, models_app_with_redis, mock_db_models, caplog,
):
    """If runtime_config.get raises (real Redis outage, mirrored from the
    meta-shadow handler's pattern), the fallback must NOT 500. It logs the
    failure and passes None/None to DatasetBuilder.build, which disables the
    exclusion filter (graceful degrade to the pre-A2 dashboard behavior).
    """
    import logging as _logging
    from unittest.mock import patch
    from services.model_service.dataset import DatasetBuilder

    _wire_model_row_on(mock_db_models, _make_model_registry_row("hist_gradient_boost"))

    runtime_config = MagicMock()
    runtime_config.get = AsyncMock(side_effect=ConnectionError("redis down"))
    models_app_with_redis["runtime_config"] = runtime_config

    captured = {}

    async def spy_build(self, *args, **kwargs):
        captured["kwargs"] = kwargs
        return None

    with patch.object(DatasetBuilder, "build", spy_build), caplog.at_level(_logging.ERROR):
        client = await aiohttp_client(models_app_with_redis)
        resp = await client.get("/api/models/42/feature_importance")

    assert resp.status == 200
    assert captured["kwargs"].get("open_buffer_min") is None
    assert captured["kwargs"].get("close_buffer_min") is None
    # The runtime_config failure is surfaced in logs (not silently swallowed).
    assert any(
        "feature_importance" in r.message and "runtime_config" in r.message
        for r in caplog.records
    )
```

- [ ] **Step 2: Run it and verify it passes**

The implementation from Task 3 already handles this path via the `try/except`, so this test should pass on first run. That's expected — the test exists to **lock** the behavior against future regressions, not to drive new code. (TDD discipline still applies: we wrote the test before declaring the path covered, and we run it to confirm.)

```bash
bin/test python -m pytest tests/test_dashboard_models_api.py::test_feature_importance_fallback_runtime_config_raises_falls_back_to_none -v
```

Expected: PASS.

- [ ] **Step 3: If it does NOT pass, the implementation is wrong — fix it before continuing**

Most likely failure: log message format doesn't include "feature_importance" and "runtime_config". Adjust the `logger.exception(...)` call in `models_api.py` to include both substrings, or relax the assertion. Re-run.

- [ ] **Step 4: Commit**

```bash
git add bullforge/tests/test_dashboard_models_api.py
git commit -m "test(dashboard): lock runtime_config-raises fallback in feature_importance handler"
```

---

## Task 5: Surface buffer values on the success-path JSON

**Files:**
- Test: `bullforge/tests/test_dashboard_models_api.py` (append a third test)
- Modify: `bullforge/services/dashboard_service/models_api.py` (the success-path response)

This adds `open_buffer_min` / `close_buffer_min` fields to the success response so the SPA can label the importance values. We only surface them when importance was actually computed — the early-return paths (`available: False`) keep their current shape.

- [ ] **Step 1: Add the failing test**

Append to `bullforge/tests/test_dashboard_models_api.py`:

```python
@pytest.mark.asyncio
async def test_feature_importance_success_response_carries_buffer_values(
    aiohttp_client, models_app_with_redis, mock_db_models,
):
    """The success-path JSON must surface the buffer values used for the
    fallback rebuild so the SPA can label whether importance was computed
    against a filtered or unfiltered population.
    """
    from unittest.mock import patch
    import numpy as np
    import pandas as pd
    from services.model_service.dataset import DatasetBuilder
    from sklearn.dummy import DummyClassifier
    from sklearn.pipeline import Pipeline

    _wire_model_row_on(mock_db_models, _make_model_registry_row("hist_gradient_boost"))

    async def _rc_get(key):
        return {
            "equity_window_open_buffer_minutes": 30,
            "equity_window_close_buffer_minutes": 20,
        }[key]

    runtime_config = MagicMock()
    runtime_config.get = AsyncMock(side_effect=_rc_get)
    models_app_with_redis["runtime_config"] = runtime_config

    # A trivially-fitted DummyClassifier wrapped in a sklearn Pipeline so the
    # ModelStore.load + permutation_importance call chain works without a real
    # artifact on disk. The pipeline lacks feature_importances_, so the handler
    # hits the fallback. We patch ModelStore.load to return our pipeline and
    # patch DatasetBuilder.build to return a tiny X/y so permutation runs fast.
    X = pd.DataFrame({
        "f1": np.linspace(0, 1, 20),
        "f2": np.linspace(1, 0, 20),
        "f3": np.random.RandomState(0).rand(20),
    })
    y = pd.Series([0, 1] * 10)
    pipe = Pipeline([("clf", DummyClassifier(strategy="stratified", random_state=0))])
    pipe.fit(X, y)

    async def spy_build(self, *args, **kwargs):
        return X, y

    from services.model_service.model_store import ModelStore

    with patch.object(DatasetBuilder, "build", spy_build), \
         patch.object(ModelStore, "load", return_value=pipe):
        client = await aiohttp_client(models_app_with_redis)
        resp = await client.get("/api/models/42/feature_importance")

    assert resp.status == 200
    body = await resp.json()
    assert body["available"] is True
    assert body["open_buffer_min"] == 30
    assert body["close_buffer_min"] == 20
```

- [ ] **Step 2: Run it and verify it fails**

```bash
bin/test python -m pytest tests/test_dashboard_models_api.py::test_feature_importance_success_response_carries_buffer_values -v
```

Expected: FAIL with a `KeyError` or `AssertionError` because the success response does not include those fields yet.

- [ ] **Step 3a: Hoist `open_buf` / `close_buf` so they're defined on both branches**

In `bullforge/services/dashboard_service/models_api.py`, add a pre-init right after the early-return `lstm` branch (so it's defined before the `if hasattr(classifier, "feature_importances_")` split). Insert immediately above the line `if hasattr(classifier, "feature_importances_"):`:

```python
    # Buffer values are only assigned in the permutation-importance fallback
    # branch. Pre-init to None so the success-path JSON shape is consistent
    # whether or not the fallback ran.
    open_buf = None
    close_buf = None
```

(In the fallback `else:` block from Task 3, the assignments to `open_buf` / `close_buf` will overwrite this pre-init. The fast path leaves them as None.)

- [ ] **Step 3b: Add the fields to the success response**

Change the final `return web.json_response({...})` of `_handle_feature_importance`. Before:

```python
    pairs = list(zip(features, importances))
    pairs.sort(key=lambda p: p[1], reverse=True)
    return web.json_response({
        "available": True,
        "features": [
            {"name": name, "importance": float(imp)}
            for name, imp in pairs[:20]
        ],
    })
```

After:

```python
    pairs = list(zip(features, importances))
    pairs.sort(key=lambda p: p[1], reverse=True)
    return web.json_response({
        "available": True,
        "features": [
            {"name": name, "importance": float(imp)}
            for name, imp in pairs[:20]
        ],
        # None on the fast path (importance came from feature_importances_);
        # int on the permutation fallback path.
        "open_buffer_min": open_buf,
        "close_buffer_min": close_buf,
    })
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
bin/test python -m pytest tests/test_dashboard_models_api.py::test_feature_importance_success_response_carries_buffer_values -v
```

Expected: PASS.

- [ ] **Step 5: Run the full feature-importance test trio together**

```bash
bin/test python -m pytest tests/test_dashboard_models_api.py -v -k feature_importance
```

Expected: 3 PASS.

- [ ] **Step 6: Commit**

```bash
git add bullforge/services/dashboard_service/models_api.py bullforge/tests/test_dashboard_models_api.py
git commit -m "feat(dashboard): surface exclusion buffer values on feature_importance JSON"
```

---

## Task 6: Document the empty `017_*` migration slot

**Files:**
- Modify: `bullforge/migrations/README.md` (append one subsection at the end)

- [ ] **Step 1: Append the new subsection**

Edit `bullforge/migrations/README.md`. The current file ends with the "## Numbering" section. Append a new "## Numbering history" subsection after it. The exact text to add:

```markdown

## Numbering history

The `017_*` slot is intentionally empty. `017_backtest_meta_columns.sql` was authored on 2026-04-17 (commit `eb3b65c`) to backfill the `estimator_type` column onto `backtest.model_registry`, but was deleted ~20 minutes later (commit `5cbc4f1`) when the targeted backfill was replaced by the general `_sync_sandbox_schema` mirror in `shared/db.py`. The slot has no production footprint — the file lived only on a feature branch and never reached master.

**Future migrations should continue from `024+` without reclaiming `017_*`.** Renumbering `018+` would touch every file from `018` onward and force the `schema_migrations` ledger to re-key on cutover for no benefit (the ledger keys on filename, not on slot index). Leave the gap.
```

- [ ] **Step 2: Run the migrations test to confirm the runner is still happy**

The README change is doc-only, but the migrations test exercises `_run_migrations` end-to-end, so it's a cheap sanity check.

```bash
bin/test python -m pytest tests/test_db.py::test_run_migrations -v
```

Expected: PASS. (If the test name is different, run `bin/test python -m pytest tests/test_db.py -v` instead and confirm everything passes.)

- [ ] **Step 3: Commit**

```bash
git add bullforge/migrations/README.md
git commit -m "docs(migrations): explain the intentionally empty 017_* slot"
```

---

## Task 7: Full verification sweep

**Files:**
- None (verification only)

- [ ] **Step 1: Run the full pytest sweep**

```bash
cd bullforge
bin/test python -m pytest tests/ -v
```

Expected: all green. If any pre-existing tests fail, first confirm they fail on master too (`git stash && bin/test python -m pytest tests/<failing>.py -v && git stash pop`); if so, that's an existing flake unrelated to this work — note it but don't fix it here. If they fail only on this branch, debug via `superpowers:systematic-debugging`.

- [ ] **Step 2: Sanity-check the diff**

```bash
git log --oneline master..HEAD
git diff master..HEAD --stat
```

Expected: 4 commits (test for buffers, impl for buffers, test+impl for JSON shape, README). Diff stat should show three files: `models_api.py`, `test_dashboard_models_api.py`, `README.md`.

- [ ] **Step 3: Verify no scope creep**

```bash
git diff master..HEAD -- bullforge/migrations/
```

Expected: ONLY `README.md` shows up. No `.sql` files renamed or renumbered. If anything else appears, you took the wrong branch on item #4 — revert it.

---

## Task 8: Code review

**Files:**
- None (review only)

- [ ] **Step 1: Run `superpowers:code-reviewer` over the full branch**

Invoke the `code-reviewer` agent with a prompt like:

> Review the `chore/exclusion-fallback-and-017-gap` branch against `docs/superpowers/specs/2026-05-04-exclusion-fallback-and-017-gap-design.md`. Two changes: (1) `services/dashboard_service/models_api.py::_handle_feature_importance` now reads equity-window buffers from RuntimeConfig and threads them into `DatasetBuilder.build`, with surface on the success-path JSON. (2) `bullforge/migrations/README.md` documents the empty 017_* slot. Verify scope, error handling, test coverage, and that nothing broke surrounding behavior. Flag anything from the spec's "Out of scope" list that crept in.

- [ ] **Step 2: Address any blocking findings**

Apply review-before-completion judgment per `superpowers:receiving-code-review`:
- **Correctness or spec-violation findings:** fix.
- **Style nits in unchanged code:** ignore (not in scope).
- **Speculative refactors (e.g. "extract this into a helper"):** ignore unless the reviewer points to a real problem.

For any fix, mirror Task 2/3/4/5 structure (failing test where applicable, minimal change, commit).

---

## Task 9: Merge to master

**Files:**
- None (merge only)

- [ ] **Step 1: Confirm the branch is clean and tests are green**

```bash
git status                       # clean tree
bin/test python -m pytest tests/ -v   # all green
```

- [ ] **Step 2: Merge with `--no-ff` (matching the 2026-05-03 session style)**

From the worktree's `bullforge/` parent (i.e., the worktree root):

```bash
cd ..   # back to .claude/worktrees/exclusion-fallback-and-017-gap
git checkout master
git merge --no-ff chore/exclusion-fallback-and-017-gap -m "Merge branch 'chore/exclusion-fallback-and-017-gap'"
```

If the worktree's `master` is detached or behind, fetch first:

```bash
git fetch origin
git rebase origin/master   # only if master had moved; otherwise skip
```

- [ ] **Step 3: Trim the items from `TODO.md`**

Both items #3 and #4 from the 2026-05-03 carry-over now have landing commits. Open `bullforge/TODO.md`, find the bullets that match (likely under a "Next session" or "Carry-over" heading, mentioning the dashboard permutation fallback gap and the 017_* slot), and either delete them or tick them off + trim per the project's established style. Single chore commit:

```bash
git add bullforge/TODO.md
git commit -m "chore(todo): trim ticked carry-over items from 2026-05-04 session"
```

- [ ] **Step 4: Pause for push permission**

Per `CLAUDE.md` and `NEXT_PROMPT.md`: push to `origin/master` is gated per-turn. Do NOT `git push` autonomously. Surface the branch state to the operator and wait for explicit "push" instruction.

- [ ] **Step 5: Clean up the worktree (after operator confirms the push)**

```bash
cd <repo>
git worktree remove .claude/worktrees/exclusion-fallback-and-017-gap
git branch -d chore/exclusion-fallback-and-017-gap
```

---

## Done criteria

- [ ] Three new tests in `tests/test_dashboard_models_api.py` pass (kwargs reach builder; runtime_config-raises falls back gracefully; success JSON carries buffer fields).
- [ ] `_handle_feature_importance` threads `open_buffer_min` / `close_buffer_min` from `request.app["runtime_config"]` into `DatasetBuilder.build`, with logged graceful fallback to `None`/`None` on `runtime_config.get` exceptions.
- [ ] Success-path JSON includes `open_buffer_min` and `close_buffer_min` (None on fast path, int on fallback).
- [ ] `migrations/README.md` has a "Numbering history" subsection explaining the empty `017_*` slot and instructing future migrations to continue from `024+`.
- [ ] No `.sql` files renamed, renumbered, or added.
- [ ] `bin/test python -m pytest tests/ -v` all green from `bullforge/`.
- [ ] One `superpowers:code-reviewer` pass with no blocking findings (or all blocking findings addressed).
- [ ] Branch merged to `master` with `--no-ff`, no Co-Authored-By trailer.
- [ ] `TODO.md` carry-over items trimmed/ticked.
- [ ] Push to `origin/master` left for the operator (gated per-turn).
