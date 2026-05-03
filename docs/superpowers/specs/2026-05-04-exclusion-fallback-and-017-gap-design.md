# Dashboard exclusion-buffer fallback + 017 migration-slot doc — design

**Status:** approved (auto-mode brainstorm)
**Date:** 2026-05-04
**Author:** Claude (Opus 4.7)
**Source:** `NEXT_PROMPT.md` (carry-over from the 2026-05-03 training-exclusion-zone session) — items #3 and #4

Two small leftovers from the migrations-hygiene + training-exclusion-zone session of 2026-05-03. Bundled into one spec because they share a worktree, are both low-risk, and would be silly to plan separately.

## Item #3 — pass equity buffer minutes to the dashboard's permutation-importance fallback

### Problem

`services/dashboard_service/models_api.py::_handle_feature_importance` falls back to `sklearn.inspection.permutation_importance` for classifiers that don't expose `feature_importances_` (e.g. `HistGradientBoostingClassifier`). Inside that fallback (lines 280–284) it calls `DatasetBuilder.build(...)` *without* `open_buffer_min` / `close_buffer_min`, so the rebuilt training set bypasses the equity-exclusion filter that A2 (2026-05-03) put on the trainer's path. The dashboard therefore reports importance values computed against a population the live trainer would no longer fit on. A real correctness gap — small in scope, but a divergence worth closing now that A2 ships.

### Goals

1. The fallback rebuild must filter rows the same way `Trainer.train` does today, using the operator-tuned buffer values from `RuntimeConfig`.
2. Backward-compat with the rest of the handler's contract: same response shape, same status codes, same early-return paths.
3. Surface enough information in the success-path response that the SPA can label whether importance was computed against a filtered or unfiltered population.
4. Degrade gracefully if `runtime_config.get` raises (Redis outage) — don't 500 the handler; fall back to the unfiltered behavior the endpoint shipped with.

### Design

**Buffer source — option (1), live values.** Read both keys from `request.app["runtime_config"]`:

```python
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

`DatasetBuilder.build`'s contract treats either kwarg being `None` as "filter disabled", which is the correct degraded behavior for a Redis outage. Pattern matches the meta-shadow handler in the same file (`models_api.py:60–70`).

**JSON response (success path only).** Add `open_buffer_min` and `close_buffer_min` to the success response so the SPA can label the importance values. Skip on the early-return paths (`available: False` cases) — those mean importance wasn't computed at all, so the buffer values are irrelevant.

**What we explicitly don't do.**

- We don't persist training-time buffer values in `model_registry` (option 2). That would touch the schema, every `Trainer.train` call site, and the meta-trainer, for a niche correctness improvement. Tracked in TODO if it later proves to be worth the churn.
- We don't change error handling on the meta-shadow handler — only on the feature-importance handler. The two are independent.
- We don't add a UI label change in this work; the JSON addition is forward-compat for a future SPA tweak.

### Caveat (logged in the spec, not in code)

If the operator changed buffer values between training and request time, passing the live values still produces a less-divergent training set than passing nothing — but it does not perfectly recreate the population the artifact was fit on. We accept that delta. The exact-recreation alternative is option (2), tracked as a TODO.

### Test plan (TDD, hermetic)

Mirror `tests/test_backtest_engine_overrides.py::test_backtest_engine_passes_buffer_minutes_to_trainer` and the `models_app_with_redis` fixture in `tests/test_dashboard_models_api.py`:

1. **Happy path — buffers reach `build()`:**
   - `runtime_config.get` returns 60 for `equity_window_open_buffer_minutes`, 45 for `equity_window_close_buffer_minutes` (distinct values to catch a key swap).
   - Mock the model row to surface a classifier without `feature_importances_` so we hit the permutation branch.
   - `patch.object(DatasetBuilder, "build", spy_build)` to capture kwargs; spy returns `None` so the handler short-circuits before importing sklearn / loading an artifact (avoids needing a real artifact on disk).
   - Assert `captured["open_buffer_min"] == 60` and `captured["close_buffer_min"] == 45`.

2. **Degraded path — runtime_config raises:**
   - `runtime_config.get` raises `ConnectionError`.
   - Same spy contract.
   - Assert handler returns 200 (or the existing `available: False` 200 from the spy returning `None`), kwargs are both `None`, and an error was logged.

Optional polish: a third assertion confirming the success-path response carries `open_buffer_min` / `close_buffer_min`. Skip if the spy-returns-None scaffold makes that awkward — the field is forward-compat, not load-bearing.

### Files touched

- `bullforge/services/dashboard_service/models_api.py` — ~10 lines in `_handle_feature_importance`.
- `bullforge/tests/test_dashboard_models_api.py` — two new tests + any imports.

## Item #4 — document the empty `017_*` migration slot

### Problem

`bullforge/migrations/` jumps from `016_create_meta_tables.sql` to `018_backtest_run_scoping.sql`. To a future contributor scanning the directory, the gap looks like an accident; without context they might either (a) renumber `018+` down to close it or (b) reclaim `017_*` for a new migration. Both are wrong: the slot was used by a feature-branch migration that lived for 20 minutes before being deleted in favor of a framework-level fix. The doc needs to say so.

### Verified history

- `eb3b65c` (2026-04-17 16:29) added `017_backtest_meta_columns.sql` to backfill `estimator_type` onto `backtest.model_registry`.
- `5cbc4f1` (~20 minutes later) deleted it: the targeted backfill was generalized into `_sync_sandbox_schema` in `shared/db.py`, which made the per-migration fix unnecessary.
- The 017 file never landed on master. No production environment ever ran it.

### Design — doc-only

Append a "Numbering history" subsection to `bullforge/migrations/README.md` after the existing "Numbering" section. Suggested wording (lifted from `NEXT_PROMPT.md` with light polish):

> ## Numbering history
>
> The `017_*` slot is intentionally empty. `017_backtest_meta_columns.sql` was authored on 2026-04-17 (commit `eb3b65c`) to backfill the `estimator_type` column onto `backtest.model_registry`, but was deleted ~20 minutes later (commit `5cbc4f1`) when the targeted backfill was replaced by the general `_sync_sandbox_schema` mirror in `shared/db.py`. The slot has no production footprint.
>
> **Future migrations should continue from `024+` without reclaiming `017_*`.** Renumbering `018+` would touch every file from `018` onward and force the `schema_migrations` ledger to re-key on cutover for no benefit (the ledger keys on filename, not slot number). Leave the gap.

### Why not renumber

- Verified history shows the gap is intentional, not an accident.
- Renumbering touches 6+ files (`018`, `019`, `019b`, `020`, `021`, `022`, `023`) and forces a ledger re-key on cutover.
- The B1 ledger keys on filename, not on slot index — gaps are cosmetic.

### Test plan

None for the doc change itself. Run `tests/test_db.py::test_run_migrations` once after the README edit lands to confirm nothing about the migration runner is upset. The full pytest sweep would catch any regression anyway.

### Files touched

- `bullforge/migrations/README.md` — one new subsection (~12 lines).

## Workflow

1. One worktree: `chore/exclusion-fallback-and-017-gap`.
2. TDD for #3 (failing test → minimal change → green). #4 is doc-only; no test.
3. Verification: `bin/test python -m pytest tests/ -v` from `bullforge/` — all green before claiming done.
4. One `superpowers:code-reviewer` pass at the end.
5. Conventional Commits, no Co-Authored-By trailer, `--no-ff` merge — matching the 2026-05-03 session's style.
6. After merge: trim both items from `TODO.md`. The remaining sections (Phase 2, Phase 1.5 follow-ups) are time-gated until ~2026-05-14, so the repo is at a natural pause.

## Out of scope

- Phase 2 work (gated on shadow-data accumulation through ~2026-05-14).
- Persisting buffer values in `model_registry` (option 2 under #3) — TODO candidate.
- Vectorizing the per-row exclusion filter (S-2 from the A2 review) — speculative without measured pain.
- Renumbering `018+` migrations — explicitly the wrong call.
- Holiday-calendar handling in `is_trading_allowed`.
- `RiskManager`, `runtime_config`, or dashboard Config-tab refactors beyond what these items require.
