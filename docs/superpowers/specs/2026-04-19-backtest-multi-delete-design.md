# Backtest multi-delete — design

**Date:** 2026-04-19
**Owner:** Jarquet
**Status:** Draft

## Summary

Add a dashboard feature that lets the user multi-select completed / failed /
cancelled backtest runs on the Backtests list and delete them along with their
artifacts. Running runs must be cancelled first — the Delete action never
touches an in-flight run. Deletion is hard (permanent) and removes both the
cascade-deleted tables and the non-cascaded `backtest.*` sandbox-schema rows
keyed by `backtest_run_id`.

This is app-layer cleanup (no schema migration). See Alternatives for the
DB-level cascade option that was considered and deferred.

## Motivation

The Backtests list currently has no way to remove runs. Operators accumulate
failed and exploratory runs that clutter the list and bloat the sandbox tables
(`backtest.model_registry`, `backtest.predictions`, `backtest.orders`), which
are not cascade-cleaned by the existing FK chain.

## Scope

**In scope:**

- Multi-select (checkboxes + Shift+click range) in the Backtests list table at
  [backtests.js](../../../bullforge/services/dashboard_service/static/backtests.js).
- Delete action that removes the `backtest_runs` row and its non-cascaded
  sandbox rows.
- Guardrails: running runs cannot be selected (UI) and cannot be deleted
  (server).
- Two backend endpoints: primitive single-item `DELETE` and bulk `POST /delete`.

**Out of scope:**

- Deletion of queued / running job rows (`backtest_jobs` with no `run_id`).
  Cancelling a queued job is already supported and is how the user removes
  queued items from the list.
- Soft-delete / trash / undo. Deletion is permanent.
- Invalidating detail-page state in other open tabs — stale tabs will see
  404s on subsequent API calls, which is the existing behaviour.
- Adding DB-level cascade constraints on `backtest.*` tables (see
  Alternatives).

## Artifact inventory per run

| Table | Scope | Cleanup method |
|---|---|---|
| `public.backtest_runs` (row) | The run itself | Direct DELETE (with status guard) |
| `public.backtest_folds` | Per-run | FK `ON DELETE CASCADE` (already in place) |
| `public.backtest_trades` | Per-run | FK `ON DELETE CASCADE` (already in place) |
| `public.backtest_equity` | Per-run | FK `ON DELETE CASCADE` (already in place) |
| `backtest.model_registry` | `backtest_run_id` | Explicit DELETE in helper |
| `backtest.predictions` | `backtest_run_id` | Explicit DELETE in helper |
| `backtest.orders` | `backtest_run_id` | Explicit DELETE in helper |
| `public.backtest_jobs.run_id` | Links job → run | Existing `ON DELETE SET NULL` — job row survives |
| `public.runtime_config_audit.source_backtest_run_id` | Tracks config promotions | Existing `ON DELETE SET NULL` — audit row survives, loses provenance link |

## Backend design

### Shared helper

Add `_delete_run_impl(db, run_id: int) -> dict` in
[backtests_api.py](../../../bullforge/services/dashboard_service/backtests_api.py).

```python
async def _delete_run_impl(db, run_id: int) -> dict:
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM backtest.orders WHERE backtest_run_id = $1", run_id)
            await conn.execute(
                "DELETE FROM backtest.predictions WHERE backtest_run_id = $1", run_id)
            await conn.execute(
                "DELETE FROM backtest.model_registry WHERE backtest_run_id = $1", run_id)
            row = await conn.fetchrow(
                """DELETE FROM backtest_runs
                   WHERE id = $1 AND status IN ('completed','failed','cancelled')
                   RETURNING id""",
                run_id,
            )
    if row:
        return {"status": "deleted", "run_id": run_id}
    async with db.pool.acquire() as conn:
        status = await conn.fetchval(
            "SELECT status FROM backtest_runs WHERE id = $1", run_id)
    if status is None:
        return {"status": "not_found", "run_id": run_id}
    return {"status": "still_running", "run_id": run_id, "current_status": status}
```

Properties:

- **One transaction per run.** If the status guard on the final DELETE fails
  (run is still running), the sandbox deletes roll back — the run stays
  intact.
- **No cross-run atomicity.** Bulk deletes iterate the helper; one failure
  does not abort others.
- **Status disambiguation.** A failed final DELETE could mean "not found" or
  "still running" — a follow-up `SELECT status` distinguishes them. This is a
  tiny cost (only runs when the DELETE returned zero rows).

### Routes

Registered in `register_backtests_routes` in
[backtests_api.py](../../../bullforge/services/dashboard_service/backtests_api.py#L129):

**`DELETE /api/backtests/{id}`**

- Parse `id` as int; 400 if not int.
- Call `_delete_run_impl`.
- Return 200 `{"status": "deleted", "run_id": …}` on deleted.
- Return 404 `{"error": "not found", "run_id": …}` on not_found.
- Return 409 `{"error": "run is still running", "run_id": …, "current_status": …}`
  on still_running.

**`POST /api/backtests/delete`**

- Body: `{"ids": [int, …]}`.
- Validation:
  - Parse JSON; 400 on bad JSON.
  - `ids` must be a list of ints; 400 otherwise.
  - Empty list → 400 `"ids must be a non-empty list"`.
  - `len(ids) > 500` → 400 `"too many ids (max 500)"`.
  - Dedupe before iterating.
- Iterate, calling `_delete_run_impl` per id.
- Return 200 with (ids use `run_id` consistently, matching the cancel
  endpoint's convention at
  [backtests_api.py:125](../../../bullforge/services/dashboard_service/backtests_api.py#L125)):
  ```json
  {
    "deleted": [1, 2, 3],
    "not_found": [99],
    "still_running": [{"run_id": 42, "current_status": "running"}]
  }
  ```

### Route ordering note

In aiohttp, routes match in registration order. `POST /api/backtests/delete`
must be registered *before* the `{id}` routes so it isn't shadowed by a
parameterized pattern. See the comment at
[backtests_extra_api.py:14](../../../bullforge/services/dashboard_service/backtests_extra_api.py#L14)
for the existing precedent.

## Frontend design

Changes in
[backtests.js](../../../bullforge/services/dashboard_service/static/backtests.js)
and
[backtests.css](../../../bullforge/services/dashboard_service/static/backtests.css).

### Table shape

- Prepend a checkbox column to `.bt-table` (`<th>` and `<td>` at index 0).
- Header checkbox `#bt-select-all` toggles every *selectable* (non-disabled)
  row on the current page.
- Row checkboxes carry `data-kind`, `data-id`, `data-status`, `data-name`
  attributes and a stable DOM id `bt-sel-{kind}-{id}`.
- Disabled conditions (Q6 guardrail):
  - `kind === 'job'` (any job row — queued/running jobs must be cancelled).
  - `kind === 'run' && status === 'running'` (running run — must be cancelled).
  - Disabled checkboxes have `title="Cancel this backtest first"`.

### Selection state

Module-scoped:

```js
const selection = new Set();  // keys of form "run:123"
let lastChecked = null;       // last clicked checkbox element (for Shift-range)
```

- Selection keys use `"run:{id}"`; jobs can never enter the Set (disabled).
- `reload()` preserves selection across the 10-second poll: after re-rendering
  rows, re-check boxes whose keys are still in `selection`, and prune keys
  whose rows no longer appear.

### Shift+click range

On checkbox click:

1. If `ev.shiftKey && lastChecked`, compute indices of `lastChecked` and the
   clicked box in the current order of *selectable* checkboxes.
2. Set every selectable checkbox in `[min, max]` to the clicked box's new
   checked state. Sync Set accordingly.
3. Always update `lastChecked` to the clicked box.

### Action bar

- Container placed inside `.bt-overview`, just above the table.
- Hidden when `selection.size === 0`.
- When visible: `Delete selected (N)` button + `Clear` link.
- `Clear` empties the Set, unchecks boxes, hides the bar.

### Confirm + dispatch

- Click `Delete selected` → `confirm("Delete N backtest(s)? This cannot be undone.")` (minimal, Q4 A).
- On confirm, dispatch:
  - If `selection.size <= 10`: parallel `DELETE /api/backtests/{id}` via
    `Promise.allSettled`; classify by HTTP status (200 → deleted, 404 →
    not_found, 409 → still_running).
  - If `selection.size > 10`: single `POST /api/backtests/delete` with
    `{ids: [...]}`; read the response buckets.
- After dispatch, show an inline summary in the action bar:
  `"Deleted N · Skipped M (M still running)"` — auto-clears after 5s or on
  next `reload()`. Selection is cleared; `reload()` is called.

### Interactions with existing polling

`setInterval(reload, 10000)` continues while the confirm dialog is open. Mid-
confirm deletion of a selected row by another session → that id lands in
`not_found` when the delete dispatches. This is reported, not treated as an
error.

## Error handling summary

| Situation | Single DELETE | Bulk POST |
|---|---|---|
| `id` malformed | 400 | 400 for the whole request if any non-int |
| Run not found | 404 | id goes to `not_found` bucket |
| Run still running | 409 | id goes to `still_running` bucket |
| Empty body / empty ids | n/a | 400 |
| Too many ids (>500) | n/a | 400 |
| DB error mid-transaction | 500; partial state rolled back | per-id; other ids unaffected |

## Testing

Backend — integration tests against a real DB (per the project's standing
"no-mock DB" rule):

1. `DELETE /api/backtests/{id}` on a completed run → 200, cascaded tables
   emptied for that run, sandbox tables emptied for that `backtest_run_id`,
   `runtime_config_audit` row for that run survives with
   `source_backtest_run_id = NULL`, `backtest_jobs.run_id` for the linked job
   becomes NULL.
2. `DELETE /api/backtests/{id}` on a `status='running'` run → 409, nothing
   deleted (cascaded tables still populated, sandbox tables still populated).
3. `DELETE /api/backtests/{id}` on a nonexistent id → 404.
4. `POST /api/backtests/delete` with a mix of completed / running /
   nonexistent ids → buckets correctly populated.
5. `POST /api/backtests/delete` validation: `ids=[]` → 400; 501 ids → 400;
   non-int element → 400; duplicates deduped.
6. Idempotency: `DELETE` then `DELETE` same id → 200 then 404.

Frontend — manual test plan (no JS test harness exists in the repo):

1. Select 3 completed runs, click Delete, confirm → rows vanish; action bar
   shows `Deleted 3`.
2. Shift+click range: click first checkbox, Shift+click fifth → exactly 5
   boxes checked; action bar count matches.
3. On a running run, checkbox is disabled and has the "Cancel this backtest
   first" tooltip.
4. Select 12 runs: network tab shows a single `POST /api/backtests/delete`,
   not 12 `DELETE` calls.
5. Header `#bt-select-all` checks/unchecks only selectable rows; disabled
   rows stay unselected.
6. With a run selected, wait for the 10s poll: selection persists if the row
   is still present; is pruned if the row is gone.

## Alternatives considered

**DB-level cascade (deferred).** Add `FOREIGN KEY (backtest_run_id) REFERENCES
public.backtest_runs(id) ON DELETE CASCADE` to all three `backtest.*` sandbox
tables. Deletion would then be a single `DELETE FROM backtest_runs`. Deferred
because it expands scope (migration + orphan scrub + a tiny FK-check cost on
every sandbox-row insert during live backtests) and the app-layer approach is
easy to migrate to the DB-level approach later if we want that stronger
invariant.

**Soft-delete.** Mark rows `deleted_at` instead of deleting. Rejected — user
explicitly wants artifacts removed, and the sandbox tables are the primary
reason (storage reclaim).

## Open questions

None.
