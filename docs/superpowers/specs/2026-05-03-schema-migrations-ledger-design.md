# `schema_migrations` ledger — design

**Status:** approved (auto-mode brainstorm)
**Date:** 2026-05-03
**Author:** Claude (Opus 4.7)
**Source:** TODO.md § "Migrations / DB schema hygiene", item 1 ("Add a `schema_migrations` ledger so applied migrations are skipped automatically")

## Problem

`shared/db.py::_run_migrations` (lines 217–222) re-executes every numbered SQL file in `bullforge/migrations/` on every live `Database.connect()`. There is no `(filename, applied_at)` ledger.

Today's implicit convention is "every migration must be self-idempotent" — `CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, hand-rolled `DO $$ ... IF data_type='X'` guards for type changes. Migrations 001–023 follow this by accident of the patterns they used, but the convention is undocumented and a contributor who lands a non-idempotent migration introduces a silent restart-time DDL flap (every service start re-runs the ALTER and pays whatever lock it requires).

The footgun was surfaced by the 023 (`bars.volume` widen to double) review, which caught that an unguarded `ALTER COLUMN ... TYPE` would re-run on every restart and pay `AccessExclusiveLock` on `bars_1m` / `bars_daily` each time.

## Goals

1. Skip already-applied migrations on subsequent connects without relying on per-migration idempotency.
2. Don't break the production cutover: a healthy DB with all 22 migrations applied should not suddenly re-run them on first deploy of this change.
3. Don't change behavior for sandbox / backtest connections.
4. Inherit the existing `pg_advisory_lock(0x42554C4C)` serialization so concurrent service starts can't race the ledger.
5. Self-bootstrap — the ledger must come into existence on a fresh DB without depending on a numbered migration file (chicken-and-egg).

## Design

### Inline ledger creation

At the top of `_run_migrations` (before the file loop), execute unconditionally:

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Idempotent. Runs every connect, no-ops once the table exists. Solves the chicken-and-egg by living inline rather than as `024_create_schema_migrations.sql`.

### Cutover detection (one-time)

After creating the ledger, check whether this is the cutover case (ledger empty but schema already populated). The detection:

```python
ledger_count = await conn.fetchval("SELECT COUNT(*) FROM schema_migrations")
if ledger_count == 0:
    schema_populated = await conn.fetchval(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name='bars_1m')"
    )
    if schema_populated:
        # Production cutover: bulk-insert all current filenames as already-applied.
        for sql_file in sql_files:
            await conn.execute(
                "INSERT INTO schema_migrations (filename) VALUES ($1) "
                "ON CONFLICT DO NOTHING",
                sql_file.name,
            )
        return  # No migrations re-run.
```

`bars_1m` is the canary because it's created by `001_create_bars.sql` — the very first migration, and a hypertable so its presence is unambiguous.

After the bulk-insert, subsequent connects find `ledger_count > 0` and fall through to the steady-state path.

### Steady-state per-file guard

Otherwise, for each `.sql` file in sorted order:

```python
applied = {r["filename"] for r in await conn.fetch(
    "SELECT filename FROM schema_migrations"
)}
for sql_file in sql_files:
    if sql_file.name in applied:
        continue
    sql = sql_file.read_text()
    await conn.execute(sql)
    await conn.execute(
        "INSERT INTO schema_migrations (filename) VALUES ($1) "
        "ON CONFLICT DO NOTHING",
        sql_file.name,
    )
```

`ON CONFLICT DO NOTHING` is defensive against a rare race where two services slip past the advisory lock (shouldn't happen, but cheap insurance).

### Concurrency

The whole `_run_migrations` body already runs under `pg_advisory_lock(0x42554C4C)` (db.py:200–209). The ledger reads/writes inherit that serialization — no separate locking needed.

### Sandbox / backtest

`Database.connect()` only calls `_run_migrations` when `not self._sandbox` (db.py:198). Sandbox connections never see the ledger and don't write to `schema_migrations`. The `backtest.*` schema is structurally synced from `public` by `_sync_sandbox_schema`, but `schema_migrations` lives in `public` and isn't a candidate for sync (it's metadata, not application data).

### Rollback / recovery

No env-var escape hatch. If the ledger logic misbehaves, the operator can:

1. `DROP TABLE schema_migrations` — next connect creates it inline, sees empty ledger + populated schema → cutover-detect → bulk-prepopulate from current files. Idempotent, safe.
2. Or revert this change. The pre-change behavior re-runs every file, which is fine because every file is idempotent.

## Tests (TDD)

In `tests/test_db.py`:

1. **`test_ledger_records_all_migrations`** — fresh connect from `bin/test`'s clean DB. After connect, assert `schema_migrations` contains exactly the set of filenames in `migrations/`.

2. **`test_ledger_skips_on_second_connect`** — connect, capture `applied_at` per row. Disconnect, reconnect. Assert `applied_at` per row is stable (no migration re-ran, which would have either updated the row via `ON CONFLICT DO NOTHING` (it doesn't, so timestamps stay) or inserted a new sentinel side effect).

3. **`test_cutover_bulk_prepopulate`** — connect on fresh DB; drop `schema_migrations`; insert a sentinel row into `bars_1m`; reconnect. Assert: ledger now contains every current filename, AND the sentinel row is still in `bars_1m` (proving migration `001_create_bars.sql` did not re-execute and clobber the table).

## Files changed

- `bullforge/shared/db.py` — replace 6-line `_run_migrations` with the design above.
- `bullforge/tests/test_db.py` — add three new tests.
- `bullforge/migrations/README.md` — short note describing the ledger and the "filename is the primary key" convention. (Not a substitute for the ledger; just a hand-off for future contributors.)

## Out of scope

- Migrating to alembic / a third-party migration runner. Numbered SQL files stay; this just adds an idempotency safety net.
- A per-migration checksum (detect if a file's contents changed after it was applied). Possible follow-up; not needed today.
- Documenting / enforcing the "every migration must be self-idempotent" convention. The ledger makes idempotency optional going forward; existing migrations stay idempotent for the cutover path.
- CI lint for unguarded `ALTER COLUMN ... TYPE`. Complementary to this work; tracked separately.

## Implementation notes (post-spec)

Three deltas from the test design above were applied during implementation:

1. **Test #2 mechanism strengthened.** Instead of comparing `applied_at` timestamps across two connects, the test (`test_ledger_skips_already_applied`) writes a non-idempotent sentinel migration (`INSERT INTO ledger_test_sentinel(...)`) into a temp `migrations_dir` and asserts the row count stays at 1 across two `_run_migrations` calls. If the ledger fails to skip, the row count goes 1→2 — a stronger and more direct proof than timestamp comparison.

2. **`_run_migrations` gained a private `migrations_dir=None` keyword argument.** Production callers don't pass it; tests pass a `tmp_path` to point at synthetic migrations.

3. **Per-file (execute SQL + INSERT ledger row) wrapped in `async with conn.transaction():`.** Closes a small gap where a connection drop between the two could leave the migration applied but unrecorded. Idempotency made this safe before; the transaction makes it correct regardless. None of the existing migrations contain DDL incompatible with explicit transactions (no `CREATE INDEX CONCURRENTLY`, no `VACUUM`, no `REINDEX`).
