# Migrations

Numbered SQL files run by `shared/db.py::Database._run_migrations` on the first non-sandbox `Database.connect()` per process. Concurrent service starts serialize through `pg_advisory_lock(0x42554C4C)`.

## Filename = primary key

Applied migrations are recorded in the `schema_migrations(filename TEXT PRIMARY KEY, applied_at TIMESTAMPTZ)` table. The table is self-bootstrapped inline at the top of `_run_migrations` — there is no `024_create_schema_migrations.sql` because that file would itself need a ledger to guard it.

**`schema_migrations.filename` is the *literal filename* with extension** (e.g. `001_create_bars.sql`). Renaming a migration file after it has been applied to any environment is a one-way door:

- The ledger entry under the old name remains as a tombstone.
- The renamed file looks "new" to `_run_migrations` and gets executed on the next connect.
- Production environments where the migration is already applied need either a manual `UPDATE schema_migrations SET filename=… WHERE filename=…` or the migration body must be idempotent enough to safely re-run.

If you must rename, prefer keeping the migration body idempotent (`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, hand-rolled `DO $$ … IF data_type='X'` guards). The 022→023 chain follows this pattern.

## Cutover (one-time)

When B1 (the ledger) ships, the cutover deploy hits a populated production DB whose `schema_migrations` table doesn't exist yet. `_run_migrations` detects this — empty ledger AND `bars_1m` already exists — and bulk-inserts every current filename as already-applied without re-running anything. Subsequent connects find a populated ledger and use the steady-state path.

The cutover heuristic uses `bars_1m` as a canary because `001_create_bars.sql` creates it. The heuristic does NOT verify that every migration's effects are present — a partially-migrated DB (e.g., a deploy that crashed midway through) gets bulk-marked as fully applied, and the missing migrations are silently skipped on subsequent connects.

**Recovery paths:**

- **Wrongly-empty ledger on a fully-applied DB.** Operator drops the ledger; reconnect re-triggers cutover-detect and re-populates from current filenames. Safe.
- **Partially-applied DB.** `DROP TABLE schema_migrations` does NOT recover this — reconnect re-triggers cutover-detect, which marks every file as applied (including the missing ones), and the broken state persists. Recovery requires the operator to first run the missing migrations manually (e.g., via `psql`), then drop and rebuild the ledger. Idempotency of every migration body makes the manual re-run safe even for files that are already partially applied.

## Idempotency convention

Idempotency is no longer load-bearing for correctness (the ledger guards re-runs), but new migrations should still be idempotent for two reasons:

1. **Cutover detection assumes idempotency.** If a future cutover-recovery scenario (`DROP TABLE schema_migrations`) happens against a partially-applied DB, the ledger gets wiped and the per-file path runs every migration that isn't already in the (now-empty) ledger. Idempotent files are safe to re-run; non-idempotent ones are not.
2. **Manual ops.** Operators sometimes run a migration directly via `psql` to verify the SQL before committing. Idempotency means the same script can be applied multiple times safely.

Patterns established by 001-023:

- Tables: `CREATE TABLE IF NOT EXISTS`.
- Columns: `ADD COLUMN IF NOT EXISTS`.
- Indexes: `CREATE INDEX IF NOT EXISTS` (or `DROP INDEX IF EXISTS` first if recreating).
- Type changes: `DO $$ BEGIN IF (SELECT data_type FROM information_schema.columns WHERE …) = 'X' THEN ALTER COLUMN … END IF; END $$;`.

## Numbering

Files are applied in lexical (`sorted(glob('*.sql'))`) order. Use `NNN_description.sql` (zero-padded to three digits). If two migrations land in the same PR cycle and a number collision happens, suffix one of them with `a`/`b`/etc. (e.g. `019_…` and `019b_…`). The `_` (0x5F) sorts before letters (`a`-`z` are 0x61-0x7A), so the unsuffixed file lex-sorts first.

## Numbering history

The `017_*` slot is intentionally empty. `017_backtest_meta_columns.sql` was authored on 2026-04-17 (commit `eb3b65c`) to backfill the `estimator_type` column onto `backtest.model_registry`, but was deleted ~20 minutes later (commit `5cbc4f1`) when the targeted backfill was replaced by the general `_sync_sandbox_schema` mirror in `shared/db.py`. The slot has no production footprint — the file lived only on a feature branch and never reached master.

**Future migrations should continue from `024+` without reclaiming `017_*`.** Renumbering `018+` would touch every file from `018` onward and force the `schema_migrations` ledger to re-key on cutover for no benefit (the ledger keys on filename, not on slot index). Leave the gap.
