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

The cutover heuristic uses `bars_1m` as a canary because `001_create_bars.sql` creates it. The heuristic does NOT verify that every migration's effects are present — a partially-migrated DB (e.g., a deploy that crashed midway through) gets bulk-marked as fully applied. To recover from such a state: `DROP TABLE schema_migrations` and reconnect; this re-triggers cutover-detect, which is identical to the operator running `_run_migrations` from scratch on an idempotent migration set.

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
