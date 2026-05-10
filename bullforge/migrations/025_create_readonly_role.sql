-- Read-only Postgres role for diagnostic / observability tooling
-- (e.g. a local MCP Postgres server tunneled in over SSH).
--
-- Password is intentionally NOT set here — this file is checked into git.
-- After the migration applies, an operator must set one out-of-band:
--
--   ALTER ROLE bullforge_ro WITH PASSWORD '<generated-secret>';
--
-- The role is created with LOGIN, but with no password it cannot
-- authenticate under the default md5/scram pg_hba rules. Fail-closed
-- until the operator explicitly opts in.
--
-- ALTER DEFAULT PRIVILEGES is set on public + backtest so that any
-- future migration creating a table grants SELECT to bullforge_ro
-- automatically — no need to revisit this file on every schema change.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bullforge_ro') THEN
    CREATE ROLE bullforge_ro LOGIN;
  END IF;

  EXECUTE format('GRANT CONNECT ON DATABASE %I TO bullforge_ro', current_database());
END $$;

GRANT USAGE ON SCHEMA public TO bullforge_ro;
GRANT USAGE ON SCHEMA backtest TO bullforge_ro;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO bullforge_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA backtest TO bullforge_ro;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO bullforge_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA backtest
  GRANT SELECT ON TABLES TO bullforge_ro;
