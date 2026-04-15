-- Backtest sandbox schema. Writes to mirrored tables (orders, model_registry,
-- predictions) from a sandbox Database (search_path=backtest,public) land here
-- instead of public, so backtest runs do not pollute live execution state.
--
-- Notes:
--   * LIKE ... INCLUDING ALL copies columns/indexes/constraints but not
--     hypertable status — backtest.predictions is a plain table, not a
--     TimescaleDB hypertable. This is fine for sandbox use (short-lived rows,
--     truncate between runs).
--   * Sandbox Database instances skip _run_migrations(); a live Database must
--     have connected at least once to apply this migration before sandbox use.

CREATE SCHEMA IF NOT EXISTS backtest;

CREATE TABLE IF NOT EXISTS backtest.orders (LIKE public.orders INCLUDING ALL);
CREATE TABLE IF NOT EXISTS backtest.model_registry (LIKE public.model_registry INCLUDING ALL);
CREATE TABLE IF NOT EXISTS backtest.predictions (LIKE public.predictions INCLUDING ALL);
