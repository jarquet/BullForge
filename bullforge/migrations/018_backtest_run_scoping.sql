-- 018_backtest_run_scoping.sql
--
-- Scopes backtest-sandbox rows to (backtest_run_id, fold_index) so concurrent
-- backtests can't race on the cross-run `is_active` singleton. Live rows
-- (public schema, written with scope=None) keep both columns NULL.
--
-- Sandbox-side copies of these columns would normally be propagated by the
-- live-pool schema sync on the next connect(), but we add them explicitly
-- here too because the partial unique index below references them.

ALTER TABLE public.model_registry ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE public.model_registry ADD COLUMN IF NOT EXISTS fold_index INTEGER;
ALTER TABLE public.predictions    ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE public.predictions    ADD COLUMN IF NOT EXISTS fold_index INTEGER;
ALTER TABLE public.orders         ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE public.orders         ADD COLUMN IF NOT EXISTS fold_index INTEGER;

ALTER TABLE backtest.model_registry ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE backtest.model_registry ADD COLUMN IF NOT EXISTS fold_index INTEGER;
ALTER TABLE backtest.predictions    ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE backtest.predictions    ADD COLUMN IF NOT EXISTS fold_index INTEGER;
ALTER TABLE backtest.orders         ADD COLUMN IF NOT EXISTS backtest_run_id BIGINT;
ALTER TABLE backtest.orders         ADD COLUMN IF NOT EXISTS fold_index INTEGER;

-- Drop unscoped indexes inherited from `LIKE public.<t> INCLUDING ALL` in
-- migration 014. These enforced cross-run uniqueness on
-- (symbol, timeframe, horizon_minutes, model_type) WHERE is_active = TRUE,
-- preventing concurrent runs on overlapping combos. The scoped index created
-- below replaces the constraint with the right granularity.
--
-- Postgres auto-renames indexes when copying via LIKE INCLUDING ALL based on
-- the columns, not the original name. We drop them by their actual names.
DROP INDEX IF EXISTS backtest.idx_model_registry_active;
DROP INDEX IF EXISTS backtest.backtest_idx_model_registry_active;
DROP INDEX IF EXISTS backtest.model_registry_symbol_timeframe_horizon_minutes_model_type_idx;
DROP INDEX IF EXISTS backtest.idx_model_registry_meta_active;
DROP INDEX IF EXISTS backtest.backtest_idx_model_registry_meta_active;
DROP INDEX IF EXISTS backtest.model_registry_symbol_timeframe_horizon_minutes_model_type_estimator_type_idx;

-- Wipe intermediate backtest data. User-visible backtest results live in
-- public.backtest_trades / backtest_equity / backtest_folds (already
-- run_id-scoped) and are NOT touched.
TRUNCATE backtest.model_registry, backtest.predictions, backtest.orders;

CREATE UNIQUE INDEX IF NOT EXISTS idx_backtest_model_registry_scoped_active
    ON backtest.model_registry
    (backtest_run_id, fold_index, symbol, timeframe, horizon_minutes, model_type)
    WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_backtest_model_registry_scope_lookup
    ON backtest.model_registry (backtest_run_id, fold_index);
CREATE INDEX IF NOT EXISTS idx_backtest_predictions_scope_lookup
    ON backtest.predictions (backtest_run_id, fold_index);
CREATE INDEX IF NOT EXISTS idx_backtest_orders_scope_lookup
    ON backtest.orders (backtest_run_id, fold_index);
