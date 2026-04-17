-- 017_backtest_meta_columns.sql
--
-- Backfills the meta-model schema additions from migration 016 onto the
-- backtest sandbox copy of model_registry.
--
-- Migration 014 creates `backtest.model_registry` via `LIKE public.model_registry
-- INCLUDING ALL` -- a one-time snapshot. Migration 016's unqualified
-- `ALTER TABLE model_registry ADD COLUMN estimator_type` only hits
-- `public.model_registry`; the sandbox copy is left behind. The backtest worker
-- writes through a sandbox connection (search_path=backtest,public) and
-- resolves `model_registry` to the sandbox table first, so the missing column
-- raises UndefinedColumnError on every model insert.
--
-- This migration syncs the sandbox table. Future migrations that ALTER any of
-- the tables mirrored in 014 (orders, model_registry, predictions) MUST do the
-- same for the `backtest.*` copy, or use a fully-qualified `public.<table>`
-- name. See follow-up tracked separately.

ALTER TABLE backtest.model_registry
    ADD COLUMN IF NOT EXISTS estimator_type TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_backtest_model_registry_meta_active
    ON backtest.model_registry (symbol, timeframe, horizon_minutes, model_type, estimator_type)
    WHERE is_active = TRUE AND estimator_type IS NOT NULL;
