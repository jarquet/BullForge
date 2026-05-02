-- 021_meta_direction_in_active_index.sql
--
-- Meta artifacts encode direction in artifact_path
-- (`meta_entry_long_lightgbm_*`, `meta_entry_short_*`) — there is no
-- direction column. Migration 016 added `idx_model_registry_meta_active`
-- on (symbol, timeframe, horizon_minutes, model_type, estimator_type),
-- which forbids two active rows for the same key regardless of direction.
-- Once batch-7's MetaTrainer started preserving long across the short
-- iteration's deactivation, every train_entry(long) collided with the
-- existing active short row under that index.
--
-- Fix: include a direction component (parsed from artifact_path) in the
-- meta unique index, AND exclude meta rows from the broader
-- idx_model_registry_active so it only governs base models. Once meta has
-- a real `direction` column, this functional expression can be replaced
-- with a column reference; the naming convention is already part of the
-- MetaTrainer / load_meta_models contract so the parse is no more fragile
-- than what _parse_meta_artifact already does at load time.

-- Recreate the broader active index limited to base models. Base models
-- have estimator_type=NULL; the prior version applied to meta rows too,
-- which compounded the duplicate-key error from the meta-specific index.
DROP INDEX IF EXISTS idx_model_registry_active;
CREATE UNIQUE INDEX idx_model_registry_active
    ON model_registry (symbol, timeframe, horizon_minutes, model_type)
    WHERE is_active = TRUE AND estimator_type IS NULL;

-- Recreate the meta active index with a direction expression derived from
-- artifact_path. The CASE returns 1 for long, -1 for short, 0 otherwise
-- (legacy or non-conforming paths). Two active meta rows are now allowed
-- for the same (symbol, timeframe, horizon, model_type, estimator_type)
-- as long as their directions differ.
DROP INDEX IF EXISTS idx_model_registry_meta_active;
CREATE UNIQUE INDEX idx_model_registry_meta_active
    ON model_registry (
        symbol, timeframe, horizon_minutes, model_type, estimator_type,
        (CASE
            WHEN artifact_path LIKE '%\_long\_%' ESCAPE '\' THEN 1
            WHEN artifact_path LIKE '%\_short\_%' ESCAPE '\' THEN -1
            ELSE 0
         END)
    )
    WHERE is_active = TRUE AND estimator_type IS NOT NULL;
