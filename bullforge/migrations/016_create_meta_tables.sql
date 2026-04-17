-- 016_create_meta_tables.sql

-- Meta-feature rows persisted at inference time (and from backtest-replay)
CREATE TABLE IF NOT EXISTS meta_features (
    symbol          TEXT        NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    direction       INTEGER     NOT NULL,  -- 1=long, -1=short
    feature_vector  JSONB       NOT NULL,
    entry_label     INTEGER,               -- 1=hit target, 0=hit stop, NULL=unlabeled
    optimal_pair_idx INTEGER,              -- index into symbol's (target,stop) pairs
    realized_ev     DOUBLE PRECISION,      -- realized expected value
    source          TEXT        NOT NULL DEFAULT 'replay',  -- 'replay' or 'live'
    PRIMARY KEY (symbol, timestamp, direction)
);

SELECT create_hypertable('meta_features', 'timestamp',
    if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS idx_meta_features_symbol_dir
    ON meta_features (symbol, direction, timestamp DESC);

-- Shadow-mode meta-model signal log for A/B/n comparison
CREATE TABLE IF NOT EXISTS meta_signals_shadow (
    id              BIGSERIAL   PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    direction       INTEGER     NOT NULL,
    estimator_type  TEXT        NOT NULL,
    entry_probability DOUBLE PRECISION NOT NULL,
    kelly_fraction  DOUBLE PRECISION,
    pair_index      INTEGER,
    target_pct      DOUBLE PRECISION,
    stop_pct        DOUBLE PRECISION,
    model_id        INTEGER     REFERENCES model_registry(id)
);

CREATE INDEX IF NOT EXISTS idx_meta_signals_shadow_lookup
    ON meta_signals_shadow (symbol, timestamp DESC);

-- Extend model_registry with estimator_type for meta-models
ALTER TABLE model_registry
    ADD COLUMN IF NOT EXISTS estimator_type TEXT;

-- Update unique index to include estimator_type for meta-models
-- (base models have estimator_type=NULL, so existing index still works)
CREATE UNIQUE INDEX IF NOT EXISTS idx_model_registry_meta_active
    ON model_registry (symbol, timeframe, horizon_minutes, model_type, estimator_type)
    WHERE is_active = TRUE AND estimator_type IS NOT NULL;
