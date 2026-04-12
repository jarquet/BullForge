CREATE TABLE IF NOT EXISTS model_registry (
    id              SERIAL          PRIMARY KEY,
    symbol          TEXT            NOT NULL,
    timeframe       TEXT            NOT NULL,
    horizon_minutes INTEGER         NOT NULL,
    model_type      TEXT            NOT NULL,
    artifact_path   TEXT            NOT NULL,
    metrics         JSONB           NOT NULL DEFAULT '{}',
    feature_columns JSONB           NOT NULL DEFAULT '[]',
    trained_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    train_samples   INTEGER         NOT NULL,
    test_samples    INTEGER         NOT NULL,
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE
);

-- Only one active model per symbol/timeframe/horizon/type combination
CREATE UNIQUE INDEX IF NOT EXISTS idx_model_registry_active
    ON model_registry (symbol, timeframe, horizon_minutes, model_type)
    WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_model_registry_lookup
    ON model_registry (symbol, timeframe, horizon_minutes);
