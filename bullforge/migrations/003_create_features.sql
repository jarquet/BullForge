CREATE TABLE IF NOT EXISTS features (
    symbol      TEXT            NOT NULL,
    timestamp   TIMESTAMPTZ     NOT NULL,
    timeframe   TEXT            NOT NULL,
    features    JSONB           NOT NULL,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, timestamp, timeframe)
);

SELECT create_hypertable('features', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_features_symbol_timeframe
    ON features (symbol, timeframe, timestamp DESC);
