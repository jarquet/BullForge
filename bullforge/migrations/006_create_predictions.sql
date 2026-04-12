CREATE TABLE IF NOT EXISTS predictions (
    symbol              TEXT                NOT NULL,
    timestamp           TIMESTAMPTZ         NOT NULL,
    timeframe           TEXT                NOT NULL,
    horizon_minutes     INTEGER             NOT NULL,
    model_id            INTEGER             NOT NULL,
    predicted_direction SMALLINT            NOT NULL,
    confidence          DOUBLE PRECISION    NOT NULL,
    created_at          TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, timestamp, timeframe, horizon_minutes)
);

SELECT create_hypertable('predictions', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_predictions_symbol_timeframe
    ON predictions (symbol, timeframe, timestamp DESC);
