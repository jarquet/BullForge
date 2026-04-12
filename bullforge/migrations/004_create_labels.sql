CREATE TABLE IF NOT EXISTS labels (
    symbol          TEXT            NOT NULL,
    timestamp       TIMESTAMPTZ     NOT NULL,
    timeframe       TEXT            NOT NULL,
    horizon_minutes INT             NOT NULL,
    forward_return  DOUBLE PRECISION NOT NULL,
    direction       SMALLINT        NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, timestamp, timeframe, horizon_minutes)
);

SELECT create_hypertable('labels', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_labels_symbol_timeframe
    ON labels (symbol, timeframe, timestamp DESC);
