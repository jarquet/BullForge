CREATE TABLE IF NOT EXISTS bars_1m (
    symbol      TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      BIGINT      NOT NULL,
    PRIMARY KEY (symbol, timestamp)
);

SELECT create_hypertable('bars_1m', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS bars_daily (
    symbol      TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      BIGINT      NOT NULL,
    PRIMARY KEY (symbol, timestamp)
);

SELECT create_hypertable('bars_daily', 'timestamp', if_not_exists => TRUE);
