CREATE TABLE IF NOT EXISTS backtest_runs (
    id            BIGSERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at   TIMESTAMPTZ,
    symbols       TEXT[] NOT NULL,
    timeframe     TEXT NOT NULL,
    horizons      INT[]  NOT NULL,
    range_start   TIMESTAMPTZ NOT NULL,
    range_end     TIMESTAMPTZ NOT NULL,
    train_days    INT  NOT NULL,
    test_days     INT  NOT NULL,
    embargo_min   INT  NOT NULL,
    mode          TEXT NOT NULL CHECK (mode IN ('rolling','anchored')),
    config_json   JSONB NOT NULL,
    status        TEXT NOT NULL DEFAULT 'running'
                  CHECK (status IN ('running','completed','failed'))
);

CREATE TABLE IF NOT EXISTS backtest_folds (
    id            BIGSERIAL PRIMARY KEY,
    run_id        BIGINT NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
    fold_index    INT NOT NULL,
    train_start   TIMESTAMPTZ NOT NULL,
    train_end     TIMESTAMPTZ NOT NULL,
    test_start    TIMESTAMPTZ NOT NULL,
    test_end      TIMESTAMPTZ NOT NULL,
    metrics_json  JSONB,
    UNIQUE (run_id, fold_index)
);

CREATE TABLE IF NOT EXISTS backtest_trades (
    id            BIGSERIAL PRIMARY KEY,
    run_id        BIGINT NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
    fold_id       BIGINT NOT NULL REFERENCES backtest_folds(id) ON DELETE CASCADE,
    symbol        TEXT NOT NULL,
    side          TEXT NOT NULL CHECK (side IN ('long','short')),
    entry_time    TIMESTAMPTZ NOT NULL,
    entry_price   DOUBLE PRECISION NOT NULL,
    exit_time     TIMESTAMPTZ,
    exit_price    DOUBLE PRECISION,
    quantity      DOUBLE PRECISION NOT NULL,
    pnl           DOUBLE PRECISION,
    signal_strength DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_backtest_trades_run ON backtest_trades(run_id);
CREATE INDEX IF NOT EXISTS idx_backtest_trades_fold ON backtest_trades(fold_id);

CREATE TABLE IF NOT EXISTS backtest_equity (
    run_id     BIGINT NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
    fold_id    BIGINT NOT NULL REFERENCES backtest_folds(id) ON DELETE CASCADE,
    timestamp  TIMESTAMPTZ NOT NULL,
    equity     DOUBLE PRECISION NOT NULL,
    cash       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (run_id, fold_id, timestamp)
);
