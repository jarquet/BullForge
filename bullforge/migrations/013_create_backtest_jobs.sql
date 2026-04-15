-- bullforge/migrations/013_create_backtest_jobs.sql
CREATE TABLE IF NOT EXISTS backtest_jobs (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    config_json  JSONB NOT NULL,
    status       TEXT NOT NULL DEFAULT 'queued'
                 CHECK (status IN ('queued','running','completed','failed')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    run_id       BIGINT REFERENCES backtest_runs(id) ON DELETE SET NULL,
    error        TEXT
);
CREATE INDEX IF NOT EXISTS idx_backtest_jobs_status_created
    ON backtest_jobs (status, created_at);
