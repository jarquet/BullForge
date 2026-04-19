-- 019_backtest_tags_notes.sql
--
-- Adds tags (text array) and notes (free text) to backtest_runs for
-- dashboard filtering + annotation. Adds runtime_config_audit for tracking
-- which backtest runs' configs have been promoted to live runtime_config.

ALTER TABLE backtest_runs
    ADD COLUMN IF NOT EXISTS tags TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS notes TEXT;

CREATE INDEX IF NOT EXISTS idx_backtest_runs_tags ON backtest_runs USING GIN (tags);

CREATE TABLE IF NOT EXISTS runtime_config_audit (
    id                     BIGSERIAL PRIMARY KEY,
    key                    TEXT        NOT NULL,
    old_value              TEXT,
    new_value              TEXT        NOT NULL,
    changed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_backtest_run_id BIGINT      REFERENCES backtest_runs(id) ON DELETE SET NULL,
    note                   TEXT
);

CREATE INDEX IF NOT EXISTS idx_runtime_config_audit_key
    ON runtime_config_audit (key, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_runtime_config_audit_source
    ON runtime_config_audit (source_backtest_run_id)
    WHERE source_backtest_run_id IS NOT NULL;
