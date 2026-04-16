-- bullforge/migrations/015_backtest_custom_name.sql
-- Adds a queryable column for the user-supplied portion of a backtest name.
-- The full `name` column remains the display/composed name; `custom_name`
-- isolates the user's custom tag (or an 8-char random token) for easy search.
ALTER TABLE backtest_jobs
    ADD COLUMN IF NOT EXISTS custom_name TEXT;

ALTER TABLE backtest_runs
    ADD COLUMN IF NOT EXISTS custom_name TEXT;

CREATE INDEX IF NOT EXISTS idx_backtest_runs_custom_name
    ON backtest_runs (custom_name);
CREATE INDEX IF NOT EXISTS idx_backtest_jobs_custom_name
    ON backtest_jobs (custom_name);
