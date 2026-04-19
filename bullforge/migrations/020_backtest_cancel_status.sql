-- 020_backtest_cancel_status.sql
--
-- Extends the status CHECK constraint on backtest_jobs and backtest_runs
-- to include 'cancelled'. backtest_jobs rows flip to 'cancelled' when a
-- user cancels via the dashboard/API/CLI; backtest_runs rows flip to
-- 'cancelled' when the engine catches a BacktestCancelled mid-run and
-- returns a distinct status so partial results can be rendered
-- differently from 'completed' or 'failed'.
--
-- Existing data is unchanged. Constraint names follow Postgres's default
-- <table>_<col>_check convention; both already exist as inline CHECKs in
-- migrations 013 (jobs) and 012 (runs).

ALTER TABLE backtest_jobs DROP CONSTRAINT IF EXISTS backtest_jobs_status_check;
ALTER TABLE backtest_jobs ADD CONSTRAINT backtest_jobs_status_check
    CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled'));

ALTER TABLE backtest_runs DROP CONSTRAINT IF EXISTS backtest_runs_status_check;
ALTER TABLE backtest_runs ADD CONSTRAINT backtest_runs_status_check
    CHECK (status IN ('running', 'completed', 'failed', 'cancelled'));
