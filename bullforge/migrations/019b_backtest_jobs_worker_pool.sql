-- 019b_backtest_jobs_worker_pool.sql
--
-- Adds a `worker_pool` column to backtest_jobs so CPU-native jobs go to
-- backtest-worker-cpu containers and GPU-needing jobs (LSTM/MLP/meta)
-- go to backtest-worker-gpu containers. Existing rows backfill to 'cpu'
-- via the column default, matching today's behavior.
ALTER TABLE backtest_jobs
    ADD COLUMN IF NOT EXISTS worker_pool TEXT NOT NULL DEFAULT 'cpu'
        CHECK (worker_pool IN ('cpu', 'gpu'));

CREATE INDEX IF NOT EXISTS idx_backtest_jobs_pool_status_created
    ON backtest_jobs (worker_pool, status, created_at);
