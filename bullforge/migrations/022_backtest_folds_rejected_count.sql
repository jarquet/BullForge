-- 022_backtest_folds_rejected_count.sql
--
-- Adds a per-fold counter for rejected orders so the operator can spot
-- backtests where the volume cap (or missing next-bar) silently dropped
-- most signals before they could fill. Without this metric, a fold with
-- "0 trades" is ambiguous: was the strategy quiet, or was every entry
-- rejected? See TODO.md "Backtest fill-model volume cap — crypto tuning".
--
-- backtest_folds is a public-only table (not mirrored into the `backtest`
-- sandbox schema by 014/018), so a single ALTER on public is sufficient.

ALTER TABLE backtest_folds
    ADD COLUMN IF NOT EXISTS rejected_orders_count INTEGER NOT NULL DEFAULT 0;
