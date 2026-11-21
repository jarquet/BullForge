-- 022_backtest_folds_rejected_count.sql
--
-- Adds a per-fold counter for rejected orders so the operator can spot
-- backtests where the volume cap (or missing next-bar) silently dropped
-- most signals before they could fill. Without this metric, a fold with
-- "0 trades" is ambiguous: was the strategy quiet, or was every entry
-- rejected? See docs/research/crypto-bar-volume-units.md for the crypto
-- volume-cap analysis this counter supports.
--
-- backtest_folds is a public-only table (not mirrored into the `backtest`
-- sandbox schema by 014/018), so a single ALTER on public is sufficient.

ALTER TABLE backtest_folds
    ADD COLUMN IF NOT EXISTS rejected_orders_count INTEGER NOT NULL DEFAULT 0;
