-- Add 'manual_replay' to the reconcile_source CHECK constraint.
--
-- Originally introduced in 010_order_reconciliation.sql with the set
-- {'submit','stream','poll','startup'}. The reconcile_from_alpaca CLI
-- (bullforge/scripts/reconcile_from_alpaca.py) needs a distinct source
-- so it doesn't conflate replays with the polling sweeper (which gates
-- the reconciler_orphans_ingested_total counter).
--
-- Drop-then-re-add is idempotent: DROP CONSTRAINT IF EXISTS is always
-- safe; the subsequent ADD will succeed because the prior drop
-- guarantees the named constraint is absent.

ALTER TABLE orders
  DROP CONSTRAINT IF EXISTS orders_reconcile_source_check;

ALTER TABLE orders
  ADD CONSTRAINT orders_reconcile_source_check
    CHECK (reconcile_source IN ('submit','stream','poll','startup','manual_replay'));
