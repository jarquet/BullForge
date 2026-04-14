ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'internal'
    CHECK (origin IN ('internal', 'external')),
  ADD COLUMN IF NOT EXISTS last_reconciled_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reconcile_source TEXT
    CHECK (reconcile_source IN ('submit','stream','poll','startup'));

DROP INDEX IF EXISTS idx_orders_alpaca_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_alpaca_id
  ON orders (alpaca_order_id)
  WHERE alpaca_order_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_orders_non_terminal
  ON orders (submitted_at DESC)
  WHERE status NOT IN ('filled','cancelled','rejected','expired');
