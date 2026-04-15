DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'orders_status_check'
      AND conrelid = 'orders'::regclass
  ) THEN
    ALTER TABLE orders
      ADD CONSTRAINT orders_status_check
      CHECK (status IN (
        'pending','submitted','filled','partially_filled',
        'cancelled','rejected','expired'
      ));
  END IF;
END
$$;
