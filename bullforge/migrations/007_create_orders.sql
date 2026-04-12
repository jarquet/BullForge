CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    order_type TEXT NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL,
    signal_strength DOUBLE PRECISION NOT NULL,
    alpaca_order_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    filled_at TIMESTAMPTZ,
    filled_price DOUBLE PRECISION,
    filled_quantity DOUBLE PRECISION,
    limit_price DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_symbol_submitted
    ON orders (symbol, submitted_at DESC);

CREATE INDEX IF NOT EXISTS idx_orders_status
    ON orders (status);

CREATE INDEX IF NOT EXISTS idx_orders_alpaca_id
    ON orders (alpaca_order_id)
    WHERE alpaca_order_id IS NOT NULL;
