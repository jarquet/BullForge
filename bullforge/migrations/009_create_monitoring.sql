CREATE TABLE IF NOT EXISTS model_status (
    model_id INTEGER PRIMARY KEY REFERENCES model_registry(id),
    status TEXT NOT NULL DEFAULT 'active',
    reason TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS halt_events (
    id SERIAL PRIMARY KEY,
    tier INTEGER NOT NULL,
    reason TEXT NOT NULL,
    direction TEXT,
    activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_halt_events_active
    ON halt_events(activated_at DESC) WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS metric_snapshots (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    category TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT '',
    "window" TEXT NOT NULL DEFAULT 'all',
    metrics JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_metric_snapshots_lookup
    ON metric_snapshots(category, scope, "window", timestamp DESC);

-- Index for fidelity tracker queries on predictions table
CREATE INDEX IF NOT EXISTS idx_predictions_model_id ON predictions(model_id);
