CREATE TABLE IF NOT EXISTS calendar_events (
    id          SERIAL PRIMARY KEY,
    event_type  TEXT        NOT NULL,
    symbol      TEXT,
    event_date  TIMESTAMPTZ NOT NULL,
    description TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_type, symbol, event_date)
);

CREATE INDEX IF NOT EXISTS idx_calendar_events_date
    ON calendar_events (event_date);

CREATE INDEX IF NOT EXISTS idx_calendar_events_symbol
    ON calendar_events (symbol, event_date);
