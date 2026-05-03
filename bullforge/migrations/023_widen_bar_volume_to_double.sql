-- Widen bars.volume from BIGINT to DOUBLE PRECISION so fractional crypto
-- volumes (e.g. 0.7 BTC) survive the round-trip from Alpaca through the DB.
-- Equity volumes are integer-valued and cast losslessly.
--
-- ALTER COLUMN ... TYPE on a TimescaleDB hypertable rewrites every chunk
-- under an ACCESS EXCLUSIVE lock, blocking data_pipeline writes for the
-- duration. Sub-minute at current data scale; if applied to a much larger
-- table in the future, schedule during a quiet window.
ALTER TABLE bars_1m
    ALTER COLUMN volume TYPE DOUBLE PRECISION
    USING volume::double precision;

ALTER TABLE bars_daily
    ALTER COLUMN volume TYPE DOUBLE PRECISION
    USING volume::double precision;
