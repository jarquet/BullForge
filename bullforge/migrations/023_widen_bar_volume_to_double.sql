-- Widen bars.volume from BIGINT to DOUBLE PRECISION so fractional crypto
-- volumes (e.g. 0.7 BTC) survive the round-trip from Alpaca through the DB.
-- Equity volumes are integer-valued and cast losslessly.
--
-- ALTER COLUMN ... TYPE on a TimescaleDB hypertable rewrites every chunk
-- under an ACCESS EXCLUSIVE lock. The lock blocks: (a) concurrent INSERT
-- ... ON CONFLICT writes from data_pipeline.stream._process_bar, (b) any
-- chunk compression catch-up jobs, and (c) reads from the feature_engine
-- and backtest paths for the duration. Sub-minute at current data scale;
-- if applied to a much larger table in the future, schedule during a
-- quiet window.
--
-- `_run_migrations` (shared/db.py) re-executes every numbered file on every
-- live Database.connect(), so guard each ALTER on the column's current type
-- to avoid paying AccessExclusiveLock on every service restart.
DO $$
BEGIN
    IF (SELECT data_type FROM information_schema.columns
        WHERE table_name = 'bars_1m' AND column_name = 'volume') = 'bigint' THEN
        ALTER TABLE bars_1m
            ALTER COLUMN volume TYPE DOUBLE PRECISION
            USING volume::double precision;
    END IF;
END $$;

DO $$
BEGIN
    IF (SELECT data_type FROM information_schema.columns
        WHERE table_name = 'bars_daily' AND column_name = 'volume') = 'bigint' THEN
        ALTER TABLE bars_daily
            ALTER COLUMN volume TYPE DOUBLE PRECISION
            USING volume::double precision;
    END IF;
END $$;
