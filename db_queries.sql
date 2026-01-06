/* =========================================================
   STEP 0 — DATABASE & USER (run as postgres superuser)
   ========================================================= */

CREATE DATABASE gas_data;

CREATE USER gas_user WITH PASSWORD 'YOUR_PASSWORD_HERE';

GRANT ALL PRIVILEGES ON DATABASE gas_data TO gas_user;


/* =========================================================
   STEP 1 — CONNECT TO DATABASE
   ========================================================= */

\c gas_data;


/* =========================================================
   STEP 2 — SCHEMA OWNERSHIP & PERMISSIONS
   ========================================================= */

ALTER SCHEMA public OWNER TO gas_user;

GRANT ALL ON SCHEMA public TO gas_user;
GRANT ALL ON ALL TABLES IN SCHEMA public TO gas_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO gas_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON TABLES TO gas_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON SEQUENCES TO gas_user;


/* =========================================================
   STEP 3 — META SERIES TABLE (CORE REGISTRY)
   ========================================================= */

CREATE TABLE IF NOT EXISTS meta_series (
    series_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    source_type TEXT DEFAULT 'NATIONAL_GAS',
    dataset_id TEXT,
    data_item TEXT,
    description TEXT,
    unit TEXT NOT NULL,
    frequency TEXT NOT NULL,
    timezone_source TEXT NOT NULL,
    lookback_days INTEGER DEFAULT 7,
    is_active BOOLEAN DEFAULT TRUE,
    last_ingested_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);


/* =========================================================
   STEP 4 — DATA OBSERVATIONS (NORMALIZED TIME SERIES)
   ========================================================= */

CREATE TABLE IF NOT EXISTS data_observations (
    series_id TEXT NOT NULL REFERENCES meta_series(series_id),
    observation_time TIMESTAMP NOT NULL,
    ingestion_time TIMESTAMP DEFAULT NOW(),
    value DOUBLE PRECISION NOT NULL,
    quality_flag TEXT DEFAULT 'ACTUAL',
    raw_payload JSONB,

    PRIMARY KEY (series_id, observation_time)
);

CREATE INDEX IF NOT EXISTS idx_data_obs_series_time
ON data_observations(series_id, observation_time);

CREATE INDEX IF NOT EXISTS idx_data_obs_raw
ON data_observations USING GIN (raw_payload);


/* =========================================================
   STEP 5 — RAW EVENTS (PHASE-2 ZERO-LOSS INGESTION)
   ========================================================= */

CREATE TABLE IF NOT EXISTS raw_events (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    series_hint TEXT,
    event_time TIMESTAMP NULL,
    raw_payload JSONB NOT NULL,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_dataset
ON raw_events(dataset_id);

CREATE INDEX IF NOT EXISTS idx_raw_events_payload
ON raw_events USING GIN (raw_payload);


/* =========================================================
   STEP 6 — OPTIONAL SEED DATA (ONLY FOR DEMO / TEST)
   ========================================================= */

INSERT INTO meta_series (
    series_id,
    source,
    dataset_id,
    data_item,
    description,
    unit,
    frequency,
    timezone_source,
    lookback_days,
    is_active
)
VALUES
(
    'UK_NBP_DEMAND',
    'NATIONAL_GAS',
    'PUBOB637',
    'Demand Actual, NTS, D+1',
    'Demand Actual, NTS, D+1',
    'mcm',
    'daily',
    'Europe/London',
    7,
    TRUE
)
ON CONFLICT (series_id) DO NOTHING;


/* =========================================================
   STEP 7 — VERIFICATION QUERIES
   ========================================================= */

-- Verify tables
\dt

-- Verify schema
\d meta_series
\d data_observations
\d raw_events

-- Verify ingestion
SELECT dataset_id, COUNT(*) FROM raw_events GROUP BY dataset_id;

SELECT series_id, COUNT(*) FROM data_observations GROUP BY series_id;


/* =========================================================
   STEP 8 — CREATE FIELD_CATALOG TABLE 
   ========================================================= */

CREATE TABLE IF NOT EXISTS field_catalog (
    dataset_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    inferred_type TEXT NOT NULL,
    nullable BOOLEAN NOT NULL,
    example_value TEXT,
    first_seen_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (dataset_id, field_name)
);
