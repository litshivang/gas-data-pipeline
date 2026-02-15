/* =========================================================
   STEP 0 — DATABASE & USER
   ========================================================= */

CREATE DATABASE gas_data;

CREATE USER gas_user WITH PASSWORD 'gas_password';

GRANT ALL PRIVILEGES ON DATABASE gas_data TO gas_user;


/* =========================================================
   STEP 1 — CONNECT
   ========================================================= */

\c gas_data;


/* =========================================================
   STEP 2 — EXTENSIONS
   ========================================================= */

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


/* =========================================================
   STEP 3 — SCHEMA OWNERSHIP
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
   STEP 4 — META SERIES
   ========================================================= */

CREATE TABLE IF NOT EXISTS meta_series (
    series_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    source_type TEXT DEFAULT 'NATIONAL_GAS',
    dataset_id TEXT NOT NULL,
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
   STEP 5 — DATA OBSERVATIONS
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
   STEP 6 — RAW EVENTS (ZERO-LOSS)
   ========================================================= */

CREATE TABLE IF NOT EXISTS raw_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
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
   STEP 7 — FIELD CATALOG (DISCOVERY)
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


ALTER TABLE raw_events
ADD COLUMN source TEXT NOT NULL DEFAULT 'NATIONAL_GAS';

ALTER TABLE raw_events
ADD COLUMN series_hint TEXT;

ALTER TABLE raw_events
ADD COLUMN event_time TIMESTAMP;


GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO gas_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO gas_user;


ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON TABLES TO gas_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON SEQUENCES TO gas_user;


UPDATE meta_series
SET last_ingested_at = NOW()
WHERE series_id = :series_id;


-- GIE API:

CREATE SCHEMA meta;
CREATE SCHEMA energy;

CREATE TABLE meta.assets (
    asset_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT,
    subtype TEXT,
    direction TEXT,
    level TEXT,
    quality TEXT
);


CREATE TABLE meta.series (
    series_id SERIAL PRIMARY KEY,
    series_name TEXT,
    asset_id INTEGER NOT NULL REFERENCES meta.assets(asset_id),
    series_unique_concat TEXT UNIQUE,
    variable TEXT,
    status TEXT,
    status_blend TEXT,
    source TEXT,
    source_blend TEXT,
    do_filter BOOLEAN,
    do_estimate BOOLEAN NOT NULL DEFAULT FALSE,
    exclude_noms BOOLEAN NOT NULL DEFAULT FALSE
);


CREATE TABLE energy.daily (
    value_date DATE NOT NULL,
    value NUMERIC NOT NULL,
    series_id INTEGER NOT NULL REFERENCES meta.series(series_id),
    asset_id INTEGER NOT NULL REFERENCES meta.assets(asset_id),
    PRIMARY KEY (value_date, series_id)
);
