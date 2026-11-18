-- ============================================================================
-- NYC Taxi Data Platform — Staged Ingestion Architecture & Data Pipeline DDL
-- 1. Download file -> 
-- 2. Extract data to a table (raw_taxi_trips) -> 
-- 3. Move valid data to next table (clean_taxi_trips / quarantine_taxi_trips) -> 
-- 4. Move to next table fact (fact_taxi_trips) -> 
-- 5. Load master table (master_taxi_trips)
-- ============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ----------------------------------------------------------------------------
-- 1. STEP 2: RAW TABLE (raw_taxi_trips)
-- Stores extracted parquet records in raw, unmodified form with batch lineage
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_taxi_trips (
    raw_id BIGSERIAL PRIMARY KEY,
    batch_run_id UUID NOT NULL,
    source_file VARCHAR(255) NOT NULL,
    vendor_id INT,
    tpep_pickup_datetime TIMESTAMPTZ,
    tpep_dropoff_datetime TIMESTAMPTZ,
    passenger_count INT,
    trip_distance NUMERIC(10, 2),
    rate_code_id INT,
    store_and_fwd_flag VARCHAR(5),
    pu_location_id INT,
    do_location_id INT,
    payment_type INT,
    fare_amount NUMERIC(10, 2),
    extra NUMERIC(10, 2),
    mta_tax NUMERIC(10, 2),
    tip_amount NUMERIC(10, 2),
    tolls_amount NUMERIC(10, 2),
    improvement_surcharge NUMERIC(10, 2),
    total_amount NUMERIC(10, 2),
    congestion_surcharge NUMERIC(10, 2),
    airport_fee NUMERIC(10, 2),
    extracted_at TIMESTAMPTZ DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- 2. STEP 3: CLEANSING TABLE (clean_taxi_trips)
-- Validated, standardized, and feature-engineered records
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean_taxi_trips (
    clean_id BIGSERIAL PRIMARY KEY,
    trip_hash_id VARCHAR(64) UNIQUE NOT NULL,      -- Deterministic SHA-256 idempotency fingerprint
    batch_run_id UUID NOT NULL,
    source_file VARCHAR(255) NOT NULL,
    vendor_key SMALLINT NOT NULL DEFAULT 0,
    pickup_datetime TIMESTAMPTZ NOT NULL,
    dropoff_datetime TIMESTAMPTZ NOT NULL,
    pickup_hour SMALLINT NOT NULL,
    pickup_date_key INT NOT NULL,
    trip_duration_sec INT NOT NULL,
    passenger_count SMALLINT NOT NULL DEFAULT 1,
    trip_distance NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    rate_code_key SMALLINT NOT NULL DEFAULT 99,
    pu_location_key INT NOT NULL DEFAULT 0,
    do_location_key INT NOT NULL DEFAULT 0,
    payment_type_key SMALLINT NOT NULL DEFAULT 0,
    fare_amount NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    extra NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    mta_tax NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    tip_amount NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    tolls_amount NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    improvement_surcharge NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    congestion_surcharge NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    total_amount NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    cleansed_at TIMESTAMPTZ DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- 3. DIMENSION TABLES (Star Schema Foundation)
-- ----------------------------------------------------------------------------

-- Dimension 1: Date (Calendar Dimension)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,              -- Format: YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_week SMALLINT NOT NULL,         -- 1 = Monday, 7 = Sunday
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Dimension 2: Taxi Zones & Boroughs (NYC TLC Zone Lookup)
CREATE TABLE IF NOT EXISTS dim_location (
    location_key INT PRIMARY KEY,          -- TLC LocationID (1 - 265, 0 = Unknown)
    borough VARCHAR(50) NOT NULL DEFAULT 'Unknown',
    zone VARCHAR(100) NOT NULL DEFAULT 'Unknown',
    service_zone VARCHAR(50) NOT NULL DEFAULT 'Unknown'
);

-- Dimension 3: Payment Types
CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_key SMALLINT PRIMARY KEY,
    payment_name VARCHAR(50) NOT NULL
);

-- Dimension 4: Rate Codes
CREATE TABLE IF NOT EXISTS dim_rate_code (
    rate_code_key SMALLINT PRIMARY KEY,
    rate_description VARCHAR(100) NOT NULL
);

-- Dimension 5: Vendors
CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_key SMALLINT PRIMARY KEY,
    vendor_name VARCHAR(100) NOT NULL
);

-- ----------------------------------------------------------------------------
-- 4. STEP 4: FACT TABLE (fact_taxi_trips)
-- Central Star Schema fact table with surrogate keys & dimensional foreign keys
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_taxi_trips (
    trip_sk BIGSERIAL PRIMARY KEY,
    trip_hash_id VARCHAR(64) UNIQUE NOT NULL,      -- Deterministic SHA-256 for idempotency

    -- Foreign Keys to Dimension Tables
    pickup_date_key INT NOT NULL REFERENCES dim_date(date_key),
    pu_location_key INT NOT NULL REFERENCES dim_location(location_key),
    do_location_key INT NOT NULL REFERENCES dim_location(location_key),
    payment_type_key SMALLINT NOT NULL REFERENCES dim_payment_type(payment_type_key),
    rate_code_key SMALLINT NOT NULL REFERENCES dim_rate_code(rate_code_key),
    vendor_key SMALLINT NOT NULL REFERENCES dim_vendor(vendor_key),

    -- Degenerate Attributes & Timestamps
    pickup_datetime TIMESTAMPTZ NOT NULL,
    dropoff_datetime TIMESTAMPTZ NOT NULL,
    pickup_hour SMALLINT NOT NULL CHECK (pickup_hour >= 0 AND pickup_hour <= 23),
    trip_duration_sec INT NOT NULL CHECK (trip_duration_sec >= 0),

    -- Quantitative Measures
    passenger_count SMALLINT NOT NULL DEFAULT 1,
    trip_distance NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    fare_amount NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    extra NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    mta_tax NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    tip_amount NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    tolls_amount NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    improvement_surcharge NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    congestion_surcharge NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    total_amount NUMERIC(10, 2) NOT NULL DEFAULT 0.00,

    -- Lineage & Audit Metadata
    ingestion_source VARCHAR(20) NOT NULL DEFAULT 'BATCH',
    batch_run_id UUID NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- 5. STEP 5: MASTER TABLE (master_taxi_trips)
-- Denormalized Gold Master Table optimized for sub-second analytical reporting
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS master_taxi_trips (
    master_id BIGSERIAL PRIMARY KEY,
    trip_hash_id VARCHAR(64) UNIQUE NOT NULL,
    batch_run_id UUID NOT NULL,

    -- Temporal Attributes
    pickup_datetime TIMESTAMPTZ NOT NULL,
    dropoff_datetime TIMESTAMPTZ NOT NULL,
    pickup_hour SMALLINT NOT NULL,
    trip_duration_sec INT NOT NULL,
    trip_duration_min NUMERIC(10, 2) NOT NULL,

    -- Denormalized Calendar Attributes
    pickup_date DATE NOT NULL,
    pickup_year SMALLINT NOT NULL,
    pickup_month SMALLINT NOT NULL,
    pickup_month_name VARCHAR(20) NOT NULL,
    pickup_day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,

    -- Denormalized Geographic Corridors
    pickup_borough VARCHAR(50) NOT NULL,
    pickup_zone VARCHAR(100) NOT NULL,
    pickup_service_zone VARCHAR(50) NOT NULL,
    dropoff_borough VARCHAR(50) NOT NULL,
    dropoff_zone VARCHAR(100) NOT NULL,
    dropoff_service_zone VARCHAR(50) NOT NULL,
    route_corridor VARCHAR(200) NOT NULL,

    -- Denormalized Categorical Descriptions
    payment_name VARCHAR(50) NOT NULL,
    rate_description VARCHAR(100) NOT NULL,
    vendor_name VARCHAR(100) NOT NULL,

    -- Quantitative Measures
    passenger_count SMALLINT NOT NULL,
    trip_distance_miles NUMERIC(10, 2) NOT NULL,
    fare_amount NUMERIC(10, 2) NOT NULL,
    extra NUMERIC(10, 2) NOT NULL,
    mta_tax NUMERIC(10, 2) NOT NULL,
    tip_amount NUMERIC(10, 2) NOT NULL,
    tolls_amount NUMERIC(10, 2) NOT NULL,
    improvement_surcharge NUMERIC(10, 2) NOT NULL,
    congestion_surcharge NUMERIC(10, 2) NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,

    -- Derived Economic KPI Measures
    fare_per_mile NUMERIC(10, 2),
    tip_percentage NUMERIC(10, 2),

    master_loaded_at TIMESTAMPTZ DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- 6. AUDIT & OBSERVABILITY TABLES
-- ----------------------------------------------------------------------------

-- Pipeline Execution Ledger
CREATE TABLE IF NOT EXISTS etl_pipeline_runs (
    run_id UUID PRIMARY KEY,
    source_file VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,           -- 'RUNNING', 'SUCCESS', 'FAILED', 'ROLLED_BACK'
    rows_extracted BIGINT DEFAULT 0,
    rows_cleansed BIGINT DEFAULT 0,
    rows_fact_loaded BIGINT DEFAULT 0,
    rows_master_loaded BIGINT DEFAULT 0,
    rows_quarantined BIGINT DEFAULT 0,
    duration_sec NUMERIC(10, 2),
    throughput_rows_per_sec NUMERIC(12, 2),
    peak_memory_mb NUMERIC(10, 2),
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- Data Quality Quarantine (Stores rejected records with reason & original JSON payload)
CREATE TABLE IF NOT EXISTS quarantine_taxi_trips (
    quarantine_id BIGSERIAL PRIMARY KEY,
    batch_run_id UUID NOT NULL,
    rejection_reason VARCHAR(255) NOT NULL,
    raw_record JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- 7. PERFORMANCE INDEXES
-- ----------------------------------------------------------------------------

-- Raw Table Indexes
CREATE INDEX IF NOT EXISTS idx_raw_batch_run_id ON raw_taxi_trips (batch_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_source_file ON raw_taxi_trips (source_file);

-- Clean Table Indexes
CREATE INDEX IF NOT EXISTS idx_clean_trip_hash ON clean_taxi_trips (trip_hash_id);
CREATE INDEX IF NOT EXISTS idx_clean_batch_run_id ON clean_taxi_trips (batch_run_id);

-- Fact Table Indexes
CREATE INDEX IF NOT EXISTS idx_fact_pickup_date_hour ON fact_taxi_trips (pickup_date_key, pickup_hour);
CREATE INDEX IF NOT EXISTS idx_fact_pu_location ON fact_taxi_trips (pu_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_do_location ON fact_taxi_trips (do_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_payment_type ON fact_taxi_trips (payment_type_key);
CREATE INDEX IF NOT EXISTS idx_fact_rate_code ON fact_taxi_trips (rate_code_key);
CREATE INDEX IF NOT EXISTS idx_fact_trip_hash_id ON fact_taxi_trips (trip_hash_id);
CREATE INDEX IF NOT EXISTS idx_fact_batch_run_id ON fact_taxi_trips (batch_run_id);

-- Master Table Indexes
CREATE INDEX IF NOT EXISTS idx_master_trip_hash ON master_taxi_trips (trip_hash_id);
CREATE INDEX IF NOT EXISTS idx_master_batch_run_id ON master_taxi_trips (batch_run_id);
CREATE INDEX IF NOT EXISTS idx_master_pickup_date ON master_taxi_trips (pickup_date);
CREATE INDEX IF NOT EXISTS idx_master_pickup_borough ON master_taxi_trips (pickup_borough);
CREATE INDEX IF NOT EXISTS idx_master_corridor ON master_taxi_trips (route_corridor);

-- Audit Indexes
CREATE INDEX IF NOT EXISTS idx_quarantine_run_id ON quarantine_taxi_trips (batch_run_id);
CREATE INDEX IF NOT EXISTS idx_etl_runs_started_at ON etl_pipeline_runs (started_at DESC);

-- ----------------------------------------------------------------------------
-- 8. STATIC DIMENSION SEED DATA
-- ----------------------------------------------------------------------------

-- Seed Payment Types
INSERT INTO dim_payment_type (payment_type_key, payment_name) VALUES
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip'),
    (0, 'Unknown / Not recorded'),
    (99, 'Other')
ON CONFLICT (payment_type_key) DO UPDATE SET payment_name = EXCLUDED.payment_name;

-- Seed Rate Codes
INSERT INTO dim_rate_code (rate_code_key, rate_description) VALUES
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride'),
    (99, 'Special / Unknown rate')
ON CONFLICT (rate_code_key) DO UPDATE SET rate_description = EXCLUDED.rate_description;

-- Seed Vendors
INSERT INTO dim_vendor (vendor_key, vendor_name) VALUES
    (1, 'Creative Mobile Technologies, LLC'),
    (2, 'VeriFone Inc.'),
    (0, 'Unknown Vendor'),
    (99, 'Other Vendor')
ON CONFLICT (vendor_key) DO UPDATE SET vendor_name = EXCLUDED.vendor_name;

-- Seed Default Unknown Location
INSERT INTO dim_location (location_key, borough, zone, service_zone) VALUES
    (0, 'Unknown', 'Unknown', 'Unknown'),
    (264, 'Unknown', 'NV', 'Unknown'),
    (265, 'Unknown', 'NA', 'Unknown')
ON CONFLICT (location_key) DO NOTHING;
