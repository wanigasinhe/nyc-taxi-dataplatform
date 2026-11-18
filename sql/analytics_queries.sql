-- ============================================================================
-- NYC Taxi Data Platform — Analytical Views & Parameterized Queries
-- Task 5: Core BI & Observability Metrics
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Executive Summary KPIs View
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW view_kpi_summary AS
SELECT
    COUNT(*) AS total_trips,
    COALESCE(SUM(passenger_count), 0) AS total_passengers,
    ROUND(COALESCE(SUM(total_amount), 0), 2) AS total_revenue_usd,
    ROUND(COALESCE(SUM(trip_distance), 0), 2) AS total_distance_miles,
    ROUND(COALESCE(SUM(fare_amount), 0) / NULLIF(SUM(trip_distance), 0), 2) AS weighted_avg_fare_per_mile,
    ROUND(AVG(trip_duration_sec) / 60.0, 2) AS avg_duration_minutes,
    ROUND(
        (SUM(CASE WHEN payment_type_key = 1 THEN tip_amount ELSE 0 END) /
         NULLIF(SUM(CASE WHEN payment_type_key = 1 THEN fare_amount ELSE 0 END), 0)) * 100,
        2
    ) AS credit_card_tip_pct
FROM fact_taxi_trips;

-- ----------------------------------------------------------------------------
-- 2. Average Fare Amount per Mile by Borough and Rate Code
-- Weighted formula: SUM(fare_amount) / NULLIF(SUM(trip_distance), 0)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW view_fare_per_mile_by_borough_rate AS
SELECT
    COALESCE(l.borough, 'Unknown') AS pickup_borough,
    rc.rate_description,
    COUNT(f.trip_sk) AS trip_count,
    ROUND(SUM(f.trip_distance), 2) AS total_distance,
    ROUND(SUM(f.fare_amount), 2) AS total_fare_amount,
    ROUND(
        SUM(f.fare_amount) / NULLIF(SUM(f.trip_distance), 0),
        2
    ) AS weighted_avg_fare_per_mile
FROM fact_taxi_trips f
JOIN dim_location l ON f.pu_location_key = l.location_key
JOIN dim_rate_code rc ON f.rate_code_key = rc.rate_code_key
GROUP BY l.borough, rc.rate_description
HAVING SUM(f.trip_distance) > 0
ORDER BY trip_count DESC;

-- ----------------------------------------------------------------------------
-- 3. Peak Hours for Taxi Rides (Weekday vs. Weekend)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW view_peak_hours_analysis AS
SELECT
    f.pickup_hour,
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_category,
    COUNT(*) AS total_trips,
    ROUND(AVG(f.trip_duration_sec) / 60.0, 2) AS avg_duration_min,
    ROUND(AVG(f.trip_distance), 2) AS avg_distance_miles,
    ROUND(SUM(f.total_amount), 2) AS total_revenue_usd,
    ROUND(AVG(f.total_amount), 2) AS avg_fare_usd
FROM fact_taxi_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
GROUP BY f.pickup_hour, d.is_weekend
ORDER BY f.pickup_hour, day_category;

-- ----------------------------------------------------------------------------
-- 4. Total Revenue & Tips by Payment Type
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW view_revenue_by_payment_type AS
WITH totals AS (
    SELECT SUM(total_amount) AS grand_total FROM fact_taxi_trips
)
SELECT
    p.payment_name,
    COUNT(f.trip_sk) AS trip_count,
    ROUND(SUM(f.total_amount), 2) AS total_revenue_usd,
    ROUND(SUM(f.tip_amount), 2) AS total_tips_usd,
    ROUND(
        (SUM(f.tip_amount) / NULLIF(SUM(f.fare_amount), 0)) * 100,
        2
    ) AS tip_pct_of_fare,
    ROUND(
        (SUM(f.total_amount) / NULLIF((SELECT grand_total FROM totals), 0)) * 100,
        2
    ) AS revenue_share_pct
FROM fact_taxi_trips f
JOIN dim_payment_type p ON f.payment_type_key = p.payment_type_key
GROUP BY p.payment_name
ORDER BY total_revenue_usd DESC;

-- ----------------------------------------------------------------------------
-- 5. Top Route Corridors (Pickup Zone -> Dropoff Zone)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW view_top_corridors AS
SELECT
    CONCAT(pu.zone, ' (', pu.borough, ')') AS pickup_corridor,
    CONCAT(do_loc.zone, ' (', do_loc.borough, ')') AS dropoff_corridor,
    COUNT(*) AS trip_count,
    ROUND(SUM(f.total_amount), 2) AS total_revenue_usd,
    ROUND(AVG(f.trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(f.total_amount), 2) AS avg_total_fare_usd
FROM fact_taxi_trips f
JOIN dim_location pu ON f.pu_location_key = pu.location_key
JOIN dim_location do_loc ON f.do_location_key = do_loc.location_key
GROUP BY pu.zone, pu.borough, do_loc.zone, do_loc.borough
ORDER BY trip_count DESC
LIMIT 50;

-- ----------------------------------------------------------------------------
-- 6. Pipeline Health & Telemetry Summary
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW view_pipeline_health_summary AS
SELECT
    r.run_id,
    r.source_file,
    r.status,
    r.rows_extracted,
    r.rows_cleansed,
    r.rows_fact_loaded,
    r.rows_fact_loaded AS rows_loaded,
    r.rows_master_loaded,
    r.rows_quarantined,
    ROUND(
        (r.rows_fact_loaded::NUMERIC / NULLIF(r.rows_extracted, 0)) * 100,
        2
    ) AS data_quality_pass_rate_pct,
    r.duration_sec,
    r.throughput_rows_per_sec,
    r.peak_memory_mb,
    r.started_at,
    r.completed_at
FROM etl_pipeline_runs r
ORDER BY r.started_at DESC;

-- ----------------------------------------------------------------------------
-- 7. Daily Processed Data Summary View (Day-Wise Ingestion Analytics)
-- Requirement 1: In details logs of processed data
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW view_daily_processed_summary AS
SELECT
    d.full_date AS trip_date,
    d.day_name,
    d.is_weekend,
    COUNT(f.trip_sk) AS rows_fact_loaded,
    ROUND(COALESCE(SUM(f.total_amount), 0), 2) AS total_revenue_usd,
    ROUND(COALESCE(AVG(f.fare_amount), 0), 2) AS avg_fare_usd,
    ROUND(COALESCE(AVG(f.trip_distance), 0), 2) AS avg_trip_distance_miles,
    ROUND(COALESCE(AVG(f.trip_duration_sec), 0) / 60.0, 2) AS avg_duration_minutes,
    COUNT(DISTINCT f.batch_run_id) AS distinct_batches_count
FROM fact_taxi_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
GROUP BY d.full_date, d.day_name, d.is_weekend
ORDER BY d.full_date ASC;
