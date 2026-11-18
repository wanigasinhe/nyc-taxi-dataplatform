-- ============================================================================
-- NYC Taxi Data Platform — Task 5: SQL & Dashboarding Analytics Queries
-- ============================================================================
-- Description:
-- Production SQL queries to answer core business analytics questions:
--   1. Average Fare Amount per Mile (Overall, by Borough, and by Rate Code)
--   2. Peak Hours for Taxi Rides (24-Hour Distribution, Weekday vs. Weekend)
--   3. Total Revenue by Payment Type (Revenue Share %, Tip Rates, Volume)
-- ============================================================================

-- ============================================================================
-- 1. AVERAGE FARE AMOUNT PER MILE
-- ============================================================================
-- Formulation:
-- Using weighted average: SUM(fare_amount) / NULLIF(SUM(trip_distance), 0).
-- Note on Weighted vs. Arithmetic Average:
-- Naive arithmetic mean (AVG(fare_amount / trip_distance)) suffers from extreme
-- upward distortion from ultra-short trips (e.g. 0.1 mile ride with base fare $5.00
-- evaluates to $50/mile, artificially inflating the average).
-- The weighted average accurately represents total fare dollars collected per
-- aggregate passenger mile traveled across NYC.

-- 1.1 Overall NYC Average Fare per Mile
SELECT
    COUNT(*) AS total_trips,
    ROUND(SUM(trip_distance), 2) AS total_distance_miles,
    ROUND(SUM(fare_amount), 2) AS total_fare_amount_usd,
    ROUND(SUM(fare_amount) / NULLIF(SUM(trip_distance), 0), 2) AS weighted_avg_fare_per_mile,
    ROUND(AVG(fare_amount / NULLIF(trip_distance, 0)), 2) AS unweighted_arithmetic_avg_fare_per_mile
FROM fact_taxi_trips
WHERE trip_distance > 0 AND fare_amount > 0;

-- 1.2 Average Fare Amount per Mile by Pickup Borough and Rate Code
SELECT
    COALESCE(l.borough, 'Unknown') AS pickup_borough,
    rc.rate_description,
    COUNT(f.trip_sk) AS trip_count,
    ROUND(SUM(f.trip_distance), 2) AS total_distance_miles,
    ROUND(SUM(f.fare_amount), 2) AS total_fare_amount_usd,
    ROUND(SUM(f.fare_amount) / NULLIF(SUM(f.trip_distance), 0), 2) AS weighted_avg_fare_per_mile
FROM fact_taxi_trips f
JOIN dim_location l ON f.pu_location_key = l.location_key
JOIN dim_rate_code rc ON f.rate_code_key = rc.rate_code_key
WHERE f.trip_distance > 0 AND f.fare_amount > 0
GROUP BY l.borough, rc.rate_description
ORDER BY trip_count DESC;


-- ============================================================================
-- 2. PEAK HOURS FOR TAXI RIDES
-- ============================================================================
-- Analyzes diurnal taxi demand across the 24-hour cycle, segmenting weekday
-- business commuting patterns from weekend nightlife and recreational trips.

-- 2.1 Overall Peak Hours Ranked by Ride Demand
SELECT
    f.pickup_hour,
    COUNT(*) AS total_trips,
    ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM fact_taxi_trips) * 100, 2) AS pct_of_daily_trips,
    ROUND(AVG(f.trip_duration_sec) / 60.0, 2) AS avg_duration_minutes,
    ROUND(AVG(f.trip_distance), 2) AS avg_distance_miles,
    ROUND(SUM(f.total_amount), 2) AS total_revenue_usd,
    ROUND(AVG(f.total_amount), 2) AS avg_fare_usd
FROM fact_taxi_trips f
GROUP BY f.pickup_hour
ORDER BY total_trips DESC;

-- 2.2 Peak Hours: Weekday vs. Weekend Comparison (Hourly Curve)
SELECT
    f.pickup_hour,
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_category,
    COUNT(*) AS total_trips,
    ROUND(AVG(f.trip_duration_sec) / 60.0, 2) AS avg_duration_minutes,
    ROUND(AVG(f.trip_distance), 2) AS avg_distance_miles,
    ROUND(SUM(f.total_amount), 2) AS total_revenue_usd,
    ROUND(AVG(f.total_amount), 2) AS avg_fare_usd
FROM fact_taxi_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
GROUP BY f.pickup_hour, d.is_weekend
ORDER BY f.pickup_hour, day_category;


-- ============================================================================
-- 3. TOTAL REVENUE BY PAYMENT TYPE
-- ============================================================================
-- Analyzes payment preference distribution, electronic card penetration,
-- tip generation, and overall revenue contribution.

-- 3.1 Total Revenue, Tips, and Market Share by Payment Type
WITH totals AS (
    SELECT
        SUM(total_amount) AS grand_total_revenue,
        COUNT(*) AS grand_total_trips
    FROM fact_taxi_trips
)
SELECT
    p.payment_name,
    COUNT(f.trip_sk) AS trip_count,
    ROUND((COUNT(f.trip_sk)::NUMERIC / NULLIF((SELECT grand_total_trips FROM totals), 0)) * 100, 2) AS trip_share_pct,
    ROUND(SUM(f.total_amount), 2) AS total_revenue_usd,
    ROUND((SUM(f.total_amount) / NULLIF((SELECT grand_total_revenue FROM totals), 0)) * 100, 2) AS revenue_share_pct,
    ROUND(SUM(f.tip_amount), 2) AS total_tips_usd,
    ROUND(AVG(f.total_amount), 2) AS avg_fare_per_trip_usd,
    ROUND((SUM(f.tip_amount) / NULLIF(SUM(f.fare_amount), 0)) * 100, 2) AS tip_pct_of_fare
FROM fact_taxi_trips f
JOIN dim_payment_type p ON f.payment_type_key = p.payment_type_key
GROUP BY p.payment_name
ORDER BY total_revenue_usd DESC;


-- ============================================================================
-- 4. REUSABLE ANALYTICAL VIEWS FOR BI & STREAMLIT DASHBOARD
-- ============================================================================

CREATE OR REPLACE VIEW view_fare_per_mile_by_borough_rate AS
SELECT
    COALESCE(l.borough, 'Unknown') AS pickup_borough,
    rc.rate_description,
    COUNT(f.trip_sk) AS trip_count,
    ROUND(SUM(f.trip_distance), 2) AS total_distance,
    ROUND(SUM(f.fare_amount), 2) AS total_fare_amount,
    ROUND(SUM(f.fare_amount) / NULLIF(SUM(f.trip_distance), 0), 2) AS weighted_avg_fare_per_mile
FROM fact_taxi_trips f
JOIN dim_location l ON f.pu_location_key = l.location_key
JOIN dim_rate_code rc ON f.rate_code_key = rc.rate_code_key
GROUP BY l.borough, rc.rate_description
HAVING SUM(f.trip_distance) > 0
ORDER BY trip_count DESC;

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

CREATE OR REPLACE VIEW view_revenue_by_payment_type AS
WITH totals AS (
    SELECT SUM(total_amount) AS grand_total FROM fact_taxi_trips
)
SELECT
    p.payment_name,
    COUNT(f.trip_sk) AS trip_count,
    ROUND(SUM(f.total_amount), 2) AS total_revenue_usd,
    ROUND(SUM(f.tip_amount), 2) AS total_tips_usd,
    ROUND((SUM(f.tip_amount) / NULLIF(SUM(f.fare_amount), 0)) * 100, 2) AS tip_pct_of_fare,
    ROUND((SUM(f.total_amount) / NULLIF((SELECT grand_total FROM totals), 0)) * 100, 2) AS revenue_share_pct
FROM fact_taxi_trips f
JOIN dim_payment_type p ON f.payment_type_key = p.payment_type_key
GROUP BY p.payment_name
ORDER BY total_revenue_usd DESC;

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
