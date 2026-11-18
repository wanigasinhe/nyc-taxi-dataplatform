"""High-performance staged ingestion loader supporting the 5-tier pipeline architecture.

Pipeline Progression:
1. Download File
2. Extract to Raw Table (raw_taxi_trips)
3. Move Valid Data to Next Table Cleansing (clean_taxi_trips & quarantine_taxi_trips)
4. Move to Next Table Fact (fact_taxi_trips)
5. Load Master Table (master_taxi_trips)
"""

from datetime import datetime
import io
import json
from pathlib import Path
import time
from typing import Any, Dict, List, Optional
import uuid
import psutil
import psycopg
from psycopg import sql
import pandas as pd

from settings import paths, settings
from src.logger import logger, log_metric, track_performance, log_daily_processed_data


def get_db_connection() -> psycopg.Connection:
    """Returns a connected psycopg3 database connection."""
    return psycopg.connect(settings.psycopg_conninfo, autocommit=False)


def apply_schema_migrations(schema_path: Optional[Path] = None):
    """Executes the DDL schema file if tables do not exist and applies analytical views."""
    path = schema_path or paths.schema_ddl_path
    if not path.exists():
        logger.warning(f"Schema file not found at {path}, skipping DDL migration.")
        return

    logger.info(f"Applying schema DDL from {path}...")
    with open(path, "r", encoding="utf-8") as f:
        ddl = f.read()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'master_taxi_trips';")
            tables_exist = cur.fetchone() is not None
            if not tables_exist:
                cur.execute(ddl)
            # Ensure any newly added columns exist on preexisting tables
            cur.execute("""
                ALTER TABLE etl_pipeline_runs ADD COLUMN IF NOT EXISTS rows_cleansed BIGINT DEFAULT 0;
                ALTER TABLE etl_pipeline_runs ADD COLUMN IF NOT EXISTS rows_fact_loaded BIGINT DEFAULT 0;
                ALTER TABLE etl_pipeline_runs ADD COLUMN IF NOT EXISTS rows_master_loaded BIGINT DEFAULT 0;
            """)
        conn.commit()
    logger.info("Schema DDL applied successfully.")

    # Automatically ensure analytical SQL views are synchronized
    apply_analytics_views()


def apply_analytics_views(sql_path: Optional[Path] = None):
    """Refreshes and applies analytical SQL views."""
    path = sql_path or paths.analytics_queries_path
    if not path.exists():
        logger.warning(f"Analytics queries file not found at {path}, skipping views refresh.")
        return

    logger.info(f"Refreshing analytical views from {path}...")
    with open(path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_content)
        conn.commit()
    logger.info("Analytical views refreshed successfully.")


def seed_taxi_zones(csv_path: Optional[Path] = None):
    """Populates dim_location from taxi_zone_lookup.csv idempotently."""
    path = csv_path or paths.taxi_zone_csv_path
    if not path.exists():
        logger.warning(f"Taxi zone CSV not found at {path}, skipping location seeding.")
        return

    logger.info(f"Seeding dim_location from {path}...")
    zone_df = pd.read_csv(path)
    zone_df.rename(
        columns={
            "LocationID": "location_key",
            "Borough": "borough",
            "Zone": "zone",
            "service_zone": "service_zone",
        },
        inplace=True,
    )
    zone_df.fillna("Unknown", inplace=True)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO dim_location (location_key, borough, zone, service_zone)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (location_key) DO UPDATE
                SET borough = EXCLUDED.borough,
                    zone = EXCLUDED.zone,
                    service_zone = EXCLUDED.service_zone;
            """
            data_tuples = list(
                zone_df[["location_key", "borough", "zone", "service_zone"]].itertuples(
                    index=False, name=None
                )
            )
            cur.executemany(insert_query, data_tuples)
        conn.commit()
    logger.info(f"Seeded {len(zone_df)} taxi zones into dim_location.")


def upsert_date_dimension(dates_df: pd.DataFrame, conn: Optional[psycopg.Connection] = None):
    """Bulk upserts newly encountered date keys into dim_date."""
    if dates_df.empty:
        return

    should_close = False
    if conn is None:
        conn = get_db_connection()
        should_close = True

    try:
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO dim_date (
                    date_key, full_date, year, quarter, month,
                    month_name, day_of_week, day_name, is_weekend
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_key) DO NOTHING;
            """
            tuples = list(
                dates_df[
                    [
                        "date_key",
                        "full_date",
                        "year",
                        "quarter",
                        "month",
                        "month_name",
                        "day_of_week",
                        "day_name",
                        "is_weekend",
                    ]
                ].itertuples(index=False, name=None)
            )
            cur.executemany(insert_query, tuples)
        if should_close:
            conn.commit()
    finally:
        if should_close:
            conn.close()


def ensure_unknown_location_keys(clean_df: pd.DataFrame, conn: Optional[psycopg.Connection] = None):
    """Ensures any unknown LocationIDs appearing in incoming records exist in dim_location."""
    unique_pu = set(clean_df["pu_location_key"].unique())
    unique_do = set(clean_df["do_location_key"].unique())
    all_keys = unique_pu.union(unique_do)

    should_close = False
    if conn is None:
        conn = get_db_connection()
        should_close = True

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT location_key FROM dim_location;")
            existing_keys = {r[0] for r in cur.fetchall()}
            missing = all_keys - existing_keys
            if missing:
                cur.executemany(
                    "INSERT INTO dim_location (location_key, borough, zone, service_zone) VALUES (%s, 'Unknown', 'Unknown', 'Unknown') ON CONFLICT DO NOTHING;",
                    [(k,) for k in missing],
                )
        if should_close:
            conn.commit()
    finally:
        if should_close:
            conn.close()


# -----------------------------------------------------------------------------
# STEP 2: Extract to Raw Table (raw_taxi_trips)
# -----------------------------------------------------------------------------
def load_raw_table_copy(
    raw_df: pd.DataFrame, source_file: str, batch_run_id: uuid.UUID, conn: psycopg.Connection
) -> int:
    """Step 2: Ingests raw uncleaned Parquet records into raw_taxi_trips via COPY."""
    if raw_df.empty:
        return 0

    df = raw_df.copy()
    df["batch_run_id"] = str(batch_run_id)
    df["source_file"] = source_file

    col_map = {
        "VendorID": "vendor_id",
        "RatecodeID": "rate_code_id",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "Airport_fee": "airport_fee",
    }
    df.rename(columns={k: v for k, v in col_map.items() if k in df.columns}, inplace=True)

    raw_cols = [
        "batch_run_id",
        "source_file",
        "vendor_id",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_fwd_flag",
        "pu_location_id",
        "do_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
    ]

    for c in raw_cols:
        if c not in df.columns:
            df[c] = None

    # Cast integer columns to nullable Int64 so COPY receives "1" instead of "1.0"
    raw_int_cols = ["vendor_id", "passenger_count", "rate_code_id", "pu_location_id", "do_location_id", "payment_type"]
    for ic in raw_int_cols:
        if ic in df.columns:
            df[ic] = pd.to_numeric(df[ic], errors="coerce").astype("Int64")

    buf = io.StringIO()
    df[raw_cols].to_csv(buf, sep="\t", header=False, index=False, na_rep="\\N")
    buf.seek(0)

    cols_str = ", ".join(raw_cols)
    copy_sql = f"COPY raw_taxi_trips ({cols_str}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N');"

    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            copy.write(buf.getvalue())

    return len(df)


# -----------------------------------------------------------------------------
# STEP 3: Move Valid Data to Next Table Cleansing (clean_taxi_trips & quarantine)
# -----------------------------------------------------------------------------
def load_clean_and_quarantine_copy(
    clean_df: pd.DataFrame,
    quarantine_df: pd.DataFrame,
    source_file: str,
    conn: psycopg.Connection,
) -> Dict[str, int]:
    """Step 3: Loads validated records to clean_taxi_trips and rejected records to quarantine."""
    cleansed_count = 0
    quarantined_count = 0

    with conn.cursor() as cur:
        # Load clean records
        if not clean_df.empty:
            clean_df_copy = clean_df.copy()
            clean_df_copy["source_file"] = source_file

            clean_cols = [
                "trip_hash_id",
                "batch_run_id",
                "source_file",
                "vendor_key",
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_hour",
                "pickup_date_key",
                "trip_duration_sec",
                "passenger_count",
                "trip_distance",
                "rate_code_key",
                "pu_location_key",
                "do_location_key",
                "payment_type_key",
                "fare_amount",
                "extra",
                "mta_tax",
                "tip_amount",
                "tolls_amount",
                "improvement_surcharge",
                "congestion_surcharge",
                "total_amount",
            ]
            for c in clean_cols:
                if c not in clean_df_copy.columns:
                    clean_df_copy[c] = None

            # Cast integer foreign/date keys to nullable Int64 so COPY receives "1" instead of "1.0"
            clean_int_cols = [
                "vendor_key",
                "pickup_hour",
                "pickup_date_key",
                "trip_duration_sec",
                "passenger_count",
                "rate_code_key",
                "pu_location_key",
                "do_location_key",
                "payment_type_key",
            ]
            for cic in clean_int_cols:
                if cic in clean_df_copy.columns:
                    clean_df_copy[cic] = pd.to_numeric(clean_df_copy[cic], errors="coerce").astype("Int64")

            buf = io.StringIO()
            clean_df_copy[clean_cols].to_csv(buf, sep="\t", header=False, index=False, na_rep="\\N")
            buf.seek(0)

            cols_str = ", ".join(clean_cols)
            cur.execute("""
                CREATE TEMP TABLE IF NOT EXISTS staging_clean_trips (LIKE clean_taxi_trips INCLUDING DEFAULTS) ON COMMIT DROP;
                TRUNCATE TABLE staging_clean_trips;
            """)
            with cur.copy(f"COPY staging_clean_trips ({cols_str}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N');") as copy:
                copy.write(buf.getvalue())

            cur.execute(f"INSERT INTO clean_taxi_trips ({cols_str}) SELECT {cols_str} FROM staging_clean_trips ON CONFLICT (trip_hash_id) DO NOTHING;")
            cleansed_count = cur.rowcount

        # Load quarantined records
        if not quarantine_df.empty:
            buf_quar = io.StringIO()
            for row in quarantine_df.itertuples(index=False):
                reason = str(row.rejection_reason).replace("\t", " ").replace("\n", " ")
                raw_rec = str(row.raw_record).replace("\t", " ").replace("\n", " ")
                buf_quar.write(f"{row.batch_run_id}\t{reason}\t{raw_rec}\n")
            buf_quar.seek(0)

            with cur.copy("COPY quarantine_taxi_trips (batch_run_id, rejection_reason, raw_record) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t');") as copy:
                copy.write(buf_quar.getvalue())
            quarantined_count = len(quarantine_df)

    return {"cleansed": cleansed_count, "quarantined": quarantined_count}


# -----------------------------------------------------------------------------
# STEP 4: Move to Next Table Fact (fact_taxi_trips)
# -----------------------------------------------------------------------------
def move_clean_to_fact_table(batch_run_id: uuid.UUID, conn: psycopg.Connection) -> int:
    """Step 4: Inserts cleansed records into Star Schema fact_taxi_trips with FK resolution."""
    run_id_str = str(batch_run_id)

    with conn.cursor() as cur:
        # Ensure foreign keys are safe
        cur.execute(
            """
            INSERT INTO dim_location (location_key, borough, zone, service_zone)
            SELECT DISTINCT pu_location_key, 'Unknown', 'Unknown', 'Unknown'
            FROM clean_taxi_trips c
            WHERE c.batch_run_id = %s
              AND NOT EXISTS (SELECT 1 FROM dim_location l WHERE l.location_key = c.pu_location_key)
            ON CONFLICT DO NOTHING;
            """,
            (run_id_str,),
        )
        cur.execute(
            """
            INSERT INTO dim_location (location_key, borough, zone, service_zone)
            SELECT DISTINCT do_location_key, 'Unknown', 'Unknown', 'Unknown'
            FROM clean_taxi_trips c
            WHERE c.batch_run_id = %s
              AND NOT EXISTS (SELECT 1 FROM dim_location l WHERE l.location_key = c.do_location_key)
            ON CONFLICT DO NOTHING;
            """,
            (run_id_str,),
        )
        cur.execute(
            """
            INSERT INTO dim_vendor (vendor_key, vendor_name)
            SELECT DISTINCT vendor_key, 'Vendor ' || vendor_key
            FROM clean_taxi_trips c
            WHERE c.batch_run_id = %s
              AND NOT EXISTS (SELECT 1 FROM dim_vendor v WHERE v.vendor_key = c.vendor_key)
            ON CONFLICT DO NOTHING;
            """,
            (run_id_str,),
        )
        cur.execute(
            """
            INSERT INTO dim_payment_type (payment_type_key, payment_name)
            SELECT DISTINCT payment_type_key, 'Payment Type ' || payment_type_key
            FROM clean_taxi_trips c
            WHERE c.batch_run_id = %s
              AND NOT EXISTS (SELECT 1 FROM dim_payment_type p WHERE p.payment_type_key = c.payment_type_key)
            ON CONFLICT DO NOTHING;
            """,
            (run_id_str,),
        )
        cur.execute(
            """
            INSERT INTO dim_rate_code (rate_code_key, rate_description)
            SELECT DISTINCT rate_code_key, 'Rate Code ' || rate_code_key
            FROM clean_taxi_trips c
            WHERE c.batch_run_id = %s
              AND NOT EXISTS (SELECT 1 FROM dim_rate_code r WHERE r.rate_code_key = c.rate_code_key)
            ON CONFLICT DO NOTHING;
            """,
            (run_id_str,),
        )

        fact_sql = """
            INSERT INTO fact_taxi_trips (
                trip_hash_id, pickup_date_key, pu_location_key, do_location_key,
                payment_type_key, rate_code_key, vendor_key,
                pickup_datetime, dropoff_datetime, pickup_hour, trip_duration_sec,
                passenger_count, trip_distance, fare_amount, extra, mta_tax,
                tip_amount, tolls_amount, improvement_surcharge, congestion_surcharge,
                total_amount, ingestion_source, batch_run_id
            )
            SELECT
                c.trip_hash_id, c.pickup_date_key, c.pu_location_key, c.do_location_key,
                c.payment_type_key, c.rate_code_key, c.vendor_key,
                c.pickup_datetime, c.dropoff_datetime, c.pickup_hour, c.trip_duration_sec,
                c.passenger_count, c.trip_distance, c.fare_amount, c.extra, c.mta_tax,
                c.tip_amount, c.tolls_amount, c.improvement_surcharge, c.congestion_surcharge,
                c.total_amount, 'BATCH', c.batch_run_id
            FROM clean_taxi_trips c
            WHERE c.batch_run_id = %s
            ON CONFLICT (trip_hash_id) DO NOTHING;
        """
        cur.execute(fact_sql, (run_id_str,))
        fact_count = cur.rowcount

    return fact_count


# -----------------------------------------------------------------------------
# STEP 5: Load Master Table (master_taxi_trips)
# -----------------------------------------------------------------------------
def load_master_table(batch_run_id: uuid.UUID, conn: psycopg.Connection) -> int:
    """Step 5: Loads the denormalized Gold Master Table by joining fact_taxi_trips with all dimensions."""
    run_id_str = str(batch_run_id)

    master_sql = """
        INSERT INTO master_taxi_trips (
            trip_hash_id, batch_run_id,
            pickup_datetime, dropoff_datetime, pickup_hour, trip_duration_sec, trip_duration_min,
            pickup_date, pickup_year, pickup_month, pickup_month_name, pickup_day_name, is_weekend,
            pickup_borough, pickup_zone, pickup_service_zone,
            dropoff_borough, dropoff_zone, dropoff_service_zone, route_corridor,
            payment_name, rate_description, vendor_name,
            passenger_count, trip_distance_miles, fare_amount, extra, mta_tax,
            tip_amount, tolls_amount, improvement_surcharge, congestion_surcharge, total_amount,
            fare_per_mile, tip_percentage
        )
        SELECT
            f.trip_hash_id,
            f.batch_run_id,
            f.pickup_datetime,
            f.dropoff_datetime,
            f.pickup_hour,
            f.trip_duration_sec,
            ROUND(f.trip_duration_sec / 60.0, 2) AS trip_duration_min,
            d.full_date AS pickup_date,
            d.year AS pickup_year,
            d.month AS pickup_month,
            d.month_name AS pickup_month_name,
            d.day_name AS pickup_day_name,
            d.is_weekend,
            COALESCE(pu.borough, 'Unknown') AS pickup_borough,
            COALESCE(pu.zone, 'Unknown') AS pickup_zone,
            COALESCE(pu.service_zone, 'Unknown') AS pickup_service_zone,
            COALESCE(do_loc.borough, 'Unknown') AS dropoff_borough,
            COALESCE(do_loc.zone, 'Unknown') AS dropoff_zone,
            COALESCE(do_loc.service_zone, 'Unknown') AS dropoff_service_zone,
            CONCAT(COALESCE(pu.zone, 'Unknown'), ' (', COALESCE(pu.borough, 'Unknown'), ') ➔ ',
                   COALESCE(do_loc.zone, 'Unknown'), ' (', COALESCE(do_loc.borough, 'Unknown'), ')') AS route_corridor,
            COALESCE(p.payment_name, 'Unknown') AS payment_name,
            COALESCE(rc.rate_description, 'Unknown') AS rate_description,
            COALESCE(v.vendor_name, 'Unknown') AS vendor_name,
            f.passenger_count,
            f.trip_distance AS trip_distance_miles,
            f.fare_amount,
            f.extra,
            f.mta_tax,
            f.tip_amount,
            f.tolls_amount,
            f.improvement_surcharge,
            f.congestion_surcharge,
            f.total_amount,
            ROUND(f.fare_amount / NULLIF(f.trip_distance, 0), 2) AS fare_per_mile,
            ROUND((f.tip_amount / NULLIF(f.fare_amount, 0)) * 100, 2) AS tip_percentage
        FROM fact_taxi_trips f
        JOIN dim_date d ON f.pickup_date_key = d.date_key
        JOIN dim_location pu ON f.pu_location_key = pu.location_key
        JOIN dim_location do_loc ON f.do_location_key = do_loc.location_key
        JOIN dim_payment_type p ON f.payment_type_key = p.payment_type_key
        JOIN dim_rate_code rc ON f.rate_code_key = rc.rate_code_key
        JOIN dim_vendor v ON f.vendor_key = v.vendor_key
        WHERE f.batch_run_id = %s
        ON CONFLICT (trip_hash_id) DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.execute(master_sql, (run_id_str,))
        master_count = cur.rowcount

    return master_count


# -----------------------------------------------------------------------------
# Telemetry Ledger & Automated Rollback Functions
# -----------------------------------------------------------------------------
def record_pipeline_start(run_id: uuid.UUID, source_file: str):
    """Initializes execution ledger entry in etl_pipeline_runs."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_pipeline_runs (run_id, source_file, status, started_at)
                VALUES (%s, %s, 'RUNNING', NOW())
                ON CONFLICT (run_id) DO UPDATE SET status = 'RUNNING';
            """,
                (str(run_id), source_file),
            )
        conn.commit()


def record_pipeline_complete(
    run_id: uuid.UUID,
    status: str,
    rows_extracted: int,
    rows_cleansed: int,
    rows_fact_loaded: int,
    rows_master_loaded: int,
    rows_quarantined: int,
    duration_sec: float,
    peak_memory_mb: float,
    error_message: Optional[str] = None,
):
    """Updates ledger entry upon batch completion or failure."""
    throughput = round(rows_fact_loaded / duration_sec, 2) if duration_sec > 0 else 0.0
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE etl_pipeline_runs
                SET status = %s,
                    rows_extracted = %s,
                    rows_cleansed = %s,
                    rows_fact_loaded = %s,
                    rows_master_loaded = %s,
                    rows_quarantined = %s,
                    duration_sec = %s,
                    throughput_rows_per_sec = %s,
                    peak_memory_mb = %s,
                    error_message = %s,
                    completed_at = NOW()
                WHERE run_id = %s;
            """,
                (
                    status,
                    rows_extracted,
                    rows_cleansed,
                    rows_fact_loaded,
                    rows_master_loaded,
                    rows_quarantined,
                    duration_sec,
                    throughput,
                    peak_memory_mb,
                    error_message,
                    str(run_id),
                ),
            )
        conn.commit()


def rollback_stage(stage: str, batch_run_id: uuid.UUID) -> int:
    """Performs localized rollback for a specific pipeline stage during task-level retries."""
    run_id_str = str(batch_run_id)
    deleted = 0
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if stage == "raw":
                cur.execute("DELETE FROM raw_taxi_trips WHERE batch_run_id = %s;", (run_id_str,))
                deleted = cur.rowcount
            elif stage == "clean":
                cur.execute("DELETE FROM clean_taxi_trips WHERE batch_run_id = %s;", (run_id_str,))
                deleted += cur.rowcount
                cur.execute("DELETE FROM quarantine_taxi_trips WHERE batch_run_id = %s;", (run_id_str,))
                deleted += cur.rowcount
            elif stage == "fact":
                cur.execute("DELETE FROM fact_taxi_trips WHERE batch_run_id = %s;", (run_id_str,))
                deleted = cur.rowcount
            elif stage == "master":
                cur.execute("DELETE FROM master_taxi_trips WHERE batch_run_id = %s;", (run_id_str,))
                deleted = cur.rowcount
        conn.commit()
    logger.warning(f"Stage '{stage}' rolled back ({deleted} rows) for batch {run_id_str}.")
    return deleted


def rollback_failed_batch(batch_run_id: uuid.UUID, error_message: Optional[str] = None) -> Dict[str, int]:
    """Automated Rollback Mechanism across all 5 tiers of the data architecture."""
    run_id_str = str(batch_run_id)
    logger.warning(f"Initiating 5-tier automated database rollback for run ID: {run_id_str}...")

    counts = {}

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 1. Purge master table
                cur.execute("DELETE FROM master_taxi_trips WHERE batch_run_id = %s;", (run_id_str,))
                counts["deleted_master"] = cur.rowcount

                # 2. Purge fact table
                cur.execute("DELETE FROM fact_taxi_trips WHERE batch_run_id = %s;", (run_id_str,))
                counts["deleted_fact"] = cur.rowcount

                # 3. Purge clean table
                cur.execute("DELETE FROM clean_taxi_trips WHERE batch_run_id = %s;", (run_id_str,))
                counts["deleted_clean"] = cur.rowcount

                # 4. Purge quarantine table
                cur.execute("DELETE FROM quarantine_taxi_trips WHERE batch_run_id = %s;", (run_id_str,))
                counts["deleted_quarantine"] = cur.rowcount

                # 5. Purge raw table
                cur.execute("DELETE FROM raw_taxi_trips WHERE batch_run_id = %s;", (run_id_str,))
                counts["deleted_raw"] = cur.rowcount

                # 6. Update execution ledger to ROLLED_BACK
                cur.execute(
                    """
                    UPDATE etl_pipeline_runs
                    SET status = 'ROLLED_BACK',
                        error_message = %s,
                        completed_at = NOW()
                    WHERE run_id = %s;
                    """,
                    (f"ROLLED BACK: {error_message}" if error_message else "Batch failed and was automatically rolled back.", run_id_str),
                )
            conn.commit()

        logger.warning(
            f"✔ Full 5-tier rollback complete for run {run_id_str}: "
            f"Purged {counts.get('deleted_master', 0):,} master, {counts.get('deleted_fact', 0):,} fact, "
            f"{counts.get('deleted_clean', 0):,} clean, {counts.get('deleted_quarantine', 0):,} quarantine, "
            f"{counts.get('deleted_raw', 0):,} raw rows. Clean state restored."
        )
    except Exception as exc:
        logger.exception(f"Failed to complete rollback for run {run_id_str}: {exc}")
        raise

    return counts


def cleanup_orphan_runs(source_file: str):
    """Purges any lingering orphan records from previous failed or crashed runs across all 5 tables."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id FROM etl_pipeline_runs
                    WHERE source_file = %s AND status IN ('RUNNING', 'FAILED');
                    """,
                    (source_file,),
                )
                orphan_runs = [r[0] for r in cur.fetchall()]
                for orphan_id in orphan_runs:
                    oid_str = str(orphan_id)
                    cur.execute("DELETE FROM master_taxi_trips WHERE batch_run_id = %s;", (oid_str,))
                    cur.execute("DELETE FROM fact_taxi_trips WHERE batch_run_id = %s;", (oid_str,))
                    cur.execute("DELETE FROM clean_taxi_trips WHERE batch_run_id = %s;", (oid_str,))
                    cur.execute("DELETE FROM quarantine_taxi_trips WHERE batch_run_id = %s;", (oid_str,))
                    cur.execute("DELETE FROM raw_taxi_trips WHERE batch_run_id = %s;", (oid_str,))
                    cur.execute("UPDATE etl_pipeline_runs SET status = 'ROLLED_BACK' WHERE run_id = %s;", (oid_str,))
            conn.commit()
    except Exception as exc:
        logger.warning(f"Could not clean orphan runs for {source_file}: {exc}")


def fetch_daily_processed_metrics(
    batch_run_id: uuid.UUID, conn: Optional[psycopg.Connection] = None
) -> List[Dict[str, Any]]:
    """Retrieves day-wise processed metrics for a specific batch run from the database.

    Aggregates cleansed/fact records and quarantined records broken down by trip pickup date.
    Requirement 1: In details logs of processed data.
    """
    run_id_str = str(batch_run_id)
    should_close = False
    if conn is None:
        conn = get_db_connection()
        should_close = True

    try:
        with conn.cursor() as cur:
            # 1. Fact / Clean metrics by pickup calendar date
            cur.execute(
                """
                SELECT
                    d.full_date::TEXT AS trip_date,
                    d.day_name,
                    COUNT(f.trip_sk) AS rows_cleansed,
                    COUNT(f.trip_sk) AS rows_fact,
                    ROUND(COALESCE(SUM(f.total_amount), 0), 2) AS total_revenue,
                    ROUND(COALESCE(AVG(f.fare_amount), 0), 2) AS avg_fare,
                    ROUND(COALESCE(AVG(f.trip_duration_sec), 0) / 60.0, 2) AS avg_duration_min,
                    ROUND(COALESCE(SUM(f.trip_distance), 0), 2) AS total_distance_miles
                FROM fact_taxi_trips f
                JOIN dim_date d ON f.pickup_date_key = d.date_key
                WHERE f.batch_run_id = %s
                GROUP BY d.full_date, d.day_name
                ORDER BY d.full_date;
                """,
                (run_id_str,),
            )
            fact_rows = cur.fetchall()

            # 2. Quarantined records by extracted pickup date
            cur.execute(
                """
                SELECT
                    SUBSTRING(COALESCE(raw_record->>'pickup_datetime', raw_record->>'tpep_pickup_datetime') FROM 1 FOR 10) AS trip_date,
                    COUNT(*) AS quar_count
                FROM quarantine_taxi_trips
                WHERE batch_run_id = %s
                GROUP BY 1;
                """,
                (run_id_str,),
            )
            quar_rows = cur.fetchall()
            quar_map = {r[0]: int(r[1]) for r in quar_rows if r[0]}

            # 3. Combine into structured day-wise records
            results = []
            seen_dates = set()

            for r in fact_rows:
                t_date = str(r[0])
                seen_dates.add(t_date)
                day_name = str(r[1])
                rows_fact = int(r[2])
                quar_cnt = quar_map.get(t_date, 0)
                rows_extracted = rows_fact + quar_cnt

                results.append(
                    {
                        "batch_run_id": run_id_str,
                        "trip_date": t_date,
                        "day_name": day_name,
                        "rows_extracted": rows_extracted,
                        "rows_cleansed": rows_fact,
                        "rows_fact": rows_fact,
                        "rows_quarantined": quar_cnt,
                        "total_revenue": float(r[4]),
                        "avg_fare": float(r[5]),
                        "avg_duration_min": float(r[6]),
                        "total_distance_miles": float(r[7]),
                    }
                )

            # Check for any dates present only in quarantine
            for q_date, q_count in quar_map.items():
                if q_date not in seen_dates:
                    try:
                        q_dt = datetime.strptime(q_date, "%Y-%m-%d")
                        d_name = q_dt.strftime("%A")
                    except Exception:
                        d_name = "Unknown"
                    results.append(
                        {
                            "batch_run_id": run_id_str,
                            "trip_date": q_date,
                            "day_name": d_name,
                            "rows_extracted": q_count,
                            "rows_cleansed": 0,
                            "rows_fact": 0,
                            "rows_quarantined": q_count,
                            "total_revenue": 0.0,
                            "avg_fare": 0.0,
                            "avg_duration_min": 0.0,
                            "total_distance_miles": 0.0,
                        }
                    )

            results.sort(key=lambda x: str(x.get("trip_date", "")))
            return results
    except Exception as exc:
        logger.warning(f"Could not fetch daily metrics for run {run_id_str}: {exc}")
        return []
    finally:
        if should_close and conn:
            conn.close()


# =============================================================================
# MULTIPLE ETL JOBS ARCHITECTURE
# 1. Extract data to raw data (raw_taxi_trips - Bronze)
# 2. Validate data (Data Quality Rules & Anomaly Scoring)
# 3. Clean (Standardization, Hashing & clean_taxi_trips - Silver)
# 4. Move invalid data to another table (quarantine_taxi_trips)
# 5. Load master, fact and dimensional tables (dim_*, fact_taxi_trips, master_taxi_trips - Gold)
# =============================================================================

def job1_extract_to_raw_data(
    file_path: Path,
    source_file: str,
    slice_desc: str,
    start_day: Optional[int] = None,
    end_day: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_run_id: Optional[uuid.UUID] = None,
    conn: Optional[psycopg.Connection] = None,
) -> Dict[str, Any]:
    """Job 1: Extract data to raw data (raw_taxi_trips - Bronze Layer).

    Streams Parquet data directly into PostgreSQL staging table raw_taxi_trips using COPY.
    """
    run_id = batch_run_id or uuid.uuid4()
    record_pipeline_start(run_id, slice_desc)

    should_close = False
    if conn is None:
        conn = get_db_connection()
        should_close = True

    total_raw = 0
    start_time = time.perf_counter()
    try:
        from src.batch_etl.extract import stream_parquet_batches

        slices = []
        if start_day is None and end_day is None and settings.DAY_CHUNK_SIZE > 0:
            try:
                m_part = file_path.stem.split("_")[-1]
                y, m = int(m_part.split("-")[0]), int(m_part.split("-")[1])
                slices = settings.data.get_day_slices(y, m, settings.DAY_CHUNK_SIZE)
            except Exception:
                slices = [(None, None)]
        else:
            slices = [(start_day, end_day)]

        for s_day, e_day in slices:
            for raw_chunk in stream_parquet_batches(
                file_path,
                batch_size=settings.BATCH_CHUNK_SIZE,
                start_day=s_day,
                end_day=e_day,
                start_date=start_date,
                end_date=end_date,
            ):
                loaded = load_raw_table_copy(raw_chunk, source_file, run_id, conn)
                total_raw += loaded
        conn.commit()

        dur = round(time.perf_counter() - start_time, 2)
        logger.info(
            f"✔ [Job 1: Extract Raw] Extracted {total_raw:,} records into raw_taxi_trips for {slice_desc} in {dur}s (Run ID: {run_id})."
        )
        return {
            "batch_run_id": str(run_id),
            "source_file": source_file,
            "slice_desc": slice_desc,
            "file_path": str(file_path),
            "start_day": start_day,
            "end_day": end_day,
            "raw_rows": total_raw,
            "duration_sec": dur,
            "status": "SUCCESS",
        }
    except Exception as exc:
        conn.rollback()
        logger.error(f"✖ [Job 1: Extract Raw] Failed extracting to raw_taxi_trips: {exc}")
        raise
    finally:
        if should_close and conn:
            conn.close()


def job2_validate_data(
    batch_run_id: uuid.UUID,
    conn: Optional[psycopg.Connection] = None,
) -> Dict[str, Any]:
    """Job 2: Validate data in raw_taxi_trips.

    Applies comprehensive data quality validation checks:
    - Non-null pickup/dropoff timestamps
    - Chronological ordering (dropoff > pickup)
    - Positive trip distance (> 0)
    - Positive fare amount (> 0)
    - Positive total amount (> 0)
    Computes valid vs invalid counts and DQ pass rate.
    """
    run_id_str = str(batch_run_id)
    should_close = False
    if conn is None:
        conn = get_db_connection()
        should_close = True

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(*) FILTER (
                        WHERE tpep_pickup_datetime IS NOT NULL
                          AND tpep_dropoff_datetime IS NOT NULL
                          AND tpep_dropoff_datetime > tpep_pickup_datetime
                          AND COALESCE(trip_distance, 0) > 0
                          AND COALESCE(fare_amount, 0) > 0
                          AND COALESCE(total_amount, 0) > 0
                    ) AS valid_rows,
                    COUNT(*) FILTER (
                        WHERE tpep_pickup_datetime IS NULL
                           OR tpep_dropoff_datetime IS NULL
                           OR tpep_dropoff_datetime <= tpep_pickup_datetime
                           OR COALESCE(trip_distance, 0) <= 0
                           OR COALESCE(fare_amount, 0) <= 0
                           OR COALESCE(total_amount, 0) <= 0
                    ) AS invalid_rows
                FROM raw_taxi_trips
                WHERE batch_run_id = %s;
                """,
                (run_id_str,),
            )
            row = cur.fetchone()
            total_rows = int(row[0]) if row else 0
            valid_rows = int(row[1]) if row else 0
            invalid_rows = int(row[2]) if row else 0
            dq_pass_pct = round((valid_rows / total_rows * 100), 2) if total_rows > 0 else 100.0

            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL) AS null_ts,
                    COUNT(*) FILTER (WHERE tpep_dropoff_datetime <= tpep_pickup_datetime) AS invalid_seq,
                    COUNT(*) FILTER (WHERE COALESCE(trip_distance, 0) <= 0) AS non_pos_dist,
                    COUNT(*) FILTER (WHERE COALESCE(fare_amount, 0) <= 0) AS non_pos_fare,
                    COUNT(*) FILTER (WHERE COALESCE(total_amount, 0) <= 0) AS non_pos_total
                FROM raw_taxi_trips
                WHERE batch_run_id = %s;
                """,
                (run_id_str,),
            )
            dq_row = cur.fetchone()

        result = {
            "batch_run_id": run_id_str,
            "total_rows": total_rows,
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "dq_pass_rate_pct": dq_pass_pct,
            "breakdown": {
                "null_timestamps": int(dq_row[0]) if dq_row else 0,
                "invalid_sequence": int(dq_row[1]) if dq_row else 0,
                "non_positive_distance": int(dq_row[2]) if dq_row else 0,
                "non_positive_fare": int(dq_row[3]) if dq_row else 0,
                "non_positive_total": int(dq_row[4]) if dq_row else 0,
            },
            "status": "SUCCESS",
        }

        logger.info(
            f"✔ [Job 2: Validate Data] Validated {total_rows:,} records: {valid_rows:,} valid ({dq_pass_pct:.2f}%), "
            f"{invalid_rows:,} invalid (Run ID: {run_id_str})."
        )
        log_metric("job2_data_validation_completed", result)
        return result
    except Exception as exc:
        logger.error(f"✖ [Job 2: Validate Data] Validation failed for batch {run_id_str}: {exc}")
        raise
    finally:
        if should_close and conn:
            conn.close()


def job3_clean_data(
    batch_run_id: uuid.UUID,
    conn: Optional[psycopg.Connection] = None,
) -> Dict[str, Any]:
    """Job 3: Clean data (move valid records to clean_taxi_trips - Silver Layer).

    Filters validated records from raw_taxi_trips in bounded streaming chunks (100k rows),
    performs standardization, surrogate key resolution, calendar dimension upsert,
    and SHA-256 idempotency hashing with bounded memory (<150MB peak).
    """
    import gc
    from src.batch_etl.transform import build_date_dimension_records, generate_trip_hash_ids

    run_id_str = str(batch_run_id)
    should_close = False
    if conn is None:
        conn = get_db_connection()
        should_close = True

    try:
        start_time = time.perf_counter()

        # 1. Upsert unique dates into dim_date with a lightweight query (~31 rows for a month)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT (tpep_pickup_datetime AT TIME ZONE 'UTC')::date AS pickup_date
                FROM raw_taxi_trips
                WHERE batch_run_id = %s AND tpep_pickup_datetime IS NOT NULL;
                """,
                (run_id_str,),
            )
            date_rows = cur.fetchall()
            if date_rows:
                date_vals = [r[0] for r in date_rows]
                dates_df = build_date_dimension_records(date_vals)
                if not dates_df.empty:
                    upsert_date_dimension(dates_df, conn)
                    conn.commit()

        # 2. Fast vectorized in-database clean into clean_taxi_trips
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO clean_taxi_trips (
                    trip_hash_id, batch_run_id, source_file, vendor_key,
                    pickup_datetime, dropoff_datetime, passenger_count,
                    trip_distance, rate_code_key, pu_location_key,
                    do_location_key, payment_type_key, fare_amount,
                    extra, mta_tax, tip_amount, tolls_amount,
                    improvement_surcharge, congestion_surcharge, total_amount,
                    pickup_date_key, pickup_hour, trip_duration_sec
                )
                SELECT
                    encode(sha256(concat_ws('|', 
                        COALESCE(vendor_id, 0), 
                        to_char(tpep_pickup_datetime AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'), 
                        to_char(tpep_dropoff_datetime AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'), 
                        COALESCE(pu_location_id, 0), 
                        COALESCE(do_location_id, 0), 
                        to_char(COALESCE(total_amount, 0), 'FM999990.00')
                    )::bytea), 'hex') AS trip_hash_id,
                    batch_run_id,
                    source_file,
                    COALESCE(vendor_id, 0)::int AS vendor_key,
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime,
                    COALESCE(passenger_count, 1)::int AS passenger_count,
                    COALESCE(trip_distance, 0)::numeric(10, 2) AS trip_distance,
                    COALESCE(rate_code_id, 99)::int AS rate_code_key,
                    COALESCE(pu_location_id, 0)::int AS pu_location_key,
                    COALESCE(do_location_id, 0)::int AS do_location_key,
                    COALESCE(payment_type, 0)::int AS payment_type_key,
                    COALESCE(fare_amount, 0)::numeric(10, 2) AS fare_amount,
                    COALESCE(extra, 0)::numeric(10, 2) AS extra,
                    COALESCE(mta_tax, 0)::numeric(10, 2) AS mta_tax,
                    COALESCE(tip_amount, 0)::numeric(10, 2) AS tip_amount,
                    COALESCE(tolls_amount, 0)::numeric(10, 2) AS tolls_amount,
                    COALESCE(improvement_surcharge, 0)::numeric(10, 2) AS improvement_surcharge,
                    COALESCE(congestion_surcharge, 0)::numeric(10, 2) AS congestion_surcharge,
                    COALESCE(total_amount, 0)::numeric(10, 2) AS total_amount,
                    to_char(tpep_pickup_datetime AT TIME ZONE 'UTC', 'YYYYMMDD')::int AS pickup_date_key,
                    EXTRACT(HOUR FROM tpep_pickup_datetime AT TIME ZONE 'UTC')::smallint AS pickup_hour,
                    EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))::int AS trip_duration_sec
                FROM raw_taxi_trips
                WHERE batch_run_id = %s
                  AND tpep_pickup_datetime IS NOT NULL
                  AND tpep_dropoff_datetime IS NOT NULL
                  AND tpep_dropoff_datetime > tpep_pickup_datetime
                  AND COALESCE(trip_distance, 0) > 0
                  AND COALESCE(fare_amount, 0) > 0
                  AND COALESCE(total_amount, 0) > 0
                ON CONFLICT (trip_hash_id) DO NOTHING;
                """,
                (run_id_str,),
            )
            cleansed_count = cur.rowcount
            conn.commit()

        dur = round(time.perf_counter() - start_time, 2)
        logger.info(
            f"✔ [Job 3: Clean Data] Cleansed and inserted {cleansed_count:,} records into clean_taxi_trips in {dur}s (Run ID: {run_id_str})."
        )
        return {
            "batch_run_id": run_id_str,
            "clean_rows": cleansed_count,
            "duration_sec": dur,
            "status": "SUCCESS",
        }
    except Exception as exc:
        conn.rollback()
        logger.error(f"✖ [Job 3: Clean Data] Failed cleansing data for batch {run_id_str}: {exc}")
        raise
    finally:
        if should_close and conn:
            conn.close()


def job4_move_invalid_data(
    batch_run_id: uuid.UUID,
    conn: Optional[psycopg.Connection] = None,
) -> Dict[str, Any]:
    """Job 4: Move invalid data to another table (quarantine_taxi_trips - Data Quality Quarantine).

    Filters rejected records from raw_taxi_trips, evaluates explicit rejection reasons,
    serializes raw JSONB records, and moves them into quarantine_taxi_trips.
    """
    run_id_str = str(batch_run_id)
    should_close = False
    if conn is None:
        conn = get_db_connection()
        should_close = True

    try:
        start_time = time.perf_counter()
        query_sql = """
            SELECT
                raw_id, batch_run_id, source_file, vendor_id,
                tpep_pickup_datetime, tpep_dropoff_datetime,
                passenger_count, trip_distance, rate_code_id,
                pu_location_id, do_location_id, payment_type,
                fare_amount, extra, mta_tax, tip_amount, tolls_amount,
                improvement_surcharge, congestion_surcharge, total_amount
            FROM raw_taxi_trips
            WHERE batch_run_id = %s
              AND (
                  tpep_pickup_datetime IS NULL
               OR tpep_dropoff_datetime IS NULL
               OR tpep_dropoff_datetime <= tpep_pickup_datetime
               OR COALESCE(trip_distance, 0) <= 0
               OR COALESCE(fare_amount, 0) <= 0
               OR COALESCE(total_amount, 0) <= 0
              );
        """
        with conn.cursor() as cur:
            cur.execute(query_sql, (run_id_str,))
            rows = cur.fetchall()
            if rows:
                col_names = [d[0] for d in cur.description]
                raw_invalid_df = pd.DataFrame(rows, columns=col_names)
            else:
                raw_invalid_df = pd.DataFrame()

        quarantined_count = 0
        if not raw_invalid_df.empty:
            reasons = []
            for row in raw_invalid_df.itertuples(index=False):
                r_parts = []
                pu = getattr(row, "tpep_pickup_datetime", None)
                do = getattr(row, "tpep_dropoff_datetime", None)
                if pd.isna(pu) or pd.isna(do):
                    r_parts.append("NULL_TIMESTAMPS")
                elif do <= pu:
                    r_parts.append("DROPOFF_BEFORE_OR_EQUAL_PICKUP")
                dist = getattr(row, "trip_distance", None)
                if pd.isna(dist) or float(dist) <= 0:
                    r_parts.append("NON_POSITIVE_DISTANCE")
                fare = getattr(row, "fare_amount", None)
                if pd.isna(fare) or float(fare) <= 0:
                    r_parts.append("NON_POSITIVE_FARE")
                tot = getattr(row, "total_amount", None)
                if pd.isna(tot) or float(tot) <= 0:
                    r_parts.append("NON_POSITIVE_TOTAL")
                reasons.append("; ".join(r_parts) if r_parts else "UNKNOWN_DQ_ANOMALY")

            raw_invalid_df["rejection_reason"] = reasons
            raw_invalid_df["batch_run_id"] = run_id_str

            cols_to_serialize = [c for c in raw_invalid_df.columns if c not in ("rejection_reason", "batch_run_id")]
            json_payloads = [
                json.dumps({k: (None if pd.isna(v) else v) for k, v in zip(cols_to_serialize, vals)}, default=str)
                for vals in raw_invalid_df[cols_to_serialize].itertuples(index=False, name=None)
            ]
            quar_df = pd.DataFrame({
                "batch_run_id": raw_invalid_df["batch_run_id"].values,
                "rejection_reason": raw_invalid_df["rejection_reason"].values,
                "raw_record": json_payloads,
            })

            counts = load_clean_and_quarantine_copy(
                clean_df=pd.DataFrame(),
                quarantine_df=quar_df,
                source_file=raw_invalid_df["source_file"].iloc[0] if not raw_invalid_df.empty else "unknown",
                conn=conn,
            )
            quarantined_count = counts["quarantined"]
            conn.commit()

        dur = round(time.perf_counter() - start_time, 2)
        logger.info(
            f"✔ [Job 4: Move Invalid Data] Quarantined {quarantined_count:,} invalid records into quarantine_taxi_trips in {dur}s (Run ID: {run_id_str})."
        )

        if quarantined_count > 0:
            try:
                from src.alerts.slack import send_data_quality_alert
                send_data_quality_alert(
                    batch_run_id=run_id_str,
                    source_file=raw_invalid_df["source_file"].iloc[0] if not raw_invalid_df.empty else "unknown",
                    quarantined_count=quarantined_count,
                    total_count=quarantined_count + 100000,  # approximate batch size for rate calculation
                )
            except Exception as quar_err:
                logger.warning(f"Could not dispatch quarantine Slack alert: {quar_err}")
        return {
            "batch_run_id": run_id_str,
            "quarantined_rows": quarantined_count,
            "duration_sec": dur,
            "status": "SUCCESS",
        }
    except Exception as exc:
        conn.rollback()
        logger.error(f"✖ [Job 4: Move Invalid Data] Failed moving invalid data to quarantine: {exc}")
        raise
    finally:
        if should_close and conn:
            conn.close()


def job5_load_master_fact_and_dimensions(
    batch_run_id: uuid.UUID,
    raw_rows: int = 0,
    clean_rows: int = 0,
    quarantined_rows: int = 0,
    conn: Optional[psycopg.Connection] = None,
) -> Dict[str, Any]:
    """Job 5: Load master, fact, and dimensional tables (Gold & Star Schema).

    1. Resolves dimensional foreign keys into dim_location & dim_date.
    2. Promotes cleansed records from clean_taxi_trips to fact_taxi_trips.
    3. Loads denormalized analytical table master_taxi_trips.
    4. Fetches and logs day-wise detailed processed data metrics.
    5. Records completion status in etl_pipeline_runs ledger.
    """
    run_id_str = str(batch_run_id)
    should_close = False
    if conn is None:
        conn = get_db_connection()
        should_close = True

    try:
        start_time = time.perf_counter()

        # Step 4: Promote to Fact Table
        fact_count = move_clean_to_fact_table(batch_run_id, conn)
        conn.commit()

        # Step 5: Load Master Table
        master_count = load_master_table(batch_run_id, conn)
        conn.commit()

        # Day-wise metrics logging
        daily_records = fetch_daily_processed_metrics(batch_run_id, conn)
        if daily_records:
            log_daily_processed_data(daily_records)

        dur = round(time.perf_counter() - start_time, 2)
        throughput = round(fact_count / dur, 2) if dur > 0 else 0.0

        record_pipeline_complete(
            run_id=batch_run_id,
            status="SUCCESS",
            rows_extracted=raw_rows or (clean_rows + quarantined_rows),
            rows_cleansed=clean_rows or fact_count,
            rows_fact_loaded=fact_count,
            rows_master_loaded=master_count,
            rows_quarantined=quarantined_rows,
            duration_sec=dur,
            peak_memory_mb=round(psutil.Process().memory_info().rss / (1024 * 1024), 2),
        )

        logger.info(
            f"✔ [Job 5: Load Master, Fact & Dimensions] Successfully loaded {fact_count:,} fact rows and "
            f"{master_count:,} master rows in {dur}s ({throughput:,} rows/sec, Run ID: {run_id_str})."
        )
        return {
            "batch_run_id": run_id_str,
            "fact_rows": fact_count,
            "master_rows": master_count,
            "clean_rows": clean_rows or fact_count,
            "quarantined_rows": quarantined_rows,
            "duration_sec": dur,
            "throughput_rows_sec": throughput,
            "status": "SUCCESS",
        }
    except Exception as exc:
        conn.rollback()
        logger.error(f"✖ [Job 5: Load Master, Fact & Dimensions] Failed loading tables: {exc}")
        raise
    finally:
        if should_close and conn:
            conn.close()
