"""Apache Airflow Scheduled DAG: Sequential NYC Taxi Ingestion with Batch Slicing & Auto-Healing.

5-Step Pipeline Progression per Batch Slice:
1. Download file to data/raw/ with automatic corruption healing and pre-extract validation.
2. Extract data to a table (raw_taxi_trips) - Bronze staging layer.
3. Move valid data to next table cleansing (clean_taxi_trips & quarantine_taxi_trips) - Silver layer.
4. Move to next table fact (fact_taxi_trips) - Star Schema with dimensional foreign keys.
5. Load master table (master_taxi_trips) - Denormalized Gold analytical table.

Batch Chunking & Disaster Recovery:
- Slices each monthly dataset by DAY_CHUNK_SIZE (e.g. Days 1 to 10 then finish, then 11 to 20 then finish).
- Configurable RAW_DATA_URL, START_MONTH, END_MONTH, START_DATE, END_DATE, and BATCH_CHUNK_SIZE.
"""

from datetime import datetime, timedelta
import logging
from pathlib import Path
import time
from typing import Dict, List, Optional
import uuid
import psutil

from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup

# Import project modules
from settings import paths, settings
from src.alerts.slack import (
    send_data_quality_alert,
    send_pipeline_start_alert,
    send_job_start_alert,
    send_pipeline_summary_alert,
    send_sla_miss_alert,
    send_task_failure_alert,
    send_task_retry_alert,
)
from src.logger import (
    log_metric,
    logger,
    log_daily_processed_data,
    log_job_summary,
)
from src.batch_etl.extract import (
    ParquetValidationError,
    check_raw_file_available,
    ensure_raw_file_for_timeframe,
    download_single_month_auto_heal,
    download_taxi_zone_lookup,
    get_day_slices,
    is_parquet_already_ingested,
    remove_raw_parquet_file,
    stream_parquet_batches,
    validate_parquet_file,
)
from src.batch_etl.transform import transform_batch
from src.batch_etl.load import (
    apply_analytics_views,
    apply_schema_migrations,
    cleanup_orphan_runs,
    fetch_daily_processed_metrics,
    get_db_connection,
    job1_extract_to_raw_data,
    job2_validate_data,
    job3_clean_data,
    job4_move_invalid_data,
    job5_load_master_fact_and_dimensions,
    load_clean_and_quarantine_copy,
    load_master_table,
    load_raw_table_copy,
    move_clean_to_fact_table,
    record_pipeline_complete,
    record_pipeline_start,
    rollback_failed_batch,
    seed_taxi_zones,
    upsert_date_dimension,
)

# -----------------------------------------------------------------------------
# Alerting & Auto-Healing Callbacks (Task 4)
# -----------------------------------------------------------------------------
def airflow_failure_alert(context: Dict):
    """Triggered on task failure to log critical alerts, send Slack notifications, and record failure telemetry."""
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown_task"
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    execution_date = str(context.get("execution_date", datetime.utcnow()))
    exception = str(context.get("exception", "No exception message provided"))

    alert_payload = {
        "event": "AIRFLOW_TASK_FAILURE_ALERT",
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": execution_date,
        "error_message": exception,
        "alert_timestamp": datetime.utcnow().isoformat(),
    }

    log_metric("airflow_task_failure", alert_payload, level=logging.CRITICAL)
    logging.critical(
        f"🚨 [AIRFLOW CRITICAL ALERT] Task '{task_id}' in DAG '{dag_id}' failed!\n"
        f"Execution: {execution_date}\n"
        f"Error: {exception}"
    )

    try:
        send_task_failure_alert(context)
    except Exception as slack_err:
        logging.warning(f"Could not dispatch failure alert to Slack: {slack_err}")


def airflow_retry_alert(context: Dict):
    """Triggered on task retry to log retry telemetry and notify Slack."""
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown_task"
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    try_number = getattr(task_instance, "try_number", 1)
    max_tries = getattr(task_instance, "max_tries", 3)
    exception = str(context.get("exception", "Transient failure detected"))

    logging.warning(f"⚠️ [AIRFLOW RETRY] Task '{task_id}' initiating retry {try_number} of {max_tries}...")
    log_metric("airflow_task_retry", {"task_id": task_id, "try_number": try_number, "max_tries": max_tries})

    try:
        send_task_retry_alert(context)
    except Exception as slack_err:
        logging.warning(f"Could not dispatch retry alert to Slack: {slack_err}")


def airflow_sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Triggered on SLA breach to notify Slack."""
    logging.error(f"⏰ [AIRFLOW SLA BREACH] Tasks missed SLA: {task_list}")
    try:
        send_sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis)
    except Exception as slack_err:
        logging.warning(f"Could not dispatch SLA breach alert to Slack: {slack_err}")


def airflow_success_telemetry(context: Dict):
    """Logs telemetry on successful task completion."""
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown_task"
    duration = task_instance.duration if task_instance else None

    log_metric(
        "airflow_task_success",
        {
            "task_id": task_id,
            "duration_sec": duration,
            "completed_at": datetime.utcnow().isoformat(),
        },
    )


# -----------------------------------------------------------------------------
# Default Task Arguments (Configured with Auto-Healing Retries & Slack Alerts)
# -----------------------------------------------------------------------------
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": airflow_failure_alert,
    "on_retry_callback": airflow_retry_alert,
    "on_success_callback": airflow_success_telemetry,
}

# -----------------------------------------------------------------------------
# DAG Definition: Scheduled Monthly & On-Action Execution
# -----------------------------------------------------------------------------
with DAG(
    dag_id="nyc_taxi_batch_pipeline",
    default_args=default_args,
    description="Sequential 5-Step NYC Taxi Ingestion with Batch Slicing & Auto-Healing",
    schedule_interval="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=airflow_sla_miss_alert,
    tags=["nyc_taxi", "batch_etl", "auto_healing", "validation", "batch_slices"],
) as dag:

    @task(task_id="seed_reference_data")
    def seed_reference_data_task():
        """Bootstraps Star Schema DDL and seeds TLC taxi zones (cold start / recovery)."""
        logging.info("Applying Star Schema DDL and seeding reference dimensions...")
        try:
            start_tf = settings.data.get_overall_timeframe()
            send_pipeline_start_alert(
                job_id="nyc_taxi_batch_pipeline",
                target_months=settings.target_months,
                timeframe=start_tf,
                execution_engine="Apache Airflow",
            )
        except Exception as alert_err:
            logging.warning(f"Could not dispatch pipeline start alert to Slack: {alert_err}")

        apply_schema_migrations(paths.schema_ddl_path)
        zone_csv = download_taxi_zone_lookup(paths.taxi_zone_csv_path)
        seed_taxi_zones(zone_csv)
        logging.info("Reference dimensions seeded successfully.")
        return {"status": "SUCCESS"}

    # -------------------------------------------------------------------------
    # Main ETL Job Factory: Multiple Jobs under One Main Job in Airflow
    # -------------------------------------------------------------------------
    def create_main_etl_job(month_str: str):
        clean_month = month_str.replace("-", "_")
        main_group_id = f"main_etl_job_{clean_month}" if len(settings.target_months) > 1 else "main_etl_job"

        with TaskGroup(group_id=main_group_id, tooltip=f"Main NYC Taxi ETL Job ({month_str})") as main_job:

            # -----------------------------------------------------------------
            # Step 1: Check Raw File Availability & Download if Missing
            # -----------------------------------------------------------------
            @task(task_id="step1_download_parquet")
            def download_task() -> Dict[str, any]:
                """Step 1: Check if already ingested; if not, check availability & download if missing."""
                if is_parquet_already_ingested(month_str):
                    logging.info(
                        f"⏩ [Idempotency] Month {month_str} ('yellow_tripdata_{month_str}.parquet') is ALREADY INGESTED into the warehouse. Skipping download and ingestion."
                    )
                    remove_raw_parquet_file(month_str)
                    return {
                        "month": month_str,
                        "skipped": True,
                        "already_ingested": True,
                        "file_name": paths.get_parquet_filename(month_str),
                        "file_path": str(paths.get_parquet_path(month_str)),
                        "num_rows": 0,
                    }

                start_date, end_date = settings.data.get_timeframe_for_month(month_str)
                try:
                    send_job_start_alert(
                        month=month_str,
                        file_name=paths.get_parquet_filename(month_str),
                        timeframe=(start_date, end_date),
                        job_name=f"main_etl_job_{month_str}",
                    )
                except Exception as alert_err:
                    logging.warning(f"Could not dispatch job start alert to Slack: {alert_err}")

                logging.info(
                    f"[Step 1: Timeframe Check] Considering timeframe: {start_date} to {end_date} (Month: {month_str}). Checking raw file availability..."
                )
                file_path = ensure_raw_file_for_timeframe(
                    month_str=month_str,
                    start_date=start_date,
                    end_date=end_date,
                )

                try:
                    metadata = validate_parquet_file(file_path)
                    logging.info(
                        f"✔ [Step 1] File {file_path.name} verified healthy for timeframe [{start_date} to {end_date}] ({metadata['num_rows']:,} rows)."
                    )
                except ParquetValidationError as err:
                    logging.error(
                        f"[Step 1] Pre-extract validation failed for {file_path.name}: {err}. Initiating auto-healing download..."
                    )
                    file_path = ensure_raw_file_for_timeframe(
                        month_str=month_str,
                        start_date=start_date,
                        end_date=end_date,
                        force_download=True,
                    )
                    metadata = validate_parquet_file(file_path)
                    logging.info(f"✔ [Step 1] Auto-healing succeeded. File {file_path.name} verified.")

                return {
                    "month": month_str,
                    "skipped": False,
                    "already_ingested": False,
                    "start_date": start_date,
                    "end_date": end_date,
                    "file_path": str(file_path),
                    "file_name": file_path.name,
                    "num_rows": metadata["num_rows"],
                }

            # -----------------------------------------------------------------
            # Job 1: Extract data to raw data (raw_taxi_trips - Bronze)
            # -----------------------------------------------------------------
            @task(task_id="job1_extract_to_raw_data")
            def job1_extract_task(file_meta: Dict[str, any]) -> Dict[str, any]:
                """Job 1: Extract data to raw data (raw_taxi_trips - Bronze Layer)."""
                if file_meta.get("skipped"):
                    logging.info(f"⏩ [Job 1: Extract Raw] Skipped — Month {month_str} is already ingested into the warehouse.")
                    return {"skipped": True, "raw_rows": 0, "batch_run_id": str(uuid.uuid4())}

                source_file = file_meta["file_name"]
                file_path = Path(file_meta["file_path"])
                slice_desc = f"{source_file} [Batch Ingestion]"
                batch_run_id = uuid.uuid4()

                logging.info(f"▶ [Job 1: Extract Raw] Ingesting {source_file} into raw_taxi_trips (Run ID: {batch_run_id})...")
                try:
                    res = job1_extract_to_raw_data(
                        file_path=file_path,
                        source_file=source_file,
                        slice_desc=slice_desc,
                        start_date=settings.START_DATE,
                        end_date=settings.END_DATE,
                        batch_run_id=batch_run_id,
                    )
                    return res
                except Exception as exc:
                    logging.error(f"[Job 1] Extraction failed for {source_file}: {exc}")
                    rollback_failed_batch(batch_run_id, error_message=str(exc))
                    raise

            # -----------------------------------------------------------------
            # Job 2: Validate data (DQ Rules & Anomaly Check)
            # -----------------------------------------------------------------
            @task(task_id="job2_validate_data")
            def job2_validate_task(job1_meta: Dict[str, any]) -> Dict[str, any]:
                """Job 2: Validate data (Quality checks, timestamp orders, valid fares & distances)."""
                batch_run_id = uuid.UUID(job1_meta["batch_run_id"])
                raw_rows = job1_meta.get("raw_rows", 0)
                logging.info(f"▶ [Job 2: Validate Data] Validating {raw_rows:,} records for run {batch_run_id}...")

                if raw_rows == 0:
                    logging.info("[Job 2] 0 records to validate. Skipping.")
                    return {
                        "batch_run_id": str(batch_run_id),
                        "raw_rows": 0,
                        "valid_rows": 0,
                        "invalid_rows": 0,
                        "dq_pass_rate_pct": 100.0,
                        "status": "SUCCESS",
                    }

                try:
                    res = job2_validate_data(batch_run_id)
                    res["raw_rows"] = raw_rows
                    res["slice_desc"] = job1_meta.get("slice_desc")
                    return res
                except Exception as exc:
                    logging.error(f"[Job 2] Validation failed for run {batch_run_id}: {exc}")
                    rollback_failed_batch(batch_run_id, error_message=str(exc))
                    raise

            # -----------------------------------------------------------------
            # Job 3: Clean (Standardization, Hashing & clean_taxi_trips - Silver)
            # -----------------------------------------------------------------
            @task(task_id="job3_clean_data")
            def job3_clean_task(job2_meta: Dict[str, any]) -> Dict[str, any]:
                """Job 3: Clean valid data (Standardization, SHA-256 idempotency & clean_taxi_trips)."""
                batch_run_id = uuid.UUID(job2_meta["batch_run_id"])
                valid_rows = job2_meta.get("valid_rows", 0)
                logging.info(f"▶ [Job 3: Clean Data] Cleaning {valid_rows:,} valid records for run {batch_run_id}...")

                if valid_rows == 0:
                    logging.info("[Job 3] 0 valid records to clean. Skipping.")
                    return {"batch_run_id": str(batch_run_id), "clean_rows": 0, "status": "SUCCESS"}

                try:
                    res = job3_clean_data(batch_run_id)
                    return res
                except Exception as exc:
                    logging.error(f"[Job 3] Cleaning failed for run {batch_run_id}: {exc}")
                    rollback_failed_batch(batch_run_id, error_message=str(exc))
                    raise

            # -----------------------------------------------------------------
            # Job 4: Move invalid data to another table (quarantine_taxi_trips)
            # -----------------------------------------------------------------
            @task(task_id="job4_move_invalid_data")
            def job4_quarantine_task(job2_meta: Dict[str, any]) -> Dict[str, any]:
                """Job 4: Move invalid data to another table (quarantine_taxi_trips with audit reason)."""
                batch_run_id = uuid.UUID(job2_meta["batch_run_id"])
                invalid_rows = job2_meta.get("invalid_rows", 0)
                logging.info(f"▶ [Job 4: Move Invalid Data] Quarantining {invalid_rows:,} invalid records for run {batch_run_id}...")

                if invalid_rows == 0:
                    logging.info("[Job 4] 0 invalid records to quarantine. Clean pass.")
                    return {"batch_run_id": str(batch_run_id), "quarantined_rows": 0, "status": "SUCCESS"}

                try:
                    res = job4_move_invalid_data(batch_run_id)
                    # Trigger Slack alert for quarantined data
                    try:
                        send_data_quality_alert(
                            batch_run_id=str(batch_run_id),
                            source_file=f"yellow_tripdata_{month}.parquet",
                            quarantined_count=res.get("quarantined_rows", invalid_rows),
                            total_count=invalid_rows + 100000,
                        )
                    except Exception as alert_err:
                        logging.warning(f"Slack quarantine alert dispatch failed: {alert_err}")
                    return res
                except Exception as exc:
                    logging.error(f"[Job 4] Quarantine failed for run {batch_run_id}: {exc}")
                    rollback_failed_batch(batch_run_id, error_message=str(exc))
                    raise

            # -----------------------------------------------------------------
            # Job 5: Load master, fact and dimensional tables (Gold & Star Schema)
            # -----------------------------------------------------------------
            @task(task_id="job5_load_master_fact_dimensions")
            def job5_load_task(
                job3_meta: Dict[str, any],
                job4_meta: Dict[str, any],
                job1_meta: Dict[str, any],
            ) -> Dict[str, any]:
                """Job 5: Load master, fact and dimensional tables (dim_*, fact_taxi_trips, master_taxi_trips)."""
                batch_run_id = uuid.UUID(job1_meta["batch_run_id"])
                raw_rows = job1_meta.get("raw_rows", 0)
                clean_rows = job3_meta.get("clean_rows", 0)
                quarantined_rows = job4_meta.get("quarantined_rows", 0)

                logging.info(
                    f"▶ [Job 5: Load Master, Fact & Dimensions] Loading tables for run {batch_run_id} "
                    f"({clean_rows:,} clean, {quarantined_rows:,} quarantined)..."
                )

                if raw_rows == 0 or clean_rows == 0:
                    logging.info("[Job 5] No clean records to promote to fact/master tables.")
                    if not job1_meta.get("skipped"):
                        record_pipeline_complete(
                            run_id=batch_run_id,
                            status="SUCCESS",
                            rows_extracted=raw_rows,
                            rows_cleansed=clean_rows,
                            rows_fact_loaded=0,
                            rows_master_loaded=0,
                            rows_quarantined=quarantined_rows,
                            duration_sec=0.1,
                            peak_memory_mb=round(psutil.Process().memory_info().rss / (1024 * 1024), 2),
                        )
                    remove_raw_parquet_file(month_str)
                    return {
                        "batch_run_id": str(batch_run_id),
                        "fact_rows": 0,
                        "master_rows": 0,
                        "clean_rows": clean_rows,
                        "quarantined_rows": quarantined_rows,
                        "status": "SUCCESS" if not job1_meta.get("skipped") else "SKIPPED_ALREADY_INGESTED",
                    }

                try:
                    res = job5_load_master_fact_and_dimensions(
                        batch_run_id=batch_run_id,
                        raw_rows=raw_rows,
                        clean_rows=clean_rows,
                        quarantined_rows=quarantined_rows,
                    )
                    # Requirement: Once ingest data then remove parquet files
                    remove_raw_parquet_file(month_str)
                    return res
                except Exception as exc:
                    logging.error(f"[Job 5] Load failed for run {batch_run_id}: {exc}")
                    rollback_failed_batch(batch_run_id, error_message=str(exc))
                    raise

            # Wire the pipeline: download >> job1 >> job2 >> job3 >> job4 >> job5
            t_down = download_task()
            t1 = job1_extract_task(t_down)
            t2 = job2_validate_task(t1)
            t3 = job3_clean_task(t2)
            t4 = job4_quarantine_task(t2)
            t5 = job5_load_task(t3, t4, t1)

            t_down >> t1 >> t2 >> t3 >> t4 >> t5

        return main_job, t5

    @task(task_id="refresh_analytics_views")
    def refresh_analytics_views_task(previous_results: Optional[List[Dict]] = None):
        """Refreshes analytical SQL views after all months and slices are processed."""
        logging.info("Refreshing analytical views in PostgreSQL...")
        apply_analytics_views(paths.analytics_queries_path)
        logging.info("Analytical SQL views refreshed successfully.")

        # Requirement 2: Overall logs for job process in Airflow
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            COALESCE(SUM(rows_extracted), 0) AS extracted,
                            COALESCE(SUM(rows_cleansed), 0) AS cleansed,
                            COALESCE(SUM(rows_fact_loaded), 0) AS fact,
                            COALESCE(SUM(rows_master_loaded), 0) AS master,
                            COALESCE(SUM(rows_quarantined), 0) AS quar,
                            COALESCE(SUM(duration_sec), 0) AS total_duration,
                            MAX(peak_memory_mb) AS peak_mem
                        FROM etl_pipeline_runs
                        WHERE completed_at >= NOW() - INTERVAL '24 hours';
                        """
                    )
                    row = cur.fetchone()
                    if row:
                        total_days = sum(
                            len(get_day_slices(int(m.split("-")[0]), int(m.split("-")[1]), settings.DAY_CHUNK_SIZE))
                            for m in settings.target_months
                        )
                        dur = float(row[5])
                        fact_cnt = int(row[2])
                        thru = round(fact_cnt / dur, 2) if dur > 0 else 0.0

                        summary = {
                            "job_id": "airflow-dag-nyc_taxi_batch_pipeline",
                            "status": "SUCCESS",
                            "target_months": settings.target_months,
                            "total_days_processed": total_days,
                            "total_rows_extracted": int(row[0]),
                            "total_rows_cleansed": int(row[1]),
                            "total_rows_fact_loaded": fact_cnt,
                            "total_rows_master_loaded": int(row[3]),
                            "total_rows_quarantined": int(row[4]),
                            "total_duration_sec": dur,
                            "overall_throughput_rows_sec": thru,
                            "peak_memory_mb": float(row[6] or 0.0),
                        }
                        log_job_summary(summary)
        except Exception as exc:
            logging.warning(f"Could not compute overall job summary: {exc}")

        return {"status": "SUCCESS"}

    # -------------------------------------------------------------------------
    # DAG Orchestration: Sequential Processing Across Target Months
    # -------------------------------------------------------------------------
    seed_stage = seed_reference_data_task()

    target_months = settings.target_months

    previous_stage = seed_stage
    month_final_tasks = []

    for month in target_months:
        main_job_group, main_job_last_task = create_main_etl_job(month)
        previous_stage >> main_job_group
        previous_stage = main_job_last_task
        month_final_tasks.append(main_job_last_task)

    views_stage = refresh_analytics_views_task()
    for final_task in month_final_tasks:
        final_task >> views_stage
