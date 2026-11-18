"""Prefect / Python Orchestration Flow for the 5-Step NYC Yellow Taxi Batch Data Platform.

5-Step Progression per Batch Slice:
1. Download file to data/raw/ with automatic corruption healing and pre-extract validation.
2. Extract data to a table (raw_taxi_trips) - Bronze staging layer.
3. Move valid data to next table cleansing (clean_taxi_trips & quarantine_taxi_trips) - Silver layer.
4. Move to next table fact (fact_taxi_trips) - Star Schema with dimensional foreign keys.
5. Load master table (master_taxi_trips) - Denormalized Gold analytical table.

Configurable Batch Chunking:
- RAW_DATA_URL: Configurable remote data URL (CloudFront, S3, or direct .parquet file).
- START_MONTH / END_MONTH: Configurable range of months for disaster recovery or cold-start.
- START_DATE / END_DATE: Exact date range filtering.
- DAY_CHUNK_SIZE: Configurable day interval (e.g. 10 = Extract days 1 to 10 then finish, then 11 to 20 then finish).
- BATCH_CHUNK_SIZE: Number of rows per PyArrow streaming chunk.
"""

import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional
import psutil
from prefect import flow, task

from settings import paths, settings
from src.alerts.slack import (
    send_pipeline_start_alert,
    send_job_start_alert,
    send_pipeline_summary_alert,
)
from src.logger import (
    logger,
    log_metric,
    track_performance,
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


def on_pipeline_failure(flow_obj, flow_run, state):
    """Failure hook executed when a flow run encounters an unhandled exception."""
    err_msg = str(state.result()) if state else "Unknown flow error"
    logger.critical(f"🚨 [FLOW CRITICAL ALERT] Flow {flow_obj.name} (run_id: {flow_run.id}) failed: {err_msg}")
    log_metric("flow_failure_hook_triggered", {"flow_run_id": str(flow_run.id), "error": err_msg})
    try:
        from src.alerts.slack import send_task_failure_alert
        send_task_failure_alert({
            "dag_id": flow_obj.name,
            "task_id": "flow_execution",
            "execution_date": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "exception": err_msg,
        })
    except Exception as exc:
        logger.warning(f"Could not send flow failure alert to Slack: {exc}")


@task(name="seed_reference_data", retries=2, retry_delay_seconds=5)
def seed_reference_data_task():
    """Step 0: Initializes schema DDL and seeds static dimensions including taxi zones."""
    logger.info("Initializing database schema and reference dimensions...")
    apply_schema_migrations(paths.schema_ddl_path)
    zone_csv = download_taxi_zone_lookup(paths.taxi_zone_csv_path)
    seed_taxi_zones(zone_csv)
    logger.info("Reference data seeding complete.")


@task(name="step1_download_and_verify", retries=3, retry_delay_seconds=10)
def step1_download_and_verify_task(month_str: str) -> Dict[str, any]:
    """Step 1: Check if already ingested; if not, check availability & download if missing."""
    if is_parquet_already_ingested(month_str):
        logger.info(
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
    logger.info(
        f"[Step 1: Timeframe Check] Considering timeframe: {start_date} to {end_date} (Month: {month_str}). Checking raw file availability..."
    )
    file_path = ensure_raw_file_for_timeframe(
        month_str=month_str,
        start_date=start_date,
        end_date=end_date,
    )

    try:
        metadata = validate_parquet_file(file_path)
        logger.info(
            f"✔ [Step 1] File {file_path.name} verified healthy for timeframe [{start_date} to {end_date}] ({metadata['num_rows']:,} rows)."
        )
    except ParquetValidationError as err:
        logger.error(
            f"[Step 1] Pre-extract validation failed for {file_path.name}: {err}. Initiating auto-healing download..."
        )
        file_path = ensure_raw_file_for_timeframe(
            month_str=month_str,
            start_date=start_date,
            end_date=end_date,
            force_download=True,
        )
        metadata = validate_parquet_file(file_path)
        logger.info(f"✔ [Step 1] Auto-healing succeeded. File {file_path.name} verified.")

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


def execute_staged_slice(
    source_file: str,
    file_path: Path,
    slice_desc: str,
    start_day: Optional[int],
    end_day: Optional[int],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    max_restart_attempts: int = 2,
) -> Dict[str, any]:
    """Executes the full 5-stage pipeline for a single batch slice (e.g. Days 1 to 10), then finishes."""
    process = psutil.Process()

    for attempt in range(1, max_restart_attempts + 1):
        batch_run_id = uuid.uuid4()
        start_time = time.perf_counter()
        logger.info(
            f"Processing Slice: {slice_desc} (Attempt {attempt}/{max_restart_attempts}, Run ID: {batch_run_id})"
        )
        record_pipeline_start(batch_run_id, slice_desc)

        total_raw = 0
        total_cleansed = 0
        total_quarantined = 0
        total_fact = 0
        total_master = 0
        peak_mem_mb = process.memory_info().rss / (1024 * 1024)

        try:
            with get_db_connection() as conn:
                # -------------------------------------------------------------
                # Job 1: Extract data to raw data (raw_taxi_trips - Bronze)
                # -------------------------------------------------------------
                job1_res = job1_extract_to_raw_data(
                    file_path=file_path,
                    source_file=source_file,
                    slice_desc=slice_desc,
                    start_day=start_day,
                    end_day=end_day,
                    start_date=start_date,
                    end_date=end_date,
                    batch_run_id=batch_run_id,
                    conn=conn,
                )
                total_raw = job1_res.get("raw_rows", 0)

                if total_raw == 0:
                    logger.warning(f"Slice {slice_desc} had 0 matching records. Skipping downstream.")
                    record_pipeline_complete(
                        run_id=batch_run_id,
                        status="SUCCESS",
                        rows_extracted=0,
                        rows_cleansed=0,
                        rows_fact_loaded=0,
                        rows_master_loaded=0,
                        rows_quarantined=0,
                        duration_sec=0.1,
                        peak_memory_mb=round(peak_mem_mb, 2),
                    )
                    return {"run_id": str(batch_run_id), "status": "SUCCESS", "rows_fact": 0}

                # -------------------------------------------------------------
                # Job 2: Validate data (DQ Rules & Anomaly Scoring)
                # -------------------------------------------------------------
                job2_res = job2_validate_data(batch_run_id, conn=conn)

                # -------------------------------------------------------------
                # Job 3: Clean (Standardization, Hashing & clean_taxi_trips - Silver)
                # -------------------------------------------------------------
                job3_res = job3_clean_data(batch_run_id, conn=conn)
                total_cleansed = job3_res.get("clean_rows", 0)

                # -------------------------------------------------------------
                # Job 4: Move invalid data to another table (quarantine_taxi_trips)
                # -------------------------------------------------------------
                job4_res = job4_move_invalid_data(batch_run_id, conn=conn)
                total_quarantined = job4_res.get("quarantined_rows", 0)

                # -------------------------------------------------------------
                # Job 5: Load master, fact and dimensional tables (Gold & Star Schema)
                # -------------------------------------------------------------
                job5_res = job5_load_master_fact_and_dimensions(
                    batch_run_id=batch_run_id,
                    raw_rows=total_raw,
                    clean_rows=total_cleansed,
                    quarantined_rows=total_quarantined,
                    conn=conn,
                )
                total_fact = job5_res.get("fact_rows", 0)
                total_master = job5_res.get("master_rows", 0)

            duration_sec = round(time.perf_counter() - start_time, 2)
            throughput = round(total_fact / duration_sec, 2) if duration_sec > 0 else 0.0

            logger.info(
                f"✔ COMPLETED Slice [{slice_desc}]: {total_fact:,} fact rows, {total_master:,} master rows "
                f"in {duration_sec}s ({throughput:,} rows/sec). Process finished for this batch."
            )

            return {
                "run_id": str(batch_run_id),
                "slice": slice_desc,
                "status": "SUCCESS",
                "rows_raw": total_raw,
                "rows_cleansed": total_cleansed,
                "rows_fact": total_fact,
                "rows_master": total_master,
                "rows_quarantined": total_quarantined,
                "duration_sec": duration_sec,
            }

        except Exception as exc:
            duration_sec = round(time.perf_counter() - start_time, 2)
            logger.error(
                f"Error during ingestion of {slice_desc}: {exc}. Triggering automated database rollback..."
            )
            rollback_stats = rollback_failed_batch(batch_run_id, error_message=str(exc))
            log_metric(
                "pipeline_failure_rollback",
                {
                    "slice": slice_desc,
                    "run_id": str(batch_run_id),
                    "attempt": attempt,
                    "deleted_master": rollback_stats.get("deleted_master", 0),
                    "deleted_fact": rollback_stats.get("deleted_fact", 0),
                    "deleted_clean": rollback_stats.get("deleted_clean", 0),
                    "deleted_quarantine": rollback_stats.get("deleted_quarantine", 0),
                    "deleted_raw": rollback_stats.get("deleted_raw", 0),
                    "error": str(exc),
                },
                level=logging.WARNING,
            )

            if attempt < max_restart_attempts:
                logger.warning(
                    f"Rollback complete. Auto-healing restart for {slice_desc} (Attempt {attempt + 1}/{max_restart_attempts}) in 5s..."
                )
                time.sleep(5)
                continue
            else:
                logger.critical(f"🚨 All local restart attempts exhausted for {slice_desc}.")
                raise


def execute_staged_month_pipeline(
    month_str: str, file_meta: Dict[str, any], max_restart_attempts: int = 2
) -> List[Dict[str, any]]:
    """Slices a monthly dataset into configured batch chunks (e.g. 1 to 10 then finish, then 11 to 20 then finish)."""
    source_file = file_meta["file_name"]
    file_path = Path(file_meta["file_path"])

    if file_meta.get("skipped"):
        logger.info(f"⏩ [Month Pipeline] Skipping {source_file} (already ingested into warehouse).")
        return []

    # Pre-clean orphan residues
    cleanup_orphan_runs(source_file)

    # Parse year & month to compute day slices
    year = int(month_str.split("-")[0])
    month = int(month_str.split("-")[1])
    day_slices = get_day_slices(year, month, settings.DAY_CHUNK_SIZE)
    logger.info(
        f"Processing {source_file} in {len(day_slices)} batch slices (DAY_CHUNK_SIZE={settings.DAY_CHUNK_SIZE}): {day_slices}"
    )

    slice_results = []
    for start_day, end_day in day_slices:
        slice_desc = f"{source_file} [Days {start_day:02d}-{end_day:02d}]"
        logger.info(f"▶ Starting batch chunk: {slice_desc}")

        res = execute_staged_slice(
            source_file=source_file,
            file_path=file_path,
            slice_desc=slice_desc,
            start_day=start_day,
            end_day=end_day,
            start_date=settings.START_DATE,
            end_date=settings.END_DATE,
            max_restart_attempts=max_restart_attempts,
        )
        slice_results.append(res)
        logger.info(f"⏹ Finished batch chunk: {slice_desc}. Moving to next chunk.")

    return slice_results


@task(name="execute_month_pipeline", retries=2, retry_delay_seconds=10)
def execute_month_task(month_str: str, file_meta: Dict[str, any]) -> List[Dict[str, any]]:
    """Prefect task wrapping the staged month pipeline."""
    return execute_staged_month_pipeline(month_str, file_meta)


@flow(name="nyc-taxi-batch-etl", log_prints=True, on_failure=[on_pipeline_failure])
def run_batch_pipeline(months: Optional[List[str]] = None) -> List[Dict[str, any]]:
    """Master batch orchestration flow for NYC Taxi platform.

    Supports:
    - Custom START_MONTH and END_MONTH
    - Custom START_DATE and END_DATE
    - Configurable DAY_CHUNK_SIZE (e.g. 1-10 then finish, 11-20 then finish)
    - Configurable RAW_DATA_URL
    """
    flow_start_wall = time.perf_counter()
    overall_start, overall_end = settings.data.get_overall_timeframe()
    target_months = months or settings.target_months

    logger.info(
        f"Starting NYC Taxi Data Platform Pipeline | Timeframe: {overall_start} to {overall_end} | "
        f"Months: {target_months} | Day Slices: {settings.DAY_CHUNK_SIZE} days | Row Batch Size: {settings.BATCH_CHUNK_SIZE:,} rows | "
        f"Raw Data URL: {paths.RAW_DATA_URL or paths.TLC_TRIP_DATA_BASE_URL}"
    )

    try:
        send_pipeline_start_alert(
            job_id=f"nyc-taxi-flow-{int(time.time())}",
            target_months=target_months,
            timeframe=(overall_start, overall_end),
            execution_engine="Direct Python Runner",
        )
    except Exception as exc:
        logger.warning(f"Could not dispatch pipeline start alert to Slack: {exc}")

    # Step 0: Bootstrap reference and dimension tables (idempotent / cold start)
    seed_reference_data_task()

    # Steps 1 to 5: Sequential monthly and slice processing
    all_results = []
    for month_str in target_months:
        if is_parquet_already_ingested(month_str):
            logger.info(
                f"⏩ [Idempotency] Month {month_str} ('yellow_tripdata_{month_str}.parquet') is ALREADY INGESTED into the warehouse. Skipping download and ingestion."
            )
            remove_raw_parquet_file(month_str)
            continue

        start_date, end_date = settings.data.get_timeframe_for_month(month_str)
        try:
            send_job_start_alert(
                month=month_str,
                file_name=paths.get_parquet_filename(month_str),
                timeframe=(start_date, end_date),
                job_name=f"main_etl_job_{month_str}",
            )
        except Exception as exc:
            logger.warning(f"Could not dispatch job start alert to Slack: {exc}")

        file_meta = step1_download_and_verify_task(month_str)
        month_stats = execute_month_task(month_str, file_meta)
        all_results.extend(month_stats)

        # Requirement: Once ingest data then remove parquet files
        remove_raw_parquet_file(month_str)

    # Refresh analytical views
    logger.info("Refreshing analytical reporting views...")
    apply_analytics_views(paths.analytics_queries_path)

    # Requirement 2: Overall logs for job process
    flow_duration = round(time.perf_counter() - flow_start_wall, 2)
    total_raw_all = sum(r.get("rows_raw", 0) for r in all_results)
    total_cleansed_all = sum(r.get("rows_cleansed", 0) for r in all_results)
    total_fact_all = sum(r.get("rows_fact", 0) for r in all_results)
    total_master_all = sum(r.get("rows_master", 0) for r in all_results)
    total_quarantined_all = sum(r.get("rows_quarantined", 0) for r in all_results)
    throughput_all = round(total_fact_all / flow_duration, 2) if flow_duration > 0 else 0.0
    peak_mem_all = round(psutil.Process().memory_info().rss / (1024 * 1024), 2)

    total_days = sum(
        len(get_day_slices(int(m.split("-")[0]), int(m.split("-")[1]), settings.DAY_CHUNK_SIZE))
        for m in target_months
    )

    job_summary = {
        "job_id": f"nyc-taxi-flow-{int(time.time())}",
        "status": "SUCCESS",
        "target_months": target_months,
        "total_days_processed": total_days,
        "total_rows_extracted": total_raw_all,
        "total_rows_cleansed": total_cleansed_all,
        "total_rows_fact_loaded": total_fact_all,
        "total_rows_master_loaded": total_master_all,
        "total_rows_quarantined": total_quarantined_all,
        "total_duration_sec": flow_duration,
        "overall_throughput_rows_sec": throughput_all,
        "peak_memory_mb": peak_mem_all,
    }
    log_job_summary(job_summary)

    logger.info("✔ NYC Taxi Batch Pipeline successfully processed all configured slices and months.")
    return all_results


if __name__ == "__main__":
    run_batch_pipeline()
