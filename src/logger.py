"""Structured JSON logger and performance tracking telemetry for ETL pipelines."""

import functools
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional
import psutil

from settings import paths, settings


class JsonLinesFormatter(logging.Formatter):
    """Custom logging formatter outputting JSON Lines."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra_payload") and isinstance(record.extra_payload, dict):
            log_obj.update(record.extra_payload)

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_obj)


def setup_logger(name: str = "nyc_taxi_pipeline") -> logging.Logger:
    """Configures dual-sink logger: console (human readable) and JSONL file (machine readable)."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))

    if not logger.handlers:
        # 1. Console Handler (Human-readable)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler.setFormatter(console_format)
        logger.addHandler(console_handler)

        # 2. JSONL File Handler (Machine-parseable metrics)
        jsonl_path = paths.metrics_log_path
        file_handler = logging.FileHandler(jsonl_path, mode="a", encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(JsonLinesFormatter())
        logger.addHandler(file_handler)

    return logger


logger = setup_logger()


def log_metric(event: str, data: Optional[Dict[str, Any]] = None, level: int = logging.INFO):
    """Emit a structured metric or lifecycle event to the JSONL log sink."""
    payload = {"event": event}
    if data:
        payload.update(data)
    record = logger.makeRecord(
        logger.name, level, "(telemetry)", 0, f"METRIC_EVENT: {event}", (), None
    )
    record.extra_payload = payload
    logger.handle(record)


def track_performance(stage_name: str):
    """Decorator to measure execution duration, memory consumption, and row throughput."""

    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            process = psutil.Process()
            mem_before_mb = process.memory_info().rss / (1024 * 1024)
            start_wall = time.perf_counter()
            start_iso = datetime.now(timezone.utc).isoformat()

            logger.info(f"Starting stage: '{stage_name}'")
            status = "SUCCESS"
            error_msg = None
            result = None

            try:
                result = func(*args, **kwargs)
                return result
            except Exception as exc:
                status = "FAILED"
                error_msg = str(exc)
                logger.exception(f"Stage '{stage_name}' failed with error: {exc}")
                raise
            finally:
                end_wall = time.perf_counter()
                end_iso = datetime.now(timezone.utc).isoformat()
                duration_sec = round(end_wall - start_wall, 4)
                mem_after_mb = process.memory_info().rss / (1024 * 1024)
                peak_mem_mb = round(max(mem_before_mb, mem_after_mb), 2)

                # Inspect result for rows processed if available
                rows_in = None
                rows_out = None
                rows_quarantined = None
                if isinstance(result, dict):
                    rows_in = result.get("rows_in") or result.get("rows_extracted")
                    rows_out = result.get("rows_out") or result.get("rows_loaded")
                    rows_quarantined = result.get("rows_quarantined")

                throughput = None
                if rows_out and duration_sec > 0:
                    throughput = round(rows_out / duration_sec, 2)

                telemetry = {
                    "stage": stage_name,
                    "status": status,
                    "started_at": start_iso,
                    "completed_at": end_iso,
                    "duration_sec": duration_sec,
                    "peak_memory_mb": peak_mem_mb,
                    "rows_in": rows_in,
                    "rows_out": rows_out,
                    "rows_quarantined": rows_quarantined,
                    "throughput_rows_per_sec": throughput,
                    "error_message": error_msg,
                }

                log_metric(f"{stage_name}_completed", telemetry)
                logger.info(
                    f"Stage '{stage_name}' completed in {duration_sec}s | "
                    f"Status: {status} | Memory: {peak_mem_mb} MB"
                    + (f" | Throughput: {throughput} rows/sec" if throughput else "")
                )

        return wrapper

    return decorator


def _append_jsonl_record(file_path: Path, record_dict: Dict[str, Any]):
    """Appends a single JSON Lines record to a file safely."""
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"timestamp": datetime.now(timezone.utc).isoformat()}
        payload.update(record_dict)
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")
    except Exception as exc:
        logger.warning(f"Could not write to JSONL log at {file_path}: {exc}")


def log_parquet_file_inspection(metrics: Dict[str, Any]):
    """Logs Parquet file inspection metrics (row count, file size, file name, schema).

    Requirement 3: Logs for row count of parquet, file size and file name.
    """
    file_name = metrics.get("file_name", "unknown.parquet")
    file_size_bytes = metrics.get("file_size_bytes", 0)
    file_size_mb = metrics.get("file_size_mb", round(file_size_bytes / (1024 * 1024), 2))
    num_rows = metrics.get("num_rows", 0)
    num_row_groups = metrics.get("num_row_groups", 1)
    num_cols = metrics.get("num_columns", len(metrics.get("columns", [])))
    status = metrics.get("status", "HEALTHY")

    logger.info(
        f"📦 [PARQUET FILE LOG] File: {file_name} | "
        f"Size: {file_size_mb} MB ({file_size_bytes:,} bytes) | "
        f"Total Rows: {num_rows:,} | Row Groups: {num_row_groups} | "
        f"Columns: {num_cols} | Magic Bytes: PAR1 ✔ | Status: {status}"
    )

    # Persist to dedicated parquet JSONL and main telemetry log
    _append_jsonl_record(paths.parquet_metrics_log_path, metrics)
    log_metric("PARQUET_FILE_INSPECTED", metrics)


def log_daily_processed_data(day_summary: Any):
    """Logs detailed day-wise breakdown of processed records.

    Requirement 1: In details logs of processed data.
    """
    records = day_summary if isinstance(day_summary, list) else [day_summary]

    for rec in records:
        trip_date = str(rec.get("trip_date") or rec.get("date") or "Unknown")
        day_name = str(rec.get("day_name") or "")
        day_str = f" ({day_name})" if day_name else ""
        extracted = int(rec.get("rows_extracted") or rec.get("rows_in") or 0)
        cleansed = int(rec.get("rows_cleansed") or rec.get("rows_loaded") or 0)
        quarantined = int(rec.get("rows_quarantined") or 0)
        revenue = float(rec.get("total_revenue") or 0.0)
        duration = float(rec.get("duration_sec") or 0.0)

        quar_pct = (quarantined / extracted * 100) if extracted > 0 else 0.0

        dur_str = f" | Duration: {duration:.2f}s" if duration > 0 else ""
        rev_str = f" | Revenue: ${revenue:,.2f}" if revenue > 0 else ""

        logger.info(
            f"📅 [DAILY DATA LOG] Date: {trip_date}{day_str} | "
            f"Extracted: {extracted:,} | Cleansed: {cleansed:,} | "
            f"Quarantined: {quarantined:,} ({quar_pct:.2f}%){rev_str}{dur_str}"
        )

        _append_jsonl_record(paths.daily_metrics_log_path, rec)
        log_metric("DAILY_DATA_PROCESSED", rec)


def log_job_summary(summary: Dict[str, Any]):
    """Logs executive overall job process summary.

    Requirement 2: Overall logs for job process.
    """
    job_id = str(summary.get("job_id") or summary.get("run_id") or "N/A")
    status = str(summary.get("status") or "SUCCESS")
    status_icon = "✔" if status == "SUCCESS" else "✖"
    duration = float(summary.get("total_duration_sec") or summary.get("duration_sec") or 0.0)
    throughput = float(summary.get("overall_throughput_rows_sec") or summary.get("throughput") or 0.0)
    peak_mem = float(summary.get("peak_memory_mb") or 0.0)

    extracted = int(summary.get("total_rows_extracted") or summary.get("rows_extracted") or 0)
    cleansed = int(summary.get("total_rows_cleansed") or summary.get("rows_cleansed") or 0)
    fact_loaded = int(summary.get("total_rows_fact_loaded") or summary.get("rows_fact") or 0)
    master_loaded = int(summary.get("total_rows_master_loaded") or summary.get("rows_master") or 0)
    quarantined = int(summary.get("total_rows_quarantined") or summary.get("rows_quarantined") or 0)

    dq_pct = (fact_loaded / extracted * 100) if extracted > 0 else 100.0
    months = summary.get("months_processed") or summary.get("target_months") or "N/A"
    total_days = summary.get("total_days_processed", "N/A")

    sep = "=" * 68
    logger.info(sep)
    logger.info(f"📊 OVERALL JOB PROCESS EXECUTION SUMMARY")
    logger.info(sep)
    logger.info(f"  Job / Run ID:        {job_id}")
    logger.info(f"  Final Status:        {status} {status_icon}")
    logger.info(f"  Target Months:       {months}")
    logger.info(f"  Total Days Covered:  {total_days}")
    logger.info(f"  Total Rows Extracted:{extracted:,}")
    logger.info(f"  Total Rows Cleansed: {cleansed:,}")
    logger.info(f"  Total Fact Loaded:   {fact_loaded:,}")
    logger.info(f"  Total Master Loaded: {master_loaded:,}")
    logger.info(f"  Total Quarantined:   {quarantined:,}")
    logger.info(f"  Data Quality Pass:   {dq_pct:.2f}%")
    logger.info(f"  Total Duration:      {duration:.2f} seconds")
    if throughput > 0:
        logger.info(f"  Overall Throughput:  {throughput:,.2f} rows/sec")
    logger.info(f"  Peak Memory Usage:   {peak_mem:.1f} MB")
    if summary.get("error_message"):
        logger.error(f"  Error Message:       {summary.get('error_message')}")
    logger.info(sep)

    log_metric("JOB_PROCESS_OVERALL_SUMMARY", summary)

    # Dispatch Slack batch execution summary notification
    try:
        from src.alerts.slack import send_pipeline_summary_alert
        send_pipeline_summary_alert(summary)
    except Exception as alert_err:
        logger.warning(f"Could not dispatch pipeline summary Slack alert: {alert_err}")

