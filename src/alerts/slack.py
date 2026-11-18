"""Slack Alerting & Notification Module for the NYC Taxi Data Platform.

Provides enterprise-grade Slack notifications for all platform events:
- Critical Pipeline & Task Failures (Airflow on_failure_callback & Flow exceptions)
- Task Retries & Auto-Healing Warnings (Airflow on_retry_callback)
- Data Quality & Dead-Letter Quarantine Alerts (Exceeded thresholds or invalid records)
- SLA Breach Warnings (Airflow sla_miss_callback)
- Overall Batch Process Execution Summaries (Success and throughput reports)
- Platform Infrastructure & Service Degradation Alerts
"""

from datetime import datetime, timezone
import json
import logging
import os
import platform
import socket
import sys
from typing import Any, Dict, List, Optional, Tuple
import requests

try:
    from settings import settings
except ImportError:
    settings = None

logger = logging.getLogger("nyc_taxi_pipeline.alerts.slack")


def get_slack_config() -> Dict[str, Any]:
    """Retrieves Slack configuration dynamically from settings or environment variables."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    channel = os.getenv("SLACK_CHANNEL", "#data-pipeline-alerts")
    enabled = os.getenv("SLACK_ALERTS_ENABLED", "true").lower() in ("true", "1", "yes")
    alert_on_start = os.getenv("SLACK_ALERT_ON_START", "true").lower() in ("true", "1", "yes")
    alert_on_failure = os.getenv("SLACK_ALERT_ON_FAILURE", "true").lower() in ("true", "1", "yes")
    alert_on_retry = os.getenv("SLACK_ALERT_ON_RETRY", "true").lower() in ("true", "1", "yes")
    alert_on_success = os.getenv("SLACK_ALERT_ON_SUCCESS", "true").lower() in ("true", "1", "yes")
    alert_on_sla_miss = os.getenv("SLACK_ALERT_ON_SLA_MISS", "true").lower() in ("true", "1", "yes")
    alert_on_quarantine = os.getenv("SLACK_ALERT_ON_QUARANTINE", "true").lower() in ("true", "1", "yes")
    quarantine_threshold = float(os.getenv("SLACK_QUARANTINE_ALERT_THRESHOLD_PCT", "5.0"))

    if settings is not None:
        webhook_url = getattr(settings, "SLACK_WEBHOOK_URL", webhook_url) or webhook_url
        channel = getattr(settings, "SLACK_CHANNEL", channel) or channel
        enabled = getattr(settings, "SLACK_ALERTS_ENABLED", enabled)
        alert_on_start = getattr(settings, "SLACK_ALERT_ON_START", alert_on_start)
        alert_on_failure = getattr(settings, "SLACK_ALERT_ON_FAILURE", alert_on_failure)
        alert_on_retry = getattr(settings, "SLACK_ALERT_ON_RETRY", alert_on_retry)
        alert_on_success = getattr(settings, "SLACK_ALERT_ON_SUCCESS", alert_on_success)
        alert_on_sla_miss = getattr(settings, "SLACK_ALERT_ON_SLA_MISS", alert_on_sla_miss)
        alert_on_quarantine = getattr(settings, "SLACK_ALERT_ON_QUARANTINE", alert_on_quarantine)
        quarantine_threshold = getattr(settings, "SLACK_QUARANTINE_ALERT_THRESHOLD_PCT", quarantine_threshold)

    return {
        "webhook_url": (webhook_url or "").strip(),
        "channel": channel,
        "enabled": enabled,
        "alert_on_start": alert_on_start,
        "alert_on_failure": alert_on_failure,
        "alert_on_retry": alert_on_retry,
        "alert_on_success": alert_on_success,
        "alert_on_sla_miss": alert_on_sla_miss,
        "alert_on_quarantine": alert_on_quarantine,
        "quarantine_threshold": quarantine_threshold,
    }


def is_slack_configured() -> bool:
    """Checks whether a valid Slack webhook URL is currently configured."""
    cfg = get_slack_config()
    url = cfg["webhook_url"]
    return bool(url and url.startswith("https://hooks.slack.com/services/") and "YOUR" not in url)


def send_slack_alert(
    title: str,
    message: str,
    level: str = "INFO",
    fields: Optional[Dict[str, str]] = None,
    blocks: Optional[List[Dict[str, Any]]] = None,
    custom_webhook: Optional[str] = None,
) -> Tuple[bool, str]:
    """Sends a formatted message to Slack with colored sidebars and key-value fields.
    
    If the webhook is not configured or in dry-run mode, logs the alert cleanly
    without throwing exceptions.
    """
    cfg = get_slack_config()
    if not cfg["enabled"] and not custom_webhook:
        logger.debug(f"[Slack Alert] Alerts disabled globally via SLACK_ALERTS_ENABLED.")
        return True, "ALERTS_DISABLED"

    webhook_url = custom_webhook if custom_webhook is not None else cfg["webhook_url"]

    # Color mapping for Slack attachments
    color_map = {
        "CRITICAL": "#E01E5A",   # Red
        "ERROR": "#E01E5A",      # Red
        "FAILURE": "#E01E5A",    # Red
        "WARNING": "#ECB22E",    # Amber/Yellow
        "RETRY": "#ECB22E",      # Amber/Yellow
        "SUCCESS": "#2EB67D",    # Green
        "HEALTHY": "#2EB67D",    # Green
        "DATA_QUALITY": "#8A2BE2", # Purple
        "QUARANTINE": "#8A2BE2",   # Purple
        "SLA_MISS": "#E01E5A",   # Red
        "INFO": "#1D9BD1",       # Blue
    }
    color = color_map.get(level.upper(), "#1D9BD1")

    # Build fallback text
    fallback_text = f"[{level.upper()}] {title}: {message}"

    # Build fields list for Slack attachments
    slack_fields = []
    if fields:
        for k, v in fields.items():
            slack_fields.append({"title": k, "value": str(v), "short": True})

    # Default metadata fields
    env_name = getattr(settings, "ENVIRONMENT", os.getenv("APP_ENV", "dev")) if settings else "dev"
    hostname = socket.gethostname()
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    payload = {
        "channel": cfg["channel"],
        "text": fallback_text,
        "attachments": [
            {
                "color": color,
                "title": title,
                "text": message,
                "fields": slack_fields,
                "footer": f"NYC Taxi Data Platform • Environment: {env_name.upper()} • Host: {hostname}",
                "ts": int(datetime.now(timezone.utc).timestamp()),
            }
        ],
    }

    if blocks:
        payload["blocks"] = blocks

    # Dry-run logging if no live webhook is configured
    if not webhook_url or not webhook_url.startswith("https://hooks.slack.com/services/") or "YOUR" in webhook_url:
        logger.info(
            f"📢 [SLACK DRY-RUN ALERT] [{level.upper()}] {title} | Channel: {cfg['channel']}\n"
            f"Message: {message}\n"
            f"Fields: {json.dumps(fields or {}, default=str)}"
        )
        return True, "DRY_RUN: Webhook not configured"

    # Live POST request to Slack Incoming Webhook
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=6.0,
        )
        if response.status_code == 200:
            logger.info(f"✔ [Slack Alert Sent] [{level.upper()}] {title}")
            return True, "SUCCESS"
        else:
            err_msg = f"Slack webhook returned HTTP {response.status_code}: {response.text}"
            logger.warning(f"✖ [Slack Alert Failed] {err_msg}")
            return False, err_msg
    except Exception as exc:
        err_msg = f"Slack alert dispatch encountered network exception: {exc}"
        logger.warning(f"✖ [Slack Alert Exception] {err_msg}")
        return False, err_msg


# ==============================================================================
# Specialized Alert Handlers (Covering All Event Kinds)
# ==============================================================================

def send_task_failure_alert(context: Dict[str, Any]) -> Tuple[bool, str]:
    """Triggered by Airflow on_failure_callback or unhandled pipeline exceptions.
    
    Extracts DAG ID, Task ID, execution timestamp, try number, and exception stack trace.
    """
    cfg = get_slack_config()
    if not cfg["alert_on_failure"]:
        return True, "FAILURE_ALERTS_DISABLED"

    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else context.get("task_id", "unknown_task")
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else context.get("dag_id", "nyc_taxi_batch_pipeline")
    execution_date = str(context.get("execution_date", datetime.now(timezone.utc)))
    exception = str(context.get("exception", context.get("error", "No exception message provided")))
    try_number = getattr(task_instance, "try_number", context.get("try_number", 1))
    max_tries = getattr(task_instance, "max_tries", context.get("max_tries", 3))

    title = f"🚨 [CRITICAL ALERT] Task Failure in Airflow: {dag_id}.{task_id}"
    message = (
        f"A critical error caused task *`{task_id}`* to fail in DAG *`{dag_id}`*.\n"
        f"Automatic retry policy or quarantine isolation will be evaluated."
    )

    log_url = f"http://localhost:8080/dags/{dag_id}/grid"

    fields = {
        "DAG ID": dag_id,
        "Task ID": task_id,
        "Attempt": f"{try_number} of {max_tries}",
        "Execution Date": execution_date[:19],
        "Error Summary": exception[:250] + ("..." if len(exception) > 250 else ""),
        "Airflow Log URL": log_url,
    }

    return send_slack_alert(title=title, message=message, level="CRITICAL", fields=fields)


def send_task_retry_alert(context: Dict[str, Any]) -> Tuple[bool, str]:
    """Triggered by Airflow on_retry_callback when a task fails and enters retry backoff."""
    cfg = get_slack_config()
    if not cfg["alert_on_retry"]:
        return True, "RETRY_ALERTS_DISABLED"

    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else context.get("task_id", "unknown_task")
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else context.get("dag_id", "nyc_taxi_batch_pipeline")
    try_number = getattr(task_instance, "try_number", context.get("try_number", 1))
    max_tries = getattr(task_instance, "max_tries", context.get("max_tries", 3))
    exception = str(context.get("exception", "Transient failure detected"))

    title = f"⚠️ [RETRY WARNING] Task Retry & Auto-Healing: {dag_id}.{task_id}"
    message = (
        f"Task *`{task_id}`* in DAG *`{dag_id}`* failed attempt {try_number} of {max_tries} "
        f"and is initiating exponential backoff retry."
    )

    fields = {
        "DAG ID": dag_id,
        "Task ID": task_id,
        "Next Attempt": f"{try_number + 1} of {max_tries}",
        "Error Cause": exception[:200] + ("..." if len(exception) > 200 else ""),
        "Healing Action": "Retrying with exponential backoff",
    }

    return send_slack_alert(title=title, message=message, level="RETRY", fields=fields)


def send_sla_miss_alert(
    dag: Any,
    task_list: str,
    blocking_task_list: str,
    slas: List[Any],
    blocking_tis: List[Any],
) -> Tuple[bool, str]:
    """Triggered by Airflow sla_miss_callback when a task exceeds its SLA execution window."""
    cfg = get_slack_config()
    if not cfg["alert_on_sla_miss"]:
        return True, "SLA_ALERTS_DISABLED"

    dag_id = getattr(dag, "dag_id", "nyc_taxi_batch_pipeline")
    title = f"⏰ [SLA BREACH ALERT] Pipeline SLA Missed: {dag_id}"
    message = (
        f"Execution SLA missed in DAG *`{dag_id}`*.\n"
        f"Tasks impacted: `{task_list}`"
    )

    fields = {
        "DAG ID": dag_id,
        "Impacted Tasks": str(task_list),
        "Blocking Tasks": str(blocking_task_list or "None"),
        "Breach Time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }

    return send_slack_alert(title=title, message=message, level="SLA_MISS", fields=fields)


def send_data_quality_alert(
    batch_run_id: str,
    source_file: str,
    quarantined_count: int,
    total_count: int,
    rejection_reasons: Optional[Dict[str, int]] = None,
) -> Tuple[bool, str]:
    """Triggered when invalid records are routed to quarantine_taxi_trips table."""
    cfg = get_slack_config()
    if not cfg["alert_on_quarantine"]:
        return True, "QUARANTINE_ALERTS_DISABLED"

    quar_pct = round((quarantined_count / total_count * 100), 2) if total_count > 0 else 0.0
    threshold = cfg["quarantine_threshold"]

    is_severe = quar_pct > threshold
    level = "WARNING" if is_severe else "DATA_QUALITY"
    title_prefix = "🚨 [DQ THRESHOLD BREACH]" if is_severe else "🛡️ [DATA QUALITY QUARANTINE]"

    title = f"{title_prefix} {quarantined_count:,} Invalid Records Isolated"
    message = (
        f"The Data Quality validation engine filtered and quarantined *{quarantined_count:,} records* "
        f"({quar_pct}% of batch) into table *`quarantine_taxi_trips`*.\n"
        f"All invalid rows preserved with raw JSONB audit payload."
    )

    reasons_str = "Invalid timestamps, negative fares, or outlier duration"
    if rejection_reasons:
        reasons_str = ", ".join(f"{k} ({v:,})" for k, v in list(rejection_reasons.items())[:4])

    fields = {
        "Batch Run ID": str(batch_run_id)[:18] + "...",
        "Source File": str(source_file),
        "Quarantined Records": f"{quarantined_count:,} rows",
        "Quarantine Rate": f"{quar_pct}% (Threshold: {threshold}%)",
        "Rejection Breakdown": reasons_str,
        "Storage Destination": "public.quarantine_taxi_trips",
    }

    return send_slack_alert(title=title, message=message, level=level, fields=fields)


def send_pipeline_start_alert(
    job_id: str,
    target_months: Any,
    timeframe: Optional[Tuple[str, str]] = None,
    execution_engine: str = "Apache Airflow",
) -> Tuple[bool, str]:
    """Triggered when an ETL batch pipeline starts execution."""
    cfg = get_slack_config()
    if not cfg["alert_on_start"]:
        return True, "START_ALERTS_DISABLED"

    start_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    title = f"🚀 [PIPELINE STARTED] NYC Taxi Batch Pipeline Initiated"
    message = (
        f"Batch ETL execution *`{job_id}`* has been initiated via *{execution_engine}*.\n"
        f"Processing configured timeframe with automated data validation, 5-stage ingestion, and auto-healing."
    )

    fields = {
        "Job / Run ID": str(job_id),
        "Engine": execution_engine,
        "Target Months": str(target_months),
        "Timeframe": f"{timeframe[0]} ➔ {timeframe[1]}" if timeframe else "Standard Range",
        "Started At": start_time,
        "Status": "RUNNING",
        "Dashboard": "http://localhost:8501",
        "Airflow UI": "http://localhost:8080",
    }

    return send_slack_alert(title=title, message=message, level="INFO", fields=fields)


def send_job_start_alert(
    month: str,
    file_name: str,
    timeframe: Optional[Tuple[str, str]] = None,
    job_name: str = "Main ETL Job",
) -> Tuple[bool, str]:
    """Triggered when an individual month's ingestion job begins."""
    cfg = get_slack_config()
    if not cfg["alert_on_start"]:
        return True, "START_ALERTS_DISABLED"

    start_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    title = f"▶️ [JOB STARTED] Ingestion Job Started: {month}"
    message = (
        f"Sub-job *`{job_name}`* started for month *`{month}`* (`{file_name}`).\n"
        f"Extracting raw data, running 5-rule data quality checks, cleansing Silver records, and resolving dimensions."
    )

    fields = {
        "Job Name": job_name,
        "Month": month,
        "Source File": file_name,
        "Timeframe": f"{timeframe[0]} ➔ {timeframe[1]}" if timeframe else month,
        "Started At": start_time,
        "Status": "IN_PROGRESS",
    }

    return send_slack_alert(title=title, message=message, level="INFO", fields=fields)


def send_pipeline_summary_alert(summary: Dict[str, Any]) -> Tuple[bool, str]:
    """Triggered when an entire ETL batch pipeline execution completes successfully."""
    cfg = get_slack_config()
    if not cfg["alert_on_success"]:
        return True, "SUCCESS_ALERTS_DISABLED"

    job_id = str(summary.get("job_id") or summary.get("run_id") or "nyc-taxi-batch-job")
    target_months = summary.get("target_months") or summary.get("months_processed") or "N/A"
    extracted = int(summary.get("total_rows_extracted") or summary.get("rows_extracted") or 0)
    cleansed = int(summary.get("total_rows_cleansed") or summary.get("rows_cleansed") or 0)
    fact_loaded = int(summary.get("total_rows_fact_loaded") or summary.get("rows_fact") or 0)
    master_loaded = int(summary.get("total_rows_master_loaded") or summary.get("rows_master") or 0)
    quarantined = int(summary.get("total_rows_quarantined") or summary.get("rows_quarantined") or 0)
    duration = float(summary.get("total_duration_sec") or summary.get("duration_sec") or 0.0)
    throughput = float(summary.get("overall_throughput_rows_sec") or summary.get("throughput") or 0.0)
    peak_mem = float(summary.get("peak_memory_mb") or 0.0)

    dq_pct = round((fact_loaded / extracted * 100), 2) if extracted > 0 else 100.0

    title = f"✅ [PIPELINE SUCCESS] NYC Taxi Batch Pipeline Completed"
    message = (
        f"Batch ETL execution *`{job_id}`* completed all 5 Medallion ingestion stages successfully. "
        f"Analytical views refreshed in PostgreSQL."
    )

    fields = {
        "Job / Run ID": job_id,
        "Target Months": str(target_months),
        "Rows Extracted (Bronze)": f"{extracted:,}",
        "Rows Cleansed (Silver)": f"{cleansed:,}",
        "Fact Loaded (Gold)": f"{fact_loaded:,}",
        "Master Loaded (Gold)": f"{master_loaded:,}",
        "Quarantined Records": f"{quarantined:,} (DQ Pass: {dq_pct}%)",
        "Total Duration": f"{duration:.2f} seconds",
        "Throughput": f"{throughput:,.1f} rows/sec",
        "Peak Memory": f"{peak_mem:.1f} MB",
    }

    return send_slack_alert(title=title, message=message, level="SUCCESS", fields=fields)


def send_infrastructure_alert(
    service_name: str,
    status: str,
    port: int,
    details: Optional[str] = None,
) -> Tuple[bool, str]:
    """Triggered when platform infrastructure health checks detect a degraded service."""
    is_down = status.upper() in ("OFFLINE", "DOWN", "FAILED", "ERROR")
    level = "CRITICAL" if is_down else "HEALTHY"
    icon = "🚨" if is_down else "🟢"

    title = f"{icon} [INFRASTRUCTURE ALERT] {service_name} is {status.upper()}"
    message = (
        f"Service *`{service_name}`* on port *`{port}`* reported status: *`{status.upper()}`*.\n"
        f"Details: {details or 'Automated health probe connection status update.'}"
    )

    fields = {
        "Service": service_name,
        "Port": str(port),
        "Status": status.upper(),
        "Detected At": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }

    return send_slack_alert(title=title, message=message, level=level, fields=fields)


def test_slack_connection(webhook_url: Optional[str] = None) -> Tuple[bool, str]:
    """Sends an immediate verification test notification to Slack."""
    title = "🧪 [TEST NOTIFICATION] NYC Taxi Data Platform Slack Alert System"
    message = (
        "Congratulations! Your Slack Alerting Webhook is **successfully connected**.\n"
        "You will receive automated alerts for:\n"
        "• 🚨 Pipeline & Task Failures\n"
        "• ⚠️ Auto-Healing & Task Retries\n"
        "• 🛡️ Data Quality & Quarantine Rate Breaches\n"
        "• ⏰ Pipeline SLA Misses\n"
        "• ✅ Batch Job Execution Summaries\n"
        "• 🖥️ Infrastructure Health Degenerations"
    )

    fields = {
        "Platform": "NYC Taxi Batch Data Platform",
        "Alert System": "Slack Block Kit / Webhook",
        "Environment": os.getenv("APP_ENV", "dev").upper(),
        "Test Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "Host Node": socket.gethostname(),
        "Python Runtime": platform.python_version(),
    }

    return send_slack_alert(
        title=title,
        message=message,
        level="INFO",
        fields=fields,
        custom_webhook=webhook_url,
    )


# ==============================================================================
# CLI Entry Point
# ==============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NYC Taxi Data Platform Slack Alert CLI")
    parser.add_argument("--test", action="store_true", help="Send a test alert to verify webhook connectivity")
    parser.add_argument("--webhook", type=str, default=None, help="Custom Slack Webhook URL to test")
    parser.add_argument("--message", type=str, default=None, help="Custom message text to send")
    parser.add_argument("--level", type=str, default="INFO", help="Alert severity level (INFO, WARNING, CRITICAL, SUCCESS)")

    args = parser.parse_args()

    if args.test:
        print(f"Triggering test Slack alert (Webhook: {args.webhook or 'from config'})...")
        ok, response_msg = test_slack_connection(args.webhook)
        print(f"Result: {'SUCCESS ✔' if ok else 'FAILED ✖'} -> {response_msg}")
        sys.exit(0 if ok else 1)
    elif args.message:
        ok, response_msg = send_slack_alert(
            title=f"Manual Slack Alert ({args.level.upper()})",
            message=args.message,
            level=args.level,
            custom_webhook=args.webhook,
        )
        print(f"Result: {'SUCCESS ✔' if ok else 'FAILED ✖'} -> {response_msg}")
        sys.exit(0 if ok else 1)
    else:
        cfg = get_slack_config()
        configured = is_slack_configured()
        print("NYC Taxi Data Platform — Slack Alert Configuration:")
        print(f"  Webhook Configured: {'YES ✔' if configured else 'NO ✖ (Dry-run mode)'}")
        print(f"  Channel:            {cfg['channel']}")
        print(f"  Alerts Enabled:     {cfg['enabled']}")
        print(f"  Alert on Failure:   {cfg['alert_on_failure']}")
        print(f"  Alert on Retry:     {cfg['alert_on_retry']}")
        print(f"  Alert on Success:   {cfg['alert_on_success']}")
        print(f"  Alert on Quarantine:{cfg['alert_on_quarantine']} (Threshold: {cfg['quarantine_threshold']}%)")
        print("\nRun with --test to trigger a verification notification.")
