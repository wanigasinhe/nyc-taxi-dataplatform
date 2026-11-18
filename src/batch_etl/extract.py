"""Extraction module: stream-reads Parquet metadata, batch chunks, pre-extract validation & auto-healing."""

from pathlib import Path
from typing import Dict, Iterator, List, Optional
import logging
import time
import urllib.request
import uuid
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

from settings import paths, settings
from src.logger import log_metric, log_parquet_file_inspection, logger, track_performance

# Required columns in NYC Yellow Taxi parquet files
EXPECTED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]

CRITICAL_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "total_amount",
]


class ParquetValidationError(Exception):
    """Raised when a Parquet file fails integrity, magic bytes, or schema checks."""

    pass


def check_parquet_magic_bytes(file_path: Path) -> bool:
    """Verifies that the file starts and ends with standard Parquet magic bytes 'PAR1'."""
    if not file_path.exists():
        return False
    size = file_path.stat().st_size
    if size < 8:
        return False
    with open(file_path, "rb") as f:
        header = f.read(4)
        f.seek(-4, 2)
        footer = f.read(4)
    return header == b"PAR1" and footer == b"PAR1"


def download_taxi_zone_lookup(target_path: Optional[Path] = None) -> Path:
    """Ensures taxi_zone_lookup.csv is present locally in data/raw/."""
    dest = target_path or paths.taxi_zone_csv_path
    url = paths.taxi_zone_csv_url
    if not dest.exists():
        logger.info(f"Downloading taxi zone lookup from {url} to {dest}...")
        urllib.request.urlretrieve(url, dest)
        logger.info("Taxi zone lookup downloaded successfully.")
    return dest


@track_performance("validate_parquet_integrity")
def validate_parquet_file(file_path: Path) -> Dict[str, any]:
    """Pre-extraction integrity check: validates magic bytes, metadata, row counts, and schema.

    Triggers structured alerts and logs if invalid data or file corruption is detected.
    """
    if not file_path.exists():
        raise ParquetValidationError(f"Parquet file does not exist: {file_path}")

    file_size = file_path.stat().st_size
    file_size_mb = round(file_size / (1024 * 1024), 2)

    # 1. Size check (at least 1KB required for valid parquet structure)
    if file_size < 1024:
        err_msg = f"File {file_path.name} is truncated or empty ({file_size} bytes)."
        log_metric(
            "parquet_validation_error",
            {"file": file_path.name, "reason": err_msg, "size_bytes": file_size},
            level=logging.ERROR,
        )
        raise ParquetValidationError(err_msg)

    # 2. Magic bytes check (PAR1 header & footer)
    if not check_parquet_magic_bytes(file_path):
        err_msg = f"File {file_path.name} failed magic bytes verification (not a valid Parquet file or corrupted transfer)."
        log_metric(
            "parquet_validation_error",
            {"file": file_path.name, "reason": err_msg},
            level=logging.ERROR,
        )
        raise ParquetValidationError(err_msg)

    # 3. Read metadata via PyArrow
    try:
        parquet_file = pq.ParquetFile(file_path)
        metadata = parquet_file.metadata
        num_rows = metadata.num_rows
        num_row_groups = metadata.num_row_groups
        schema_names = parquet_file.schema.names
    except Exception as exc:
        err_msg = f"Failed reading Parquet metadata from {file_path.name}: {exc}"
        log_metric(
            "parquet_validation_error",
            {"file": file_path.name, "reason": err_msg},
            level=logging.ERROR,
        )
        raise ParquetValidationError(err_msg) from exc

    # 4. Row count check
    if num_rows <= 0:
        err_msg = f"File {file_path.name} contains zero rows."
        log_metric(
            "parquet_validation_error",
            {"file": file_path.name, "reason": err_msg},
            level=logging.ERROR,
        )
        raise ParquetValidationError(err_msg)

    # 5. Schema check for critical columns
    missing_critical = [col for col in CRITICAL_COLUMNS if col not in schema_names]
    if missing_critical:
        err_msg = f"File {file_path.name} missing critical schema columns: {missing_critical}"
        log_metric(
            "parquet_validation_error",
            {"file": file_path.name, "reason": err_msg, "missing": missing_critical},
            level=logging.ERROR,
        )
        raise ParquetValidationError(err_msg)

    # 6. Test-read sample row group to verify decompression (Snappy/Gzip)
    try:
        sample_batch = parquet_file.read_row_group(0, columns=[CRITICAL_COLUMNS[0]])
        if len(sample_batch) == 0:
            raise ParquetValidationError(f"Sample row group read returned 0 rows for {file_path.name}")
    except Exception as exc:
        err_msg = f"Decompression or data block corruption in {file_path.name}: {exc}"
        log_metric(
            "parquet_validation_error",
            {"file": file_path.name, "reason": err_msg},
            level=logging.ERROR,
        )
        raise ParquetValidationError(err_msg) from exc

    file_info = {
        "file_name": file_path.name,
        "file_path": str(file_path),
        "num_rows": num_rows,
        "num_row_groups": num_row_groups,
        "num_columns": len(schema_names),
        "file_size_bytes": file_size,
        "file_size_mb": file_size_mb,
        "columns": schema_names,
        "magic_bytes": "PAR1",
        "status": "HEALTHY",
        "validated": True,
    }

    log_parquet_file_inspection(file_info)

    return file_info


def download_single_month_auto_heal(
    month_str: str, max_healing_retries: int = 3, force_download: bool = False
) -> Path:
    """Downloads monthly Parquet file into data/raw/ with automatic corruption healing and retries.

    If validation fails, the corrupt file is quarantined and re-downloaded fresh with exponential backoff.
    """
    dest_path = paths.get_parquet_path(month_str)
    url = paths.get_parquet_url(month_str)
    filename = paths.get_parquet_filename(month_str)

    # If file already exists and passes validation, skip re-downloading
    if dest_path.exists() and not force_download:
        try:
            validate_parquet_file(dest_path)
            logger.info(f"Existing file {dest_path.name} is healthy and ready for extraction.")
            return dest_path
        except ParquetValidationError as err:
            logger.warning(
                f"Existing file {dest_path.name} failed integrity check: {err}. Initiating auto-healing..."
            )

    for attempt in range(1, max_healing_retries + 1):
        tmp_path = dest_path.parent / f".tmp_{filename}_{uuid.uuid4().hex[:6]}"
        try:
            logger.info(
                f"Downloading {filename} (Attempt {attempt}/{max_healing_retries}) from {url}..."
            )
            # Stream download to temporary file
            urllib.request.urlretrieve(url, tmp_path)

            # Pre-move magic bytes sanity check
            if not check_parquet_magic_bytes(tmp_path):
                raise ParquetValidationError(
                    f"Temporary download {tmp_path.name} failed magic bytes header check."
                )

            # Move validated file into data/raw/
            if dest_path.exists():
                dest_path.unlink()
            tmp_path.rename(dest_path)

            # Perform comprehensive pre-extract validation
            validate_parquet_file(dest_path)
            logger.info(f"✔ Successfully downloaded and verified {filename} into data/raw/.")
            return dest_path

        except Exception as exc:
            logger.warning(
                f"Auto-healing triggered: Download/validation attempt {attempt} failed for {filename}: {exc}"
            )
            log_metric(
                "auto_healing_retry",
                {
                    "month": month_str,
                    "attempt": attempt,
                    "max_retries": max_healing_retries,
                    "error": str(exc),
                },
                level=logging.WARNING,
            )

            # Cleanup failed temp file
            if tmp_path.exists():
                tmp_path.unlink()

            # Quarantine corrupt file if present
            if dest_path.exists():
                quarantine_target = dest_path.parent / f"{dest_path.name}.corrupt_{int(time.time())}"
                dest_path.rename(quarantine_target)
                logger.warning(f"Quarantined corrupted file to {quarantine_target.name}")

            # If all attempts exhausted, trigger critical alert and raise
            if attempt == max_healing_retries:
                log_metric(
                    "auto_healing_exhausted_alert",
                    {
                        "month": month_str,
                        "attempts": max_healing_retries,
                        "error": str(exc),
                    },
                    level=logging.CRITICAL,
                )
                logger.critical(
                    f"🚨 [ALERT] Auto-healing exhausted for {filename} after {max_healing_retries} attempts!"
                )
                raise ParquetValidationError(
                    f"Auto-healing failed for {filename} after {max_healing_retries} retries: {exc}"
                ) from exc

            # Exponential backoff sleep before retry (2s, 4s, 8s...)
            backoff_sec = 2**attempt
            logger.info(f"Waiting {backoff_sec}s before auto-healing retry...")
            time.sleep(backoff_sec)

    return dest_path


def remove_raw_parquet_file(month_str: str) -> bool:
    """Removes the local raw Parquet file from data/raw/ once data ingestion is complete.

    Keeps storage usage minimal and ensures completed files are not left lingering.
    """
    dest_path = paths.get_parquet_path(month_str)
    if dest_path.exists():
        try:
            dest_path.unlink()
            logger.info(
                f"🗑️ [Cleanup] Successfully removed raw parquet file '{dest_path.name}' from data/raw/ after ingestion."
            )
            return True
        except Exception as exc:
            logger.warning(f"Could not remove raw file {dest_path.name}: {exc}")
            return False
    return False


def is_parquet_already_ingested(month_str: str, min_rows: int = 1000, conn=None) -> bool:
    """Checks whether the dataset for month_str has already been ingested into the warehouse.

    Idempotency check to prevent duplicate ingestion:
    1. Verifies etl_pipeline_runs for a SUCCESS execution with rows loaded.
    2. Verifies master_taxi_trips for significant row volume matching year & month.
    """
    try:
        from src.batch_etl.load import get_db_connection
    except ImportError:
        return False

    year, month = int(month_str.split("-")[0]), int(month_str.split("-")[1])
    filename = f"yellow_tripdata_{month_str}.parquet"

    close_conn = False
    if conn is None:
        try:
            conn = get_db_connection()
            close_conn = True
        except Exception as conn_err:
            logger.warning(f"Could not connect to database to check ingestion status: {conn_err}")
            return False

    try:
        with conn.cursor() as cur:
            # 1. Check etl_pipeline_runs for verified SUCCESS with loaded rows
            cur.execute(
                """
                SELECT 1 FROM etl_pipeline_runs
                WHERE (source_file = %s OR source_file LIKE %s)
                  AND status = 'SUCCESS'
                  AND (rows_fact_loaded >= %s OR rows_master_loaded >= %s)
                LIMIT 1;
                """,
                (filename, f"%{filename}%", min_rows, min_rows),
            )
            if cur.fetchone():
                return True

            # 2. Check master_taxi_trips volume for that year and month
            cur.execute(
                """
                SELECT 1 FROM master_taxi_trips
                WHERE pickup_year = %s AND pickup_month = %s
                LIMIT %s;
                """,
                (year, month, min_rows),
            )
            rows = cur.fetchall()
            return len(rows) >= min_rows
    except Exception as exc:
        logger.warning(f"Could not verify database ingestion status for {month_str}: {exc}")
        return False
    finally:
        if close_conn:
            try:
                conn.close()
            except Exception:
                pass


def check_raw_file_available(month_str: str) -> bool:
    """Checks whether the raw Parquet file for month_str is present in data/raw/ and passes magic bytes integrity."""
    dest_path = paths.get_parquet_path(month_str)
    if not dest_path.exists():
        return False
    return check_parquet_magic_bytes(dest_path)


def ensure_raw_file_for_timeframe(
    month_str: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    force_download: bool = False,
    max_healing_retries: int = 3,
) -> Optional[Path]:
    """Checks whether the raw Parquet file is available for the considering timeframe (start date to next end date).

    - If month is ALREADY INGESTED: skips download, cleans up local file, returns None.
    - If available: logs availability and skips downloading.
    - If missing or corrupt: downloads on-demand just-in-time for the considering timeframe.
    """
    timeframe_desc = f"{start_date} to {end_date}" if start_date and end_date else month_str

    # 1. Idempotency Check: if already ingested, do not download or re-ingest
    if not force_download and is_parquet_already_ingested(month_str):
        logger.info(
            f"⏩ [Idempotency] Month {month_str} ('yellow_tripdata_{month_str}.parquet') is ALREADY INGESTED into the warehouse. Skipping download."
        )
        remove_raw_parquet_file(month_str)
        return None

    dest_path = paths.get_parquet_path(month_str)

    # 2. Local File Check
    if dest_path.exists() and not force_download:
        if check_parquet_magic_bytes(dest_path):
            logger.info(
                f"[Check Raw File] Raw file '{dest_path.name}' is ALREADY AVAILABLE for considering timeframe [{timeframe_desc}]. Skipping download."
            )
            return dest_path
        else:
            logger.warning(
                f"[Check Raw File] Raw file '{dest_path.name}' is corrupt for timeframe [{timeframe_desc}]. Initiating auto-healing download..."
            )

    # 3. On-demand Download
    logger.info(
        f"[Check Raw File] Raw file '{dest_path.name}' is NOT available for considering timeframe [{timeframe_desc}]. Downloading on-demand..."
    )
    return download_single_month_auto_heal(
        month_str=month_str,
        max_healing_retries=max_healing_retries,
        force_download=force_download,
    )


def download_parquet_if_missing(month_str: str) -> Optional[Path]:
    """Wrapper backwards-compatible download function utilizing auto-healing."""
    return ensure_raw_file_for_timeframe(month_str)


def inspect_parquet_file(file_path: Path) -> Dict[str, any]:
    """Inspects and validates Parquet file metadata before extract."""
    return validate_parquet_file(file_path)


import calendar
from typing import Dict, Iterator, List, Optional, Tuple


def get_day_slices(
    year: int, month: int, day_interval: Optional[int] = None
) -> List[Tuple[int, int]]:
    """Calculates sequential day intervals for batch processing, delegated to settings.data."""
    return settings.data.get_day_slices(year, month, day_interval)


def stream_parquet_batches(
    file_path: Path,
    batch_size: Optional[int] = None,
    columns: Optional[List[str]] = None,
    start_day: Optional[int] = None,
    end_day: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Iterator[pd.DataFrame]:
    """Yields batches of rows as Pandas DataFrames from Parquet file using PyArrow streaming.

    Supports optional day-of-month slicing (e.g. start_day=1, end_day=10) and exact date filtering
    (e.g. start_date='2023-01-01', end_date='2023-01-10').
    Keeps memory footprint strictly bounded (under ~300-400MB) for multi-million row datasets.
    """
    chunk_size = batch_size or settings.BATCH_CHUNK_SIZE
    parquet_file = pq.ParquetFile(file_path)
    cols_to_read = columns or [c for c in EXPECTED_COLUMNS if c in parquet_file.schema.names]

    # Stream record batches directly
    batch_iterator = parquet_file.iter_batches(batch_size=chunk_size, columns=cols_to_read)
    for batch in batch_iterator:
        df = batch.to_pandas()
        if df.empty:
            continue

        # Apply date/day filtering if specified
        ts_col = "tpep_pickup_datetime"
        if ts_col in df.columns and (start_day is not None or start_date is not None or end_date is not None):
            if not pd.api.types.is_datetime64_any_dtype(df[ts_col]):
                df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")

            mask = pd.Series(True, index=df.index)

            # Slicing by day-of-month (e.g. 1 to 10)
            if start_day is not None and end_day is not None:
                mask = mask & (df[ts_col].dt.day >= start_day) & (df[ts_col].dt.day <= end_day)

            # Filtering by exact date
            if start_date:
                mask = mask & (df[ts_col] >= pd.to_datetime(start_date))
            if end_date:
                end_ts = pd.to_datetime(end_date)
                if len(end_date.strip()) == 10:
                    end_ts = end_ts + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
                mask = mask & (df[ts_col] <= end_ts)

            df = df[mask]
            if df.empty:
                continue

        yield df
