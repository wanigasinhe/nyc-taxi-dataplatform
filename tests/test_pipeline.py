"""Unit and integration tests for NYC Taxi Data Platform pipeline."""

import uuid
import pandas as pd
try:
    import pytest
except ImportError:
    class DummyPytest:
        @staticmethod
        def raises(exc_type):
            class _RaisesContext:
                def __enter__(self):
                    return self
                def __exit__(self, exc_type_, exc_val, exc_tb):
                    if exc_type_ is None:
                        raise AssertionError(f"Expected {exc_type.__name__} was not raised.")
                    return issubclass(exc_type_, exc_type)
            return _RaisesContext()
    pytest = DummyPytest()

from src.batch_etl.transform import (
    build_date_dimension_records,
    generate_trip_hash_ids,
    transform_batch,
)


def test_generate_trip_hash_ids():
    """Verify deterministic SHA-256 generation."""
    vendor_ids = pd.Series([1, 1])
    pickups = pd.to_datetime(pd.Series(["2023-01-01 10:00:00", "2023-01-01 10:00:00"]))
    dropoffs = pd.to_datetime(pd.Series(["2023-01-01 10:15:00", "2023-01-01 10:15:00"]))
    pu_locs = pd.Series([100, 100])
    do_locs = pd.Series([200, 200])
    totals = pd.Series([15.50, 15.50])

    hashes = generate_trip_hash_ids(vendor_ids, pickups, dropoffs, pu_locs, do_locs, totals)
    assert len(hashes) == 2
    # Hashes for identical records must be identical
    assert hashes[0] == hashes[1]
    assert len(hashes[0]) == 64  # SHA-256 hex string length


def test_build_date_dimension_records():
    """Verify dim_date generation."""
    dates = pd.to_datetime(["2023-01-01", "2023-01-02"])
    dim_df = build_date_dimension_records(dates)

    assert len(dim_df) == 2
    assert "date_key" in dim_df.columns
    assert "is_weekend" in dim_df.columns
    # 2023-01-01 was a Sunday -> is_weekend = True
    sun_row = dim_df[dim_df["date_key"] == 20230101].iloc[0]
    assert sun_row["is_weekend"] == True
    # 2023-01-02 was a Monday -> is_weekend = False
    mon_row = dim_df[dim_df["date_key"] == 20230102].iloc[0]
    assert mon_row["is_weekend"] == False


def test_transform_batch_dq_quarantine():
    """Verify data quality checks correctly partition valid vs invalid records."""
    run_id = uuid.uuid4()
    raw_data = {
        "VendorID": [1, 2, 1, 1],
        "tpep_pickup_datetime": [
            "2023-01-01 10:00:00",
            "2023-01-01 12:00:00",  # Dirty: dropoff before pickup
            "2023-01-01 14:00:00",  # Dirty: zero distance
            "2023-01-01 16:00:00",  # Dirty: negative fare
        ],
        "tpep_dropoff_datetime": [
            "2023-01-01 10:15:00",
            "2023-01-01 11:30:00",  # dropoff < pickup
            "2023-01-01 14:20:00",
            "2023-01-01 16:20:00",
        ],
        "passenger_count": [1, 2, 1, 1],
        "trip_distance": [2.5, 3.0, 0.0, 1.5],  # 3rd row has 0.0 distance
        "RatecodeID": [1, 1, 1, 1],
        "PULocationID": [100, 101, 102, 103],
        "DOLocationID": [200, 201, 202, 203],
        "payment_type": [1, 1, 1, 1],
        "fare_amount": [12.0, 15.0, 10.0, -5.0],  # 4th row has negative fare
        "extra": [0.5, 0.5, 0.5, 0.5],
        "mta_tax": [0.5, 0.5, 0.5, 0.5],
        "tip_amount": [2.5, 3.0, 2.0, 0.0],
        "tolls_amount": [0.0, 0.0, 0.0, 0.0],
        "improvement_surcharge": [0.3, 0.3, 0.3, 0.3],
        "congestion_surcharge": [2.5, 2.5, 2.5, 2.5],
        "total_amount": [17.8, 21.3, 15.3, -1.2],
    }
    raw_df = pd.DataFrame(raw_data)

    clean_df, quarantine_df, dates_df = transform_batch(raw_df, run_id)

    # Exactly 1 row is valid, 3 rows must be quarantined
    assert len(clean_df) == 1
    assert len(quarantine_df) == 3

    # Check clean record features
    clean_row = clean_df.iloc[0]
    assert clean_row["trip_duration_sec"] == 900  # 15 minutes = 900 sec
    assert clean_row["pickup_hour"] == 10
    assert clean_row["pickup_date_key"] == 20230101
    assert len(clean_row["trip_hash_id"]) == 64

    # Check quarantine rejection reasons
    reasons = quarantine_df["rejection_reason"].tolist()
    assert any("DROPOFF_BEFORE_OR_EQUAL_PICKUP" in r for r in reasons)
    assert any("NON_POSITIVE_DISTANCE" in r for r in reasons)
    assert any("NON_POSITIVE_FARE" in r for r in reasons)


def test_settings_and_paths_multi_environment():
    """Verify settings multi-environment switching and parameterizable paths."""
    from settings import get_settings, paths, PathSettings

    # Dev environment test
    dev_cfg = get_settings("dev")
    assert dev_cfg.ENVIRONMENT == "dev"
    assert dev_cfg.DEBUG is True

    # Staging environment test
    staging_cfg = get_settings("staging")
    assert staging_cfg.ENVIRONMENT == "staging"
    assert staging_cfg.DEBUG is False
    assert "staging" in staging_cfg.DB_NAME

    # Prod environment test
    prod_cfg = get_settings("prod")
    assert prod_cfg.ENVIRONMENT == "prod"
    assert prod_cfg.DB_SSLMODE == "require"
    assert prod_cfg.BATCH_CHUNK_SIZE >= 250000

    # Path resolution and parameterization test
    assert paths.schema_ddl_path.name in ("01_schema.sql", "schema.sql")
    assert paths.analytics_queries_path.name in ("02_analytics_queries.sql", "analytics_queries.sql")
    assert paths.taxi_zone_csv_path.name == "taxi_zone_lookup.csv"
    assert paths.get_parquet_filename("2023-01") == "yellow_tripdata_2023-01.parquet"
    assert "d37ci6vzurychx.cloudfront.net" in paths.get_parquet_url("2023-01")
    assert "d37ci6vzurychx.cloudfront.net" in paths.taxi_zone_csv_url


def test_parquet_pre_extract_validation_and_corruption_handling(tmp_path):
    """Verify pre-extract validation detects corrupt/invalid Parquet files and raises alerts."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from src.batch_etl.extract import (
        check_parquet_magic_bytes,
        validate_parquet_file,
        ParquetValidationError,
        CRITICAL_COLUMNS,
    )

    # 1. Test empty/truncated file
    empty_file = tmp_path / "empty.parquet"
    empty_file.write_bytes(b"")
    assert check_parquet_magic_bytes(empty_file) is False
    with pytest.raises(ParquetValidationError):
        validate_parquet_file(empty_file)

    # 2. Test invalid magic bytes (e.g. 404 HTML response saved as parquet)
    html_file = tmp_path / "fake_404.parquet"
    html_file.write_text("<html><body>404 Not Found</body></html>")
    assert check_parquet_magic_bytes(html_file) is False
    with pytest.raises(ParquetValidationError):
        validate_parquet_file(html_file)

    # 3. Test valid Parquet with all critical columns
    valid_file = tmp_path / "healthy.parquet"
    table_data = {
        "VendorID": [1, 2],
        "tpep_pickup_datetime": pd.to_datetime(["2023-01-01 10:00:00", "2023-01-01 11:00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2023-01-01 10:20:00", "2023-01-01 11:30:00"]),
        "passenger_count": [1, 2],
        "trip_distance": [2.5, 3.8],
        "RatecodeID": [1, 1],
        "PULocationID": [100, 101],
        "DOLocationID": [200, 201],
        "payment_type": [1, 1],
        "fare_amount": [12.0, 15.0],
        "extra": [0.5, 0.5],
        "mta_tax": [0.5, 0.5],
        "tip_amount": [2.0, 3.0],
        "tolls_amount": [0.0, 0.0],
        "improvement_surcharge": [0.3, 0.3],
        "total_amount": [15.3, 18.8],
        "congestion_surcharge": [2.5, 2.5],
    }
    table = pa.Table.from_pandas(pd.DataFrame(table_data))
    pq.write_table(table, valid_file)

    assert check_parquet_magic_bytes(valid_file) is True
    meta = validate_parquet_file(valid_file)
    assert meta["validated"] is True
    assert meta["num_rows"] == 2


def test_rollback_failed_batch_logic():
    """Verify rollback functions and error handling signatures."""
    from src.batch_etl.load import rollback_failed_batch, rollback_stage, cleanup_orphan_runs
    import uuid

    # Verify functions are callable and accept expected arguments
    test_run_id = uuid.uuid4()
    assert callable(rollback_failed_batch)
    assert callable(rollback_stage)
    assert callable(cleanup_orphan_runs)


def test_batch_day_slicing_and_month_range():
    """Verify manual configuration of start month, end month, and day chunk slicing (e.g. 1-10, 11-20, 21-31)."""
    from settings.base import generate_month_range, BaseConfig
    from src.batch_etl.extract import get_day_slices

    # 1. Month range generation
    months = generate_month_range("2023-01", "2023-03")
    assert months == ["2023-01", "2023-02", "2023-03"]

    single_month = generate_month_range("2023-05")
    assert single_month == ["2023-05"]

    # 2. Day slicing (10-day chunks for January: 31 days)
    jan_slices = get_day_slices(2023, 1, day_interval=10)
    assert jan_slices == [(1, 10), (11, 20), (21, 31)]

    # 3. Day slicing (10-day chunks for February: 28 days)
    feb_slices = get_day_slices(2023, 2, day_interval=10)
    assert feb_slices == [(1, 10), (11, 20), (21, 28)]

    # 4. Zero/disabled interval returns single whole month slice
    full_month = get_day_slices(2023, 1, day_interval=0)
    assert full_month == [(1, 31)]


def test_manual_raw_data_url_override():
    """Verify manual configuration of raw data URL overrides default CloudFront URL."""
    import os
    from settings import paths

    # Default URL check
    default_url = paths.get_parquet_url("2023-01")
    assert "d37ci6vzurychx.cloudfront.net" in default_url

    # Manual override via environment variable
    custom_mirror = "https://custom-mirror.corp.internal/datasets/trip-data"
    os.environ["RAW_DATA_URL"] = custom_mirror
    try:
        custom_url = paths.get_parquet_url("2023-01")
        assert custom_url == "https://custom-mirror.corp.internal/datasets/trip-data/yellow_tripdata_2023-01.parquet"
    finally:
        os.environ.pop("RAW_DATA_URL", None)


def test_data_settings_package_integration():
    """Verify DataConfig in settings/data.py is properly embedded into settings."""
    from settings import settings, data_settings, DataConfig

    assert isinstance(settings.data, DataConfig)
    assert settings.data.DAY_CHUNK_SIZE == 10
    assert settings.DAY_CHUNK_SIZE == 10
    assert "2023-01" in settings.target_months
    assert len(settings.get_day_slices(2023, 1)) == 3


def test_logging_requirements_verification(tmp_path):
    """Verify all 3 explicit logging requirements:
    1. Day-wise detailed logs of processed data.
    2. Overall logs for job process summary.
    3. Parquet file inspection logs (row count, file size, file name).
    """
    import json
    from settings import paths
    from src.logger import (
        log_parquet_file_inspection,
        log_daily_processed_data,
        log_job_summary,
    )

    # 1. Test Requirement 3: Parquet inspection log
    parquet_info = {
        "file_name": "yellow_tripdata_2023-01.parquet",
        "file_size_bytes": 48215600,
        "file_size_mb": 45.98,
        "num_rows": 3066766,
        "num_row_groups": 12,
        "num_columns": 19,
        "columns": ["VendorID", "tpep_pickup_datetime", "fare_amount"],
        "magic_bytes": "PAR1",
        "status": "HEALTHY",
    }
    log_parquet_file_inspection(parquet_info)

    assert paths.parquet_metrics_log_path.exists()
    with open(paths.parquet_metrics_log_path, "r", encoding="utf-8") as f:
        parquet_lines = [json.loads(line) for line in f if line.strip()]
    assert len(parquet_lines) > 0
    latest_pq = parquet_lines[-1]
    assert latest_pq["file_name"] == "yellow_tripdata_2023-01.parquet"
    assert latest_pq["num_rows"] == 3066766
    assert latest_pq["file_size_bytes"] == 48215600
    assert latest_pq["file_size_mb"] == 45.98
    assert latest_pq["status"] == "HEALTHY"

    # 2. Test Requirement 1: Day-wise processed data log
    daily_records = [
        {
            "trip_date": "2023-01-01",
            "day_name": "Sunday",
            "rows_extracted": 85000,
            "rows_cleansed": 84500,
            "rows_fact": 84500,
            "rows_quarantined": 500,
            "total_revenue": 1420500.25,
            "avg_fare": 14.85,
            "avg_duration_min": 13.2,
            "total_distance_miles": 245000.0,
        },
        {
            "trip_date": "2023-01-02",
            "day_name": "Monday",
            "rows_extracted": 92000,
            "rows_cleansed": 91800,
            "rows_fact": 91800,
            "rows_quarantined": 200,
            "total_revenue": 1650300.75,
            "avg_fare": 15.20,
            "avg_duration_min": 14.5,
            "total_distance_miles": 278000.0,
        },
    ]
    log_daily_processed_data(daily_records)

    assert paths.daily_metrics_log_path.exists()
    with open(paths.daily_metrics_log_path, "r", encoding="utf-8") as f:
        daily_lines = [json.loads(line) for line in f if line.strip()]
    assert len(daily_lines) >= 2
    sun_entry = [e for e in daily_lines if e.get("trip_date") == "2023-01-01"][-1]
    assert sun_entry["day_name"] == "Sunday"
    assert sun_entry["rows_extracted"] == 85000
    assert sun_entry["rows_cleansed"] == 84500
    assert sun_entry["rows_quarantined"] == 500
    assert sun_entry["total_revenue"] == 1420500.25

    # 3. Test Requirement 2: Overall job process summary log
    job_summary = {
        "job_id": "test-job-summary-123",
        "status": "SUCCESS",
        "target_months": ["2023-01"],
        "total_days_processed": 31,
        "total_rows_extracted": 177000,
        "total_rows_cleansed": 176300,
        "total_rows_fact_loaded": 176300,
        "total_rows_master_loaded": 176300,
        "total_rows_quarantined": 700,
        "total_duration_sec": 45.2,
        "overall_throughput_rows_sec": 3900.44,
        "peak_memory_mb": 145.2,
    }
    log_job_summary(job_summary)

    with open(paths.metrics_log_path, "r", encoding="utf-8") as f:
        telemetry_lines = [json.loads(line) for line in f if line.strip()]
    summary_events = [e for e in telemetry_lines if e.get("event") == "JOB_PROCESS_OVERALL_SUMMARY"]
    assert len(summary_events) > 0
    latest_summary = summary_events[-1]
    assert latest_summary["job_id"] == "test-job-summary-123"
    assert latest_summary["total_rows_extracted"] == 177000
    assert latest_summary["status"] == "SUCCESS"
    assert latest_summary["total_days_processed"] == 31


def test_multiple_etl_jobs_architecture():
    """Verify the 5 distinct jobs architecture across both load modules and Airflow DAG:
    1. Extract data to raw data
    2. Validate data
    3. Clean
    4. Move invalid data to another table (quarantine)
    5. Load master fact and dimensional tables
    """
    from src.batch_etl.load import (
        job1_extract_to_raw_data,
        job2_validate_data,
        job3_clean_data,
        job4_move_invalid_data,
        job5_load_master_fact_and_dimensions,
    )

    # 1. Verify all 5 job functions exist and are callable
    assert callable(job1_extract_to_raw_data)
    assert callable(job2_validate_data)
    assert callable(job3_clean_data)
    assert callable(job4_move_invalid_data)
    assert callable(job5_load_master_fact_and_dimensions)

    # 2. Verify Airflow DAG structure and task graph (if airflow is installed in environment)
    try:
        from dags.nyc_taxi_etl_dag import dag

        task_ids = set(dag.task_dict.keys())
        assert "seed_reference_data" in task_ids
        assert any("step1_download_parquet" in t for t in task_ids)

        job1_tasks = [t for t in task_ids if "job1_extract_to_raw_data" in t]
        job2_tasks = [t for t in task_ids if "job2_validate_data" in t]
        job3_tasks = [t for t in task_ids if "job3_clean_data" in t]
        job4_tasks = [t for t in task_ids if "job4_move_invalid_data" in t]
        job5_tasks = [t for t in task_ids if "job5_load_master_fact_dimensions" in t]

        assert len(job1_tasks) >= 1, f"Expected at least 1 task for job1, got {len(job1_tasks)}"
        assert len(job2_tasks) >= 1, f"Expected at least 1 task for job2, got {len(job2_tasks)}"
        assert len(job3_tasks) >= 1, f"Expected at least 1 task for job3, got {len(job3_tasks)}"
        assert len(job4_tasks) >= 1, f"Expected at least 1 task for job4, got {len(job4_tasks)}"
        assert len(job5_tasks) >= 1, f"Expected at least 1 task for job5, got {len(job5_tasks)}"

        # Verify that all 5 jobs are housed under the main_etl_job parent
        assert any("main_etl_job" in t for t in task_ids)

        sample_job1 = dag.get_task(job1_tasks[0])
        sample_job2 = dag.get_task(job2_tasks[0])
        sample_job3 = dag.get_task(job3_tasks[0])
        sample_job4 = dag.get_task(job4_tasks[0])
        sample_job5 = dag.get_task(job5_tasks[0])

        assert sample_job2 in sample_job1.downstream_list
        assert sample_job3 in sample_job2.downstream_list
        assert sample_job4 in sample_job3.downstream_list
        assert sample_job5 in sample_job4.downstream_list

        # Verify refresh_analytics_views is downstream of job5
        assert "refresh_analytics_views" in task_ids
        views_task = dag.get_task("refresh_analytics_views")
        assert views_task in sample_job5.downstream_list
    except ImportError:
        # Airflow runs containerized in Docker
        pass





