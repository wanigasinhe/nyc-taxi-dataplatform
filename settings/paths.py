"""Centralized and parameterizable file and folder path configurations.

All project paths, directories, filenames, and data source URLs are defined here
and can be overridden via environment variables for dev, staging, and prod environments.
No paths should be hardcoded elsewhere in the codebase.
"""

from pathlib import Path
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PathSettings(BaseSettings):
    """Parameterizable file, folder, and URL path management."""

    # --------------------------------------------------------------------------
    # Base Directories
    # --------------------------------------------------------------------------
    PROJECT_ROOT: Path = Field(
        default_factory=lambda: Path(__file__).resolve().parent.parent,
        description="Root directory of the project repository.",
    )
    DATA_ROOT_DIR: Path = Field(
        default=Path("data"),
        description="Base data directory.",
    )
    RAW_DATA_DIR: Path = Field(
        default=Path("data/raw"),
        description="Directory for raw incoming datasets (Parquet, CSV).",
    )
    PROCESSED_DATA_DIR: Path = Field(
        default=Path("data/processed"),
        description="Directory for intermediate/transformed data.",
    )
    STAGING_DATA_DIR: Path = Field(
        default=Path("data/staging"),
        description="Directory for temporary staging batches.",
    )
    LOG_DIR: Path = Field(
        default=Path("logs"),
        description="Directory for application and JSONL telemetry logs.",
    )
    SQL_DIR: Path = Field(
        default=Path("sql"),
        description="Directory containing SQL DDL schemas and analytical views.",
    )

    # --------------------------------------------------------------------------
    # SQL File Names & Artifacts
    # --------------------------------------------------------------------------
    SCHEMA_DDL_FILENAME: str = Field(
        default="01_schema.sql",
        description="PostgreSQL Star Schema DDL file name.",
    )
    ANALYTICS_QUERIES_FILENAME: str = Field(
        default="02_analytics_queries.sql",
        description="Analytical views and reporting queries file name.",
    )

    # --------------------------------------------------------------------------
    # Log File Names
    # --------------------------------------------------------------------------
    METRICS_LOG_FILENAME: str = Field(
        default="etl_metrics.jsonl",
        description="Machine-parseable JSON Lines execution telemetry log file.",
    )
    DAILY_LOG_FILENAME: str = Field(
        default="daily_processed_metrics.jsonl",
        description="Machine-parseable JSON Lines daily processed data log file.",
    )
    PARQUET_LOG_FILENAME: str = Field(
        default="parquet_file_metrics.jsonl",
        description="Machine-parseable JSON Lines parquet file inspection log file.",
    )
    APP_LOG_FILENAME: str = Field(
        default="app.log",
        description="Application console log file.",
    )

    # --------------------------------------------------------------------------
    # NYC TLC Dataset URLs & File Templates
    # --------------------------------------------------------------------------
    RAW_DATA_URL: Optional[str] = Field(
        default=None,
        description="Manual override for raw data URL (direct parquet file or base directory).",
    )
    TLC_TRIP_DATA_BASE_URL: str = Field(
        default="https://d37ci6vzurychx.cloudfront.net/trip-data",
        description="Base remote CloudFront URL for NYC TLC monthly trip Parquet files.",
    )
    TLC_MISC_BASE_URL: str = Field(
        default="https://d37ci6vzurychx.cloudfront.net/misc",
        description="Base remote CloudFront URL for reference tables (e.g. taxi zones).",
    )
    TAXI_ZONE_LOOKUP_FILENAME: str = Field(
        default="taxi_zone_lookup.csv",
        description="Filename for the official TLC Taxi Zone lookup CSV.",
    )
    YELLOW_TRIPDATA_FILENAME_TEMPLATE: str = Field(
        default="yellow_tripdata_{month}.parquet",
        description="String template for monthly Yellow Taxi Parquet files.",
    )

    model_config = SettingsConfigDict(
        env_prefix="PATH_",
        extra="ignore",
    )

    def resolve_paths(self):
        """Resolves relative paths against PROJECT_ROOT and creates required directories."""
        root = self.PROJECT_ROOT.resolve()

        if not self.DATA_ROOT_DIR.is_absolute():
            self.DATA_ROOT_DIR = (root / self.DATA_ROOT_DIR).resolve()
        if not self.RAW_DATA_DIR.is_absolute():
            self.RAW_DATA_DIR = (root / self.RAW_DATA_DIR).resolve()
        if not self.PROCESSED_DATA_DIR.is_absolute():
            self.PROCESSED_DATA_DIR = (root / self.PROCESSED_DATA_DIR).resolve()
        if not self.STAGING_DATA_DIR.is_absolute():
            self.STAGING_DATA_DIR = (root / self.STAGING_DATA_DIR).resolve()
        if not self.LOG_DIR.is_absolute():
            self.LOG_DIR = (root / self.LOG_DIR).resolve()
        if not self.SQL_DIR.is_absolute():
            self.SQL_DIR = (root / self.SQL_DIR).resolve()

        # Ensure essential directories exist
        self.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.STAGING_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.SQL_DIR.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------------------------------
    # Path Properties & Accessor Helpers
    # --------------------------------------------------------------------------
    @property
    def schema_ddl_path(self) -> Path:
        """Absolute path to the Star Schema DDL file."""
        candidate = self.SQL_DIR / "schema.sql"
        if candidate.exists():
            return candidate
        return self.SQL_DIR / self.SCHEMA_DDL_FILENAME

    @property
    def analytics_queries_path(self) -> Path:
        """Absolute path to the analytical views SQL file."""
        candidate = self.SQL_DIR / "analytics_queries.sql"
        if candidate.exists():
            return candidate
        return self.SQL_DIR / self.ANALYTICS_QUERIES_FILENAME

    @property
    def metrics_log_path(self) -> Path:
        """Absolute path to the JSONL metrics log file."""
        return self.LOG_DIR / self.METRICS_LOG_FILENAME

    @property
    def daily_metrics_log_path(self) -> Path:
        """Absolute path to the day-wise processed metrics JSONL file."""
        return self.LOG_DIR / self.DAILY_LOG_FILENAME

    @property
    def parquet_metrics_log_path(self) -> Path:
        """Absolute path to the parquet inspection metrics JSONL file."""
        return self.LOG_DIR / self.PARQUET_LOG_FILENAME

    @property
    def app_log_path(self) -> Path:
        """Absolute path to the standard application log file."""
        return self.LOG_DIR / self.APP_LOG_FILENAME

    @property
    def taxi_zone_csv_path(self) -> Path:
        """Absolute local path to the taxi zone lookup CSV."""
        return self.RAW_DATA_DIR / self.TAXI_ZONE_LOOKUP_FILENAME

    @property
    def taxi_zone_csv_url(self) -> str:
        """Remote download URL for the taxi zone lookup CSV."""
        return f"{self.TLC_MISC_BASE_URL}/{self.TAXI_ZONE_LOOKUP_FILENAME}"

    def get_parquet_filename(self, month: str) -> str:
        """Returns the formatted parquet filename for a given year-month (e.g. '2023-01')."""
        return self.YELLOW_TRIPDATA_FILENAME_TEMPLATE.format(month=month)

    def get_parquet_path(self, month: str) -> Path:
        """Returns the absolute local path for a monthly parquet file."""
        return self.RAW_DATA_DIR / self.get_parquet_filename(month)

    def get_parquet_url(self, month: str) -> str:
        """Returns the remote download URL for a monthly parquet file, with manual overrides."""
        import os

        filename = self.get_parquet_filename(month)
        custom_url = (
            self.RAW_DATA_URL
            or os.getenv("RAW_DATA_URL")
            or os.getenv("RAW_DATA_BASE_URL")
            or os.getenv("PATH_RAW_DATA_URL")
            or os.getenv("PATH_TLC_TRIP_DATA_BASE_URL")
            or self.TLC_TRIP_DATA_BASE_URL
        )

        if custom_url.endswith(".parquet"):
            return custom_url
        return f"{custom_url.rstrip('/')}/{filename}"


# Instantiate and resolve default paths
paths = PathSettings()
paths.resolve_paths()
