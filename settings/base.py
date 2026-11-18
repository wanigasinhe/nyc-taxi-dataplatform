"""Base settings configuration shared across dev, staging, and prod environments."""

from typing import List, Optional, Tuple, Union
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from settings.paths import PathSettings, paths
from settings.data import DataConfig, data_settings, generate_month_range


class BaseConfig(BaseSettings):
    """Base application and pipeline configuration."""

    # Environment
    ENVIRONMENT: str = Field(default="base", description="Deployment environment: dev, staging, or prod.")
    APP_ENV: str = Field(default="dev", alias="APP_ENV", description="Active application environment.")
    DEBUG: bool = Field(default=False, description="Enable debug logging and diagnostics.")

    # Database Settings
    DB_HOST: str = Field(default="localhost", alias="DB_HOST")
    DB_PORT: int = Field(default=5432, alias="DB_PORT")
    DB_NAME: str = Field(default="nyc_taxi", alias="DB_NAME")
    DB_USER: str = Field(default="postgres", alias="DB_USER")
    DB_PASSWORD: str = Field(default="postgres", alias="DB_PASSWORD")
    DB_SSLMODE: str = Field(default="disable", alias="DB_SSLMODE")
    DB_POOL_SIZE: int = Field(default=10, alias="DB_POOL_SIZE")

    # Logging
    LOG_LEVEL: str = Field(default="INFO", alias="LOG_LEVEL")

    # Embedded Sub-Configurations (Paths & Data Ingestion)
    paths: PathSettings = Field(default_factory=lambda: paths)
    data: DataConfig = Field(default_factory=lambda: data_settings)

    # General Pipeline Orchestration
    MAX_RETRIES: int = Field(default=2)
    RETRY_DELAY_SECONDS: int = Field(default=10)

    # Slack Alerting Configuration
    SLACK_WEBHOOK_URL: Optional[str] = Field(default=None, alias="SLACK_WEBHOOK_URL")
    SLACK_CHANNEL: str = Field(default="#data-pipeline-alerts", alias="SLACK_CHANNEL")
    SLACK_ALERTS_ENABLED: bool = Field(default=True, alias="SLACK_ALERTS_ENABLED")
    SLACK_ALERT_ON_START: bool = Field(default=True, alias="SLACK_ALERT_ON_START")
    SLACK_ALERT_ON_FAILURE: bool = Field(default=True, alias="SLACK_ALERT_ON_FAILURE")
    SLACK_ALERT_ON_RETRY: bool = Field(default=True, alias="SLACK_ALERT_ON_RETRY")
    SLACK_ALERT_ON_SUCCESS: bool = Field(default=True, alias="SLACK_ALERT_ON_SUCCESS")
    SLACK_ALERT_ON_SLA_MISS: bool = Field(default=True, alias="SLACK_ALERT_ON_SLA_MISS")
    SLACK_ALERT_ON_QUARANTINE: bool = Field(default=True, alias="SLACK_ALERT_ON_QUARANTINE")
    SLACK_QUARANTINE_ALERT_THRESHOLD_PCT: float = Field(default=5.0, alias="SLACK_QUARANTINE_ALERT_THRESHOLD_PCT")

    model_config = SettingsConfigDict(
        env_file="settings/envs/dev.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --------------------------------------------------------------------------
    # Delegated Data Configuration Accessors (Backward Compatibility & Ease of Use)
    # --------------------------------------------------------------------------
    @property
    def RAW_DATA_URL(self) -> Optional[str]:
        return self.data.RAW_DATA_URL

    @property
    def START_MONTH(self) -> Optional[str]:
        return self.data.START_MONTH

    @property
    def END_MONTH(self) -> Optional[str]:
        return self.data.END_MONTH

    @property
    def START_DATE(self) -> Optional[str]:
        return self.data.START_DATE

    @property
    def END_DATE(self) -> Optional[str]:
        return self.data.END_DATE

    @property
    def DAY_CHUNK_SIZE(self) -> int:
        return self.data.DAY_CHUNK_SIZE

    @property
    def BATCH_CHUNK_SIZE(self) -> int:
        return self.data.BATCH_CHUNK_SIZE

    @property
    def DATA_MONTHS(self) -> List[str]:
        return self.data.target_months

    @property
    def target_months(self) -> List[str]:
        return self.data.target_months

    def get_timeframe_for_month(self, month_str: str) -> Tuple[str, str]:
        return self.data.get_timeframe_for_month(month_str)

    def get_overall_timeframe(self) -> Tuple[str, str]:
        return self.data.get_overall_timeframe()

    def get_day_slices(self, year: int, month: int, day_interval: Optional[int] = None) -> List[Tuple[int, int]]:
        return self.data.get_day_slices(year, month, day_interval)

    def get_parquet_url(self, month: str) -> str:
        return self.data.get_parquet_url(month)

    # --------------------------------------------------------------------------
    # Database Connection Strings
    # --------------------------------------------------------------------------
    @property
    def psycopg_conninfo(self) -> str:
        """PostgreSQL connection string for psycopg3."""
        conn_str = (
            f"host={self.DB_HOST} "
            f"port={self.DB_PORT} "
            f"dbname={self.DB_NAME} "
            f"user={self.DB_USER} "
            f"password={self.DB_PASSWORD} "
            f"sslmode={self.DB_SSLMODE}"
        )
        return conn_str

    @property
    def sqlalchemy_url(self) -> str:
        """SQLAlchemy database connection URL."""
        return (
            f"postgresql+psycopg://{self.DB_USER}:{self.DB_PASSWORD}@"
            f"{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )
