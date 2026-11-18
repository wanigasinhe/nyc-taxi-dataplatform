"""Staging environment configuration."""

from pydantic import Field
from pydantic_settings import SettingsConfigDict
from settings.base import BaseConfig
from settings.data import DataConfig


class StagingConfig(BaseConfig):
    """Configuration overrides for the staging/pre-production environment."""

    ENVIRONMENT: str = "staging"
    DEBUG: bool = False
    LOG_LEVEL: str = Field(default="INFO", alias="LOG_LEVEL")

    # Embedded Data Ingestion Configuration for Staging
    data: DataConfig = Field(
        default_factory=lambda: DataConfig(_env_file="settings/envs/staging.env")
    )

    # Staging database settings
    DB_HOST: str = Field(default="staging-db.internal", alias="DB_HOST")
    DB_PORT: int = Field(default=5432, alias="DB_PORT")
    DB_NAME: str = Field(default="nyc_taxi_staging", alias="DB_NAME")
    DB_USER: str = Field(default="taxi_staging_user", alias="DB_USER")
    DB_PASSWORD: str = Field(default="staging_secure_pass", alias="DB_PASSWORD")
    DB_SSLMODE: str = Field(default="prefer", alias="DB_SSLMODE")
    DB_POOL_SIZE: int = Field(default=20, alias="DB_POOL_SIZE")

    # Pipeline processing parameters for staging verification
    MAX_RETRIES: int = Field(default=3)
    RETRY_DELAY_SECONDS: int = Field(default=15)

    model_config = SettingsConfigDict(
        env_file="settings/envs/staging.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
