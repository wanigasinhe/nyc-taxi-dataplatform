"""Development environment configuration."""

from pydantic import Field
from pydantic_settings import SettingsConfigDict
from settings.base import BaseConfig
from settings.data import DataConfig


class DevConfig(BaseConfig):
    """Configuration overrides for the local development environment."""

    ENVIRONMENT: str = "dev"
    DEBUG: bool = True
    LOG_LEVEL: str = Field(default="DEBUG", alias="LOG_LEVEL")

    # Embedded Data Ingestion Configuration for Dev
    data: DataConfig = Field(
        default_factory=lambda: DataConfig(_env_file="settings/envs/dev.env")
    )

    # Development-specific database settings
    DB_HOST: str = Field(default="localhost", alias="DB_HOST")
    DB_PORT: int = Field(default=5432, alias="DB_PORT")
    DB_NAME: str = Field(default="nyc_taxi", alias="DB_NAME")
    DB_USER: str = Field(default="postgres", alias="DB_USER")
    DB_PASSWORD: str = Field(default="postgres", alias="DB_PASSWORD")
    DB_SSLMODE: str = Field(default="disable", alias="DB_SSLMODE")

    model_config = SettingsConfigDict(
        env_file="settings/envs/dev.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
