"""Production environment configuration."""

from pydantic import Field
from pydantic_settings import SettingsConfigDict
from settings.base import BaseConfig
from settings.data import DataConfig


class ProdConfig(BaseConfig):
    """Configuration overrides for the live production environment."""

    ENVIRONMENT: str = "prod"
    DEBUG: bool = False
    LOG_LEVEL: str = Field(default="INFO", alias="LOG_LEVEL")

    # Embedded Data Ingestion Configuration for Production
    data: DataConfig = Field(
        default_factory=lambda: DataConfig(_env_file="settings/envs/prod.env")
    )

    # Production database settings with strict SSL and connection pooling
    DB_HOST: str = Field(default="prod-db.internal", alias="DB_HOST")
    DB_PORT: int = Field(default=5432, alias="DB_PORT")
    DB_NAME: str = Field(default="nyc_taxi_prod", alias="DB_NAME")
    DB_USER: str = Field(default="taxi_prod_app", alias="DB_USER")
    DB_PASSWORD: str = Field(default="prod_secure_password_vault", alias="DB_PASSWORD")
    DB_SSLMODE: str = Field(default="require", alias="DB_SSLMODE")
    DB_POOL_SIZE: int = Field(default=35, alias="DB_POOL_SIZE")

    # Production high-throughput settings
    MAX_RETRIES: int = Field(default=3)
    RETRY_DELAY_SECONDS: int = Field(default=30)

    model_config = SettingsConfigDict(
        env_file="settings/envs/prod.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
