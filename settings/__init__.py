"""Settings package: dynamic multi-environment configuration loader.

Switches between dev, staging, and prod configurations automatically via the
APP_ENV or ENV environment variable (default: 'dev').
"""

import os
from functools import lru_cache
from typing import Union

from settings.paths import PathSettings, paths
from settings.data import DataConfig, data_settings, generate_month_range
from settings.base import BaseConfig
from settings.dev import DevConfig
from settings.staging import StagingConfig
from settings.prod import ProdConfig


@lru_cache(maxsize=4)
def get_settings(env_name: str = None) -> Union[DevConfig, StagingConfig, ProdConfig]:
    """Returns the appropriate settings instance based on the active environment name."""
    active_env = (
        env_name
        or os.getenv("APP_ENV")
        or os.getenv("ENV")
        or "dev"
    ).lower().strip()

    if active_env in ("prod", "production"):
        return ProdConfig()
    elif active_env in ("staging", "stage", "test"):
        return StagingConfig()
    elif active_env in ("dev", "development", "local"):
        return DevConfig()
    else:
        # Default fallback to dev
        return DevConfig()


# Global active settings and paths instances
settings = get_settings()

__all__ = [
    "settings",
    "paths",
    "data_settings",
    "get_settings",
    "PathSettings",
    "DataConfig",
    "BaseConfig",
    "DevConfig",
    "StagingConfig",
    "ProdConfig",
    "generate_month_range",
]
