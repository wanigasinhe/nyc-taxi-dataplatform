"""Backward-compatible configuration bridge.

Directs callers to the centralized, environment-aware settings package (`settings/`).
"""

from settings import (
    BaseConfig,
    DevConfig,
    PathSettings,
    ProdConfig,
    StagingConfig,
    get_settings,
    paths,
    settings,
)

__all__ = [
    "settings",
    "paths",
    "get_settings",
    "PathSettings",
    "BaseConfig",
    "DevConfig",
    "StagingConfig",
    "ProdConfig",
]
