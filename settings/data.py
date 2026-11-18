"""Data and ingestion configuration management.

Encapsulates all raw data source URLs, month/date range specifications,
batch chunk slicing (e.g. 1 to 10 then finish, then 11 to 20 then finish),
and memory chunk sizes for dev, staging, and prod environments.
No data paths, URLs, or chunk sizes should be hardcoded elsewhere.
"""

import calendar
from datetime import datetime
import os
from typing import List, Optional, Tuple, Union
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def generate_month_range(start_month: str, end_month: Optional[str] = None) -> List[str]:
    """Generates sequential list of YYYY-MM strings from start_month to end_month (inclusive).

    Example:
        generate_month_range("2023-01", "2023-03") -> ["2023-01", "2023-02", "2023-03"]
    """
    if not end_month:
        return [start_month.strip()]

    start_dt = datetime.strptime(start_month.strip(), "%Y-%m")
    end_dt = datetime.strptime(end_month.strip(), "%Y-%m")

    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    months = []
    curr = start_dt
    while curr <= end_dt:
        months.append(curr.strftime("%Y-%m"))
        year = curr.year + (curr.month // 12)
        month = (curr.month % 12) + 1
        curr = datetime(year, month, 1)
    return months


class DataConfig(BaseSettings):
    """Centralized Data and Ingestion Configuration."""

    # --------------------------------------------------------------------------
    # 1. Raw Data Source URLs (Manual Mirror / S3 / CloudFront Overrides)
    # --------------------------------------------------------------------------
    RAW_DATA_URL: Optional[str] = Field(
        default=None,
        alias="RAW_DATA_URL",
        description="Manual URL for raw dataset (CloudFront base directory, custom S3 bucket, or direct .parquet file).",
    )
    DEFAULT_TLC_BASE_URL: str = Field(
        default="https://d37ci6vzurychx.cloudfront.net/trip-data",
        description="Official NYC TLC CloudFront base URL for monthly trip Parquet files.",
    )
    FILENAME_TEMPLATE: str = Field(
        default="yellow_tripdata_{month}.parquet",
        description="Filename template for monthly Parquet files.",
    )

    # --------------------------------------------------------------------------
    # 2. Ingestion Month Range (Cold Start / Disaster Recovery)
    # --------------------------------------------------------------------------
    START_MONTH: Optional[str] = Field(
        default="2023-01",
        alias="START_MONTH",
        description="Starting month (YYYY-MM) for ingestion. Auto-increments through END_MONTH.",
    )
    END_MONTH: Optional[str] = Field(
        default="2023-04",
        alias="END_MONTH",
        description="Ending month (YYYY-MM) for ingestion.",
    )

    # --------------------------------------------------------------------------
    # 3. Exact Date Window (Optional)
    # --------------------------------------------------------------------------
    START_DATE: Optional[str] = Field(
        default=None,
        alias="START_DATE",
        description="Exact starting date filter (YYYY-MM-DD). If set, extracts from this exact date.",
    )
    END_DATE: Optional[str] = Field(
        default=None,
        alias="END_DATE",
        description="Exact ending date filter (YYYY-MM-DD).",
    )

    # --------------------------------------------------------------------------
    # 4. Batch Chunking & Slicing (1 to 10 then finish, 11 to 20 then finish)
    # --------------------------------------------------------------------------
    DAY_CHUNK_SIZE: int = Field(
        default=10,
        alias="DAY_CHUNK_SIZE",
        description="Days per batch slice (e.g. 10 = Extract days 1 to 10 then finish, then 11 to 20 then finish).",
    )
    BATCH_CHUNK_SIZE: int = Field(
        default=150000,
        alias="BATCH_CHUNK_SIZE",
        description="Number of rows per memory streaming PyArrow chunk.",
    )

    model_config = SettingsConfigDict(
        env_file="settings/envs/dev.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def target_months(self) -> List[str]:
        """Calculates the list of months to process by auto-incrementing from START_MONTH to END_MONTH, or START_DATE to END_DATE."""
        if self.START_DATE and self.START_DATE.strip():
            start_m = self.START_DATE.strip()[:7]
            end_m = self.END_DATE.strip()[:7] if (self.END_DATE and self.END_DATE.strip()) else start_m
            return generate_month_range(start_m, end_m)
        if self.START_MONTH and self.START_MONTH.strip():
            start_m = self.START_MONTH.strip()
            end_m = self.END_MONTH.strip() if (self.END_MONTH and self.END_MONTH.strip()) else start_m
            return generate_month_range(start_m, end_m)
        return ["2023-01"]

    @property
    def DATA_MONTHS(self) -> List[str]:
        """Compatibility property forwarding to target_months."""
        return self.target_months

    def get_timeframe_for_month(self, month_str: str) -> Tuple[str, str]:
        """Returns the (start_date, next_end_date) timeframe for a month, clamped to explicit date filters."""
        year, month = int(month_str.split("-")[0]), int(month_str.split("-")[1])
        _, last_day = calendar.monthrange(year, month)
        m_start = f"{year:04d}-{month:02d}-01"
        m_end = f"{year:04d}-{month:02d}-{last_day:02d}"

        effective_start = (
            max(self.START_DATE.strip(), m_start)
            if (self.START_DATE and self.START_DATE.strip())
            else m_start
        )
        effective_end = (
            min(self.END_DATE.strip(), m_end)
            if (self.END_DATE and self.END_DATE.strip())
            else m_end
        )
        return effective_start, effective_end

    def get_overall_timeframe(self) -> Tuple[str, str]:
        """Returns the overall start date and next end date for the entire processing run."""
        months = self.target_months
        first_month, last_month = months[0], months[-1]
        first_start, _ = self.get_timeframe_for_month(first_month)
        _, last_end = self.get_timeframe_for_month(last_month)
        return first_start, last_end

    def get_parquet_filename(self, month: str) -> str:
        """Returns the formatted parquet filename for a given year-month (e.g. '2023-01')."""
        return self.FILENAME_TEMPLATE.format(month=month)

    def get_parquet_url(self, month: str) -> str:
        """Returns the remote download URL for a monthly parquet file, respecting manual overrides."""
        filename = self.get_parquet_filename(month)
        custom_url = (
            self.RAW_DATA_URL
            or os.getenv("RAW_DATA_URL")
            or os.getenv("RAW_DATA_BASE_URL")
            or os.getenv("PATH_RAW_DATA_URL")
            or os.getenv("PATH_TLC_TRIP_DATA_BASE_URL")
            or self.DEFAULT_TLC_BASE_URL
        )
        if custom_url.endswith(".parquet"):
            return custom_url
        return f"{custom_url.rstrip('/')}/{filename}"

    def get_day_slices(self, year: int, month: int, day_interval: Optional[int] = None) -> List[Tuple[int, int]]:
        """Calculates sequential day intervals for batch processing.

        Example with day_interval=10 for January (31 days):
            [(1, 10), (11, 20), (21, 31)]
        """
        _, last_day = calendar.monthrange(year, month)
        interval = day_interval if day_interval is not None else self.DAY_CHUNK_SIZE

        if not interval or interval <= 0 or interval >= last_day:
            return [(1, last_day)]

        slices = []
        curr = 1
        while curr <= last_day:
            end = min(curr + interval - 1, last_day)
            # If remaining tail days are small (<= 3 days), merge with current slice
            if last_day - end <= 3:
                end = last_day
            slices.append((curr, end))
            curr = end + 1
        return slices


# Default data configuration singleton
data_settings = DataConfig()
