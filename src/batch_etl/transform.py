"""Transformation module: vectorized cleaning, feature engineering, and DQ quarantine partitioning."""

import hashlib
import json
import uuid
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np

from src.logger import logger, track_performance


def generate_trip_hash_ids(
    vendor_ids: pd.Series,
    pickups: pd.Series,
    dropoffs: pd.Series,
    pu_locs: pd.Series,
    do_locs: pd.Series,
    totals: pd.Series,
) -> List[str]:
    """Computes deterministic SHA-256 hash fingerprints for idempotent loads."""
    concatenated = (
        vendor_ids.astype(str)
        + "|"
        + pickups.dt.strftime("%Y-%m-%d %H:%M:%S")
        + "|"
        + dropoffs.dt.strftime("%Y-%m-%d %H:%M:%S")
        + "|"
        + pu_locs.astype(str)
        + "|"
        + do_locs.astype(str)
        + "|"
        + totals.round(2).astype(str)
    )
    return [hashlib.sha256(val.encode("utf-8")).hexdigest() for val in concatenated]


def build_date_dimension_records(unique_dates) -> pd.DataFrame:
    """Generates normalized dim_date records from observed pickup datetimes."""
    dates_series = pd.Series(pd.to_datetime(unique_dates)).dt.normalize().drop_duplicates()
    dim_df = pd.DataFrame({"full_date": dates_series.dt.date})
    dim_df["full_date"] = pd.to_datetime(dim_df["full_date"])
    dim_df["date_key"] = dim_df["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_df["year"] = dim_df["full_date"].dt.year.astype(int)
    dim_df["quarter"] = dim_df["full_date"].dt.quarter.astype(int)
    dim_df["month"] = dim_df["full_date"].dt.month.astype(int)
    dim_df["month_name"] = dim_df["full_date"].dt.strftime("%B")
    dim_df["day_of_week"] = dim_df["full_date"].dt.dayofweek + 1  # 1 = Monday, 7 = Sunday
    dim_df["day_name"] = dim_df["full_date"].dt.strftime("%A")
    dim_df["is_weekend"] = dim_df["day_of_week"].isin([6, 7])
    return dim_df.drop_duplicates(subset=["date_key"])


def transform_batch(
    raw_df: pd.DataFrame, batch_run_id: uuid.UUID
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Cleans raw batch, performs DQ checks, routes invalid records to quarantine, and engineers features.

    Returns:
        (clean_df, quarantine_df, dates_df)
    """
    if raw_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df = raw_df.copy()

    # 1. Standardize column names
    col_map = {
        "VendorID": "vendor_key",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pu_location_key",
        "DOLocationID": "do_location_key",
        "RatecodeID": "rate_code_key",
        "payment_type": "payment_type_key",
    }
    df.rename(columns={k: v for k, v in col_map.items() if k in df.columns}, inplace=True)

    # 2. Parse Timestamps
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

    # 3. Handle Missing/Null Values with Safe Defaults
    df["passenger_count"] = df["passenger_count"].fillna(1).astype(int)
    df["rate_code_key"] = df["rate_code_key"].fillna(99).astype(int)
    df["payment_type_key"] = df["payment_type_key"].fillna(0).astype(int)
    df["vendor_key"] = df["vendor_key"].fillna(0).astype(int)
    df["pu_location_key"] = df["pu_location_key"].fillna(0).astype(int)
    df["do_location_key"] = df["do_location_key"].fillna(0).astype(int)

    # Ensure measures are numeric
    numeric_cols = [
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        else:
            df[col] = 0.0

    # 4. Data Quality Rules & Validation Masks
    valid_ts = df["pickup_datetime"].notna() & df["dropoff_datetime"].notna()
    duration_valid = valid_ts & (df["dropoff_datetime"] > df["pickup_datetime"])
    distance_valid = df["trip_distance"] > 0
    fare_valid = df["fare_amount"] > 0
    total_valid = df["total_amount"] > 0

    clean_mask = valid_ts & duration_valid & distance_valid & fare_valid & total_valid

    # 5. Process Quarantined Records
    dirty_mask = ~clean_mask
    quarantine_df = pd.DataFrame()
    if dirty_mask.any():
        dirty_records = df[dirty_mask].copy()
        reasons = []
        for idx, row in dirty_records.iterrows():
            reason_parts = []
            if pd.isna(row["pickup_datetime"]) or pd.isna(row["dropoff_datetime"]):
                reason_parts.append("NULL_TIMESTAMPS")
            elif row["dropoff_datetime"] <= row["pickup_datetime"]:
                reason_parts.append("DROPOFF_BEFORE_OR_EQUAL_PICKUP")
            if row["trip_distance"] <= 0:
                reason_parts.append("NON_POSITIVE_DISTANCE")
            if row["fare_amount"] <= 0:
                reason_parts.append("NON_POSITIVE_FARE")
            if row["total_amount"] <= 0:
                reason_parts.append("NON_POSITIVE_TOTAL")
            reasons.append("; ".join(reason_parts) if reason_parts else "UNKNOWN_DQ_ANOMALY")

        dirty_records["rejection_reason"] = reasons
        dirty_records["batch_run_id"] = str(batch_run_id)

        # Serialize record to JSONB string format
        json_payloads = [
            json.dumps(r, default=str)
            for r in dirty_records.drop(columns=["rejection_reason", "batch_run_id"]).to_dict(
                orient="records"
            )
        ]
        quarantine_df = pd.DataFrame(
            {
                "batch_run_id": dirty_records["batch_run_id"].values,
                "rejection_reason": dirty_records["rejection_reason"].values,
                "raw_record": json_payloads,
            }
        )

    # 6. Process Clean Records & Feature Engineering
    clean_df = df[clean_mask].copy()
    dates_df = pd.DataFrame()

    if not clean_df.empty:
        # Trip duration in integer seconds
        clean_df["trip_duration_sec"] = (
            clean_df["dropoff_datetime"] - clean_df["pickup_datetime"]
        ).dt.total_seconds().astype(int)

        # Temporal partition keys
        clean_df["pickup_hour"] = clean_df["pickup_datetime"].dt.hour.astype(int)
        clean_df["pickup_date_key"] = clean_df["pickup_datetime"].dt.strftime("%Y%m%d").astype(int)

        # Deterministic SHA-256 Hash Idempotency Key
        clean_df["trip_hash_id"] = generate_trip_hash_ids(
            vendor_ids=clean_df["vendor_key"],
            pickups=clean_df["pickup_datetime"],
            dropoffs=clean_df["dropoff_datetime"],
            pu_locs=clean_df["pu_location_key"],
            do_locs=clean_df["do_location_key"],
            totals=clean_df["total_amount"],
        )

        # Deduplicate within the chunk itself if identical events exist
        clean_df.drop_duplicates(subset=["trip_hash_id"], inplace=True)

        # Metadata
        clean_df["ingestion_source"] = "BATCH"
        clean_df["batch_run_id"] = str(batch_run_id)

        # Extract unique dates to populate dim_date
        unique_pickup_dates = clean_df["pickup_datetime"].dt.normalize().drop_duplicates()
        dates_df = build_date_dimension_records(unique_pickup_dates)

        # Keep only target fact table columns
        fact_cols = [
            "trip_hash_id",
            "pickup_date_key",
            "pu_location_key",
            "do_location_key",
            "payment_type_key",
            "rate_code_key",
            "vendor_key",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_hour",
            "trip_duration_sec",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "congestion_surcharge",
            "total_amount",
            "ingestion_source",
            "batch_run_id",
        ]
        clean_df = clean_df[[c for c in fact_cols if c in clean_df.columns]]

    return clean_df, quarantine_df, dates_df
