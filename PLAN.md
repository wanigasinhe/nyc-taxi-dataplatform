# NYC Taxi Data Platform — Implementation Plan & Architecture Blueprint

This implementation plan covers **Assignment 1** from the technical assessment, designed with senior-level engineering standards, idempotency, structured observability, and forward compatibility for **Assignment 2** (Kafka streaming + Prometheus/Grafana).

---

## 1. Executive Summary & Architectural Blueprint (Assignment 1 → Assignment 2)

To demonstrate production engineering judgment, **Assignment 1** is designed not as a throwaway batch script, but as a robust, decoupled, and idempotent data platform. The database schema, repository structure, and container network are built so that **Assignment 2** plugs directly into the existing infrastructure without modifying core data models.

```mermaid
flowchart TD
    subgraph Data Sources
        P1["Yellow Taxi Parquet (2023-01/02)"]
        Z1["Taxi Zone Lookup CSV"]
    end

    subgraph Batch Pipeline (Assignment 1)
        EXT["extract.py\n(PyArrow Stream Reader)"]
        TRF["transform.py\n(Vectorized DQ, Hashing, Surrogate Keys)"]
        LOD["load.py\n(PostgreSQL COPY + Staging Merge)"]
        PRF["flow.py\n(Prefect 3.x Orchestration & Retries)"]
    end

    subgraph PostgreSQL 17 Storage
        STG[("staging_taxi_trips (UNLOGGED)")]
        FACT[("fact_taxi_trips (Star Schema)")]
        DIMS[("dim_date, dim_location,\ndim_payment_type, dim_rate_code,\ndim_vendor")]
        QUAR[("quarantine_taxi_trips (Audit DQ)")]
        RUNS[("etl_pipeline_runs (Telemetry)")]
    end

    subgraph Presentation & Observability
        DASH["Streamlit Dashboard\n(KPIs, Peak Hours, Fare/Mile, Pipeline Health)"]
        JSONL["Structured Logs\n(logs/etl_metrics.jsonl)"]
    end

    subgraph Streaming Forward Compatibility (Assignment 2)
        KAFKA["Kafka Cluster (Broker)"]
        PRODUCER["Streaming Producer\n(Replay Parquet @ 10-50 events/sec)"]
        CONSUMER["Streaming Consumer\n(Upsert to fact_taxi_trips / agg_taxi_windows_5m)"]
        PROM["Prometheus (/metrics)"]
        GRAF["Grafana Dashboards"]
    end

    P1 --> EXT
    Z1 --> EXT
    EXT --> TRF
    TRF -->|Clean Records| LOD
    TRF -->|Rejected Records| QUAR
    LOD --> STG
    STG -->|Atomic MERGE / ON CONFLICT| FACT
    PRF -.->|Orchestrates| EXT
    PRF -.->|Orchestrates| TRF
    PRF -.->|Orchestrates| LOD
    PRF --> RUNS
    PRF --> JSONL

    FACT --> DASH
    DIMS --> DASH
    QUAR --> DASH
    RUNS --> DASH

    P1 -.->|Assignment 2| PRODUCER
    PRODUCER --> KAFKA
    KAFKA --> CONSUMER
    CONSUMER --> FACT
    CONSUMER --> PROM
    PROM --> GRAF
```

---

## 2. Key Architectural Decisions & Trade-offs

1. **Database: PostgreSQL 17 (Containerized with `shm_size: 256mb`)**
   - *Why:* While DuckDB is popular for pure local analytics, PostgreSQL is the shared enterprise OLAP/storage target for both Assignment 1 and Assignment 2. Choosing PostgreSQL 17 in Docker allows Assignment 2's streaming consumers to write directly into the same Star Schema using `INSERT ... ON CONFLICT DO NOTHING / UPDATE` (UPSERT) semantics.
   - *Memory Tuning:* `shm_size: 256mb` is allocated in `docker-compose.yml` to support PostgreSQL parallel query workers across multi-million-row fact tables.

2. **Orchestrator: Prefect 3.x (Local Ephemeral/Server Mode)**
   - *Why:* Pythonic `@flow` and `@task` decorators with native retry policies, caching, task-level execution telemetry, and structured state hooks (`on_failure`). Unlike Apache Airflow, Prefect does not require heavy scheduler/webserver/worker containers, leaving CPU and RAM headroom for Kafka in Assignment 2.

3. **High-Throughput Ingestion: PyArrow Chunking + PostgreSQL `COPY` Protocol**
   - *Why:* NYC Yellow Taxi monthly Parquet files contain ~3 million rows per file (~6.1 million rows for 2 months). Standard `pandas.to_sql()` issues row-by-row or multi-value `INSERT` statements that cause severe memory bloat and take hours.
   - *Solution:* PyArrow reads memory-bounded record batches (250,000 rows). Transformed batches are streamed into PostgreSQL via native `COPY` into an `UNLOGGED` staging table, achieving **~100,000+ rows/sec** throughput.

4. **Deterministic Idempotency: SHA-256 Trip Hash**
   - *Why:* Source Parquet files lack natural primary keys. Simply inserting serial IDs allows duplicate trips upon batch pipeline reruns.
   - *Solution:* Generate a deterministic SHA-256 hash `trip_hash_id` over `(vendor_id, pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, total_amount)`. An atomic `INSERT INTO fact_taxi_trips SELECT ... FROM staging ON CONFLICT (trip_hash_id) DO NOTHING` guarantees zero duplicate records even on continuous replays.

5. **Data Quality Governance: Audit Quarantine (`quarantine_taxi_trips`)**
   - *Why:* Silently dropping malformed records conceals upstream data drift.
   - *Solution:* Strict validation rules (e.g. `trip_distance > 0`, `fare_amount > 0`, `total_amount > 0`, `dropoff_datetime > pickup_datetime`) partition incoming rows. Malformed rows are routed to `quarantine_taxi_trips` with error taxonomy codes and original JSON payloads for post-mortem auditing.

---

## 3. Star Schema Data Model

The dimensional model exceeds the 3-table requirement by implementing 5 dimension tables and 1 central fact table, alongside 2 operational audit tables:

### Fact Table
* **`fact_taxi_trips`**
  - `trip_sk` (`BIGSERIAL PRIMARY KEY`): Synthetic surrogate key.
  - `trip_hash_id` (`VARCHAR(64) UNIQUE NOT NULL`): Idempotency key.
  - Foreign Keys: `pickup_date_key`, `pu_location_key`, `do_location_key`, `payment_type_key`, `rate_code_key`, `vendor_key`.
  - Timestamps: `pickup_datetime`, `dropoff_datetime`, `pickup_hour`, `trip_duration_sec`.
  - Measures: `passenger_count`, `trip_distance`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `total_amount`.
  - Lineage: `ingestion_source` (`'BATCH'` / `'STREAM'`), `batch_run_id` (`UUID`), `loaded_at` (`TIMESTAMPTZ`).

### Dimension Tables
1. **`dim_date`**: `date_key` (`YYYYMMDD`), `full_date`, `year`, `quarter`, `month`, `month_name`, `day_of_week`, `day_name`, `is_weekend`.
2. **`dim_location`**: `location_key` (TLC LocationID 1–265), `borough`, `zone`, `service_zone`.
3. **`dim_payment_type`**: `payment_type_key` (1–6), `payment_name` (`Credit card`, `Cash`, `No charge`, `Dispute`, `Unknown`, `Voided trip`).
4. **`dim_rate_code`**: `rate_code_key` (1–6, 99), `rate_description` (`Standard rate`, `JFK`, `Newark`, `Nassau/Westchester`, `Negotiated fare`, `Group ride`).
5. **`dim_vendor`**: `vendor_key` (1–2), `vendor_name` (`Creative Mobile Technologies`, `VeriFone Inc.`).

### Audit & Observability Tables
- **`etl_pipeline_runs`**: Records `run_id`, `source_file`, `status`, `rows_extracted`, `rows_loaded`, `rows_quarantined`, `duration_sec`, `throughput_rows_per_sec`, `peak_memory_mb`, `error_message`, `started_at`, `completed_at`.
- **`quarantine_taxi_trips`**: Records `quarantine_id`, `batch_run_id`, `rejection_reason`, `raw_record` (`JSONB`), `quarantined_at`.

---

## 4. Assignment 2 Bridge & Forward Compatibility

| Shared Layer | Assignment 1 (Batch) | Assignment 2 (Real-Time Streaming) |
| :--- | :--- | :--- |
| **Raw Dataset** | `data/raw/yellow_tripdata_2023-01.parquet` | Reused directly by Kafka Stream Producer at 10–50 events/sec |
| **Docker Network** | `docker-compose.yml` runs `postgres` | Extends network with `kafka-1`, `prometheus`, `grafana` |
| **Database & Schema** | Writes to `fact_taxi_trips` (`ingestion_source = 'BATCH'`) | Writes raw events (`ingestion_source = 'STREAM'`) + sliding window table `agg_taxi_windows_5m` |
| **Metrics & Telemetry**| Structured JSON lines + `etl_pipeline_runs` | Exposes metrics via `prometheus_client` on `/metrics` |
| **Analytics** | Streamlit queries static Star Schema | Streamlit adds real-time metrics feed alongside historical batch data |
