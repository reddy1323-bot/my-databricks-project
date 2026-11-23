# ETL Pipeline Notebooks

This directory houses the Databricks PySpark notebooks forming the 3-stage medallion data processing flow.

## Notebooks

### 0. [`InitCatalogSchema.py`](file:///C:/my-databricks-project/my-databricks-project/src/etl/InitCatalogSchema.py)
- **Purpose**: Pre-requisite initialization utility.
- **Actions**:
  - Executes `CREATE CATALOG IF NOT EXISTS <catalog>`.
  - Executes `CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>`.
- **Parameters**: `catalog`, `schema`.

---

### 1. [`LandingtoFeed.py`](file:///C:/my-databricks-project/my-databricks-project/src/etl/LandingtoFeed.py)
- **Task Key**: `landing_to_feed`
- **Source**: Raw CSV files in Unity Catalog Volume (`{landing_path}/{entity}/run_date={run_date}`).
- **Actions**:
  - Validates source partition availability (fails fast if empty).
  - Normalizes column names to `snake_case`.
  - Injects ingestion audit metadata (`_ingested_at`, `_source_file`, `_run_date`).
- **Target**: Replaces `{catalog}.{schema}.feed_{entity}` (Delta table).

---

### 2. [`FeedtoSTG.py`](file:///C:/my-databricks-project/my-databricks-project/src/etl/FeedtoSTG.py)
- **Task Key**: `feed_to_stg`
- **Source**: `{catalog}.{schema}.feed_{entity}`
- **Actions**:
  - Trims whitespace across string columns.
  - Quarantines rows missing a business key into `{catalog}.{schema}.stg_{entity}_rejected` (`append` mode) with `_reject_reason`.
  - Deduplicates valid rows by business key (retaining the most recent `_ingested_at`).
  - St হওয়ায় adds `_processed_at` timestamp.
- **Target**: Replaces `{catalog}.{schema}.stg_{entity}` (Delta table).

---

### 3. [`STGtoDatamart.py`](file:///C:/my-databricks-project/my-databricks-project/src/etl/STGtoDatamart.py)
- **Task Key**: `stg_to_datamart`
- **Source**: `{catalog}.{schema}.stg_{entity}`
- **Actions**:
  - Generates SHA-256 `_change_hash` over business payload columns (ignoring load/audit metadata).
  - Performs SCD Type 1 upsert using Delta `MERGE INTO`, only updating matched rows when `target._change_hash <> source._change_hash`.
  - Captures Delta history operation metrics (`numTargetRowsInserted`, `numTargetRowsUpdated`).
- **Targets**:
  - `{catalog}.{schema}.dim_{entity}` (Dimension Delta table).
  - `{catalog}.{schema}.datamart_{entity}_run_metrics` (Appends run counts).

---

## Task Parameters

| Parameter | Default | Purpose |
| --- | --- | --- |
| `catalog` | `dev_catalog` | Unity Catalog catalog |
| `schema` | `etl` | Target schema name |
| `landing_path` | `/Volumes/dev_catalog/etl/landing` | Raw landing volume root |
| `entity` | `customer` | Entity identifier driving table names |
| `business_key` | `customer_id` | Unique business key for dedupe and upsert |
| `run_date` | `""` | Run partition date (e.g. `2026-11-23`) |
