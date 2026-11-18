# Databricks Medallion ETL

A three-layer, incremental sales-events pipeline built with Databricks Asset
Bundles (DABs), Delta Live Tables (DLT), Auto Loader, and GitHub Actions.

## Pipeline

`src/pipelines/medallion_pipeline.py` reads JSON files incrementally from a
Unity Catalog Volume and publishes three managed Delta tables:

| Layer | Table | Purpose |
|---|---|---|
| Bronze | `bronze_sales_events` | Source fields plus rescued data, source filename, and ingestion timestamp |
| Silver | `silver_sales_events` | Typed and normalized records; invalid IDs, timestamps, customer IDs, and negative/invalid amounts are dropped and counted as expectation failures |
| Gold | `gold_daily_customer_sales` | Daily gross sales and event counts by customer and event type |

Expected input fields are `event_id`, `event_timestamp` (ISO-8601), `customer_id`,
`event_type`, and `amount` (decimal-compatible). The silver layer normalizes
event types to lowercase and derives `event_date` from the timestamp.

## Prerequisites and configuration

1. Install the current Databricks CLI and authenticate to your workspace:

   ```bash
   databricks auth login --host https://<workspace-url>
   ```

2. Ensure the pipeline's service principal/user has Unity Catalog access to the
   catalog/schema and read access to the input Volume.
3. Create a writable Volume for Auto Loader schema state. Configure its path and
   the input Volume path in `databricks.yml` defaults or pass overrides with
   `--var`.
4. Configure the GitHub repository:
   - Secret `DATABRICKS_TOKEN` (a service-principal token is recommended).
   - Actions variables `DATABRICKS_HOST_DEV` and `DATABRICKS_HOST_PROD`, each
     set to the full workspace URL beginning with `https://` (for example,
     `https://<workspace-host>`). These can be repository/org variables, or
     environment variables in the corresponding `dev`/`prod` GitHub
     environments. If the token is environment-scoped instead of a repository
     secret, add it to both the `dev` and `prod` environments.
   - GitHub environments named `dev` and `prod`; configure required reviewers
     for `prod` to gate production releases.
   - Set `catalog`, `source_path`, and `schema_location` in
     `databricks.yml` to values that exist in the target workspaces. Both
       targets use the same catalog and input Volume by default; their Auto
       Loader schema-state locations are separate.

The default catalog and Volume paths are examples and must be provisioned or
changed for your workspace before running the pipeline. Dev and prod use
separate output schemas (`medallion_dev` and `medallion_prod`).

## Local validation and deployment

```bash
python -m pip install pytest
python -m pytest -q

databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run medallion_pipeline --target dev
```

Override defaults without editing the bundle when needed:

```bash
databricks bundle validate --target dev \
  --var="catalog=analytics,source_path=/Volumes/analytics/landing/sales_events,schema_location=/Volumes/analytics/ops/autoloader_schemas/sales_events"
```

The pipeline is triggered in triggered mode by `bundle run`; it processes
available files and stops. Auto Loader tracks progress in the managed pipeline
checkpoint, and its schema state is stored at `schema_location`.

## GitHub Actions

- Pull requests run tests only; they do not deploy untrusted PR code.
- Pushes to `main` test, validate, deploy, and run the dev pipeline.
- `workflow_dispatch` can deploy and run either target. Protect the GitHub
  `prod` environment with required reviewers for an approval gate.

The deployment uses workspace authentication from the target environment's
`DATABRICKS_HOST` and `DATABRICKS_TOKEN` values. Bundle state and pipeline
resources are isolated by target.
