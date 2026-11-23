# Job and Workflow Resources

This directory contains Databricks Asset Bundle resource manifests defining automated workflows.

## Manifests

| File | Job Key | Description |
| --- | --- | --- |
| [`etl_flow.job.yml`](file:///C:/my-databricks-project/my-databricks-project/databricks/resources/etl_flow.job.yml) | `etl_flow` | Primary 3-stage medallion pipeline (Landing -> Feed -> STG -> Datamart). |
| [`etl_flow.job2.yml`](file:///C:/my-databricks-project/my-databricks-project/databricks/resources/etl_flow.job2.yml) | `etl_flow_2` | Secondary pipeline workflow definition. |

## Compute Model

All jobs in this directory run on **Databricks Serverless Compute**:
- No classic VM clusters (`job_clusters` or `new_cluster`) are declared.
- Databricks dynamically allocates and scales serverless compute per task execution.

## Task Graph (`etl_flow`)

```
init_catalog_schema  (Notebook: src/etl/InitCatalogSchema.py)
        │
        ▼
 landing_to_feed     (Notebook: src/etl/LandingtoFeed.py)
        │
        ▼
   feed_to_stg       (Notebook: src/etl/FeedtoSTG.py)
        │
        ▼
 stg_to_datamart     (Notebook: src/etl/STGtoDatamart.py)
```

Each task passes parameters dynamically from bundle variables and runtime values (`{{job.start_time.iso_date}}`).
