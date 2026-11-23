# Source Code Directory

This directory contains all pipeline logic, ETL notebooks, and source artifacts for the Databricks project.

## Subdirectories

- **[`etl/`](file:///C:/my-databricks-project/my-databricks-project/src/etl)**: PySpark notebooks implementing the Landing, Feed, Staging, and Datamart layers.
- **[`jobs/`](file:///C:/my-databricks-project/my-databricks-project/src/jobs)**: Job entry points and legacy workspace export notes.

## Sync Rules

The legacy workspace export `ETLPipeline.dbc` is explicitly excluded from bundle deployment synchronization in [`databricks.yml`](file:///C:/my-databricks-project/my-databricks-project/databricks.yml):

```yaml
sync:
  exclude:
    - src/ETLPipeline.dbc
```
All active pipelines execute notebooks directly from [`src/etl/`](file:///C:/my-databricks-project/my-databricks-project/src/etl).
