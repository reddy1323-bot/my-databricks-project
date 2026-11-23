# Databricks Bundle Configuration

This directory contains Databricks Asset Bundle (DAB) asset specifications and resource definitions.

## Subdirectories

- **[`resources/`](file:///C:/my-databricks-project/my-databricks-project/databricks/resources)**: YAML resource manifests declaring Databricks workflows, jobs, and task graphs.

## Bundle Integration

Files in `databricks/resources/*.yml` are automatically included by the root [`databricks.yml`](file:///C:/my-databricks-project/my-databricks-project/databricks.yml) configuration:

```yaml
include:
  - databricks/resources/*.yml
```
