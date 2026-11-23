# Test Suite

This directory contains the Python pytest suite validating bundle configurations, task wiring, and notebook contracts before deployment.

## Files

- **[`bundle_loader.py`](file:///C:/my-databricks-project/my-databricks-project/tests/bundle_loader.py)**: Helper utilities for loading and parsing `databricks.yml` and included resource manifests.
- **[`test_bundle_config.py`](file:///C:/my-databricks-project/my-databricks-project/tests/test_bundle_config.py)**: Asserts bundle targets (`dev`, `prod`), variable declarations, and file exclusions.
- **[`test_etl_job.py`](file:///C:/my-databricks-project/my-databricks-project/tests/test_etl_job.py)**: Verifies task dependencies, parameter bindings, notebook path resolution, Databricks notebook header integrity, and serverless compute compliance.

## Running Tests Locally

Install dependencies and run:

```bash
pip install pytest pyyaml
python -m pytest -q
```
