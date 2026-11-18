# Databricks Project

A Databricks Asset Bundle project using GitHub Actions for validation and deployment.

## Structure

- `databricks.yml` - bundle, variables, and environments
- `databricks/resources/` - Databricks Job definitions
- `src/notebooks/` - notebook source
- `src/jobs/` - Python job entry points
- `conf/` - configuration templates
- `tests/` - unit tests
- `.github/workflows/` - CI/CD pipeline

## GitHub Actions configuration

Create these GitHub repository secrets or variables:

- Secret `DATABRICKS_TOKEN` - a Databricks personal access token or a service principal token
- Variable `DATABRICKS_HOST_DEV` - development workspace URL
- Variable `DATABRICKS_HOST_PROD` - production workspace URL

## Local usage

```bash
# Install the Databricks CLI from:
# https://docs.databricks.com/aws/en/dev-tools/cli/install

databricks auth login --host <workspace-url>
databricks bundle validate

databricks bundle deploy --target dev

databricks bundle run sample_job --target dev
```

The workflow deploys changes to `dev` on pull requests and pushes to `main`, and to `prod` after a manual approval or tag workflow decision.
