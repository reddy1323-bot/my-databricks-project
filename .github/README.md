# GitHub Workflows and CI/CD

This directory contains GitHub Actions workflow definitions for continuous integration and bundle deployments.

## Workflows

### [`databricks-deploy.yml`](file:///C:/my-databricks-project/my-databricks-project/.github/workflows/databricks-deploy.yml)

Automates bundle validation, testing, and multi-environment deployment:

| Event | Target | Actions |
| --- | --- | --- |
| `pull_request` | N/A | Runs unit tests via `pytest`. |
| `push` to `main` | `dev` | Runs tests, validates bundle, and deploys to development workspace. |
| `workflow_dispatch` | `dev` / `prod` | Manually deploys to the selected target after passing tests. |

## Required Secrets and Variables

Configure these in repository or environment settings (`dev` and `prod`):

- **Secrets**:
  - `DATABRICKS_TOKEN`: Databricks personal access token or service principal token.
- **Variables**:
  - `DATABRICKS_HOST_DEV`: Development Databricks workspace URL (e.g. `https://<dev-url>.cloud.databricks.com`).
  - `DATABRICKS_HOST_PROD`: Production Databricks workspace URL (e.g. `https://<prod-url>.cloud.databricks.com`).
