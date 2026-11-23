# Databricks Project

A Databricks Asset Bundle project containing the `etl_flow` job: a three-stage
Landing -> Feed -> STG -> Datamart pipeline driven by one entity.

## What the bundle contains

| Path | Purpose |
| --- | --- |
| `databricks.yml` | Bundle root: targets, variables, includes |
| `databricks/resources/etl_flow.job.yml` | The `etl_flow` job definition |
| `src/etl/LandingtoFeed.py` | Task 1: landing files -> `feed_{entity}` |
| `src/etl/FeedtoSTG.py` | Task 2: feed -> `stg_{entity}` plus `stg_{entity}_rejected` |
| `src/etl/STGtoDatamart.py` | Task 3: STG -> `dim_{entity}` via Delta MERGE |
| `tests/` | Pytest suite that reads the bundle and asserts its structure |

The three tasks share a single job cluster and run in order: `feed_to_stg` waits
for `landing_to_feed`, and `stg_to_datamart` waits for `feed_to_stg`, so a failed
layer stops the chain instead of publishing stale data.

## Parameters

Bundle variables in `databricks.yml` feed the job's task parameters:

| Variable | Default | Purpose |
| --- | --- | --- |
| `catalog` | `dev_catalog` (`prod_catalog` on `prod`) | Catalog holding every layer |
| `schema` | `etl` | Schema holding the tables |
| `landing_path` | `/Volumes/dev_catalog/etl/landing` | Raw landing location |
| `entity` | `customer` | Drives file and table naming |
| `business_key` | `customer_id` | Key used for dedupe and the dimension merge |
| `spark_version` | `15.4.x-scala2.12` | Job cluster runtime |
| `node_type_id` | `Standard_DS3_v2` | Job cluster node type |
| `num_workers` | `1` | Job cluster workers (0 = single node) |

Override any of them per deploy, for example:
`databricks bundle deploy --target dev --var entity=orders --var business_key=order_id`

## Local validation and deployment

```bash
python -m pip install pytest pyyaml
python -m pytest -q

databricks auth login --host https://<workspace-url>
databricks bundle validate --target dev
databricks bundle deploy  --target dev
databricks bundle run etl_flow --target dev
```

`dev` is the default target and runs in development mode, so every resource is
prefixed with `[dev <your-username>]` and never collides with another engineer's
deployment. `prod` runs in production mode and overrides `catalog` and
`landing_path`; deploy it with a service principal and set `run_as` on the job so
ownership does not follow a personal account.

## Data quality and observability

- `FeedtoSTG` routes rows with a missing or empty business key into
  `stg_{entity}_rejected` with a `_reject_reason` instead of dropping them silently.
- `STGtoDatamart` upserts with a change hash, so unchanged keys are not rewritten,
  and appends one row per run to `datamart_{entity}_run_metrics` (rows staged,
  inserted, updated, unchanged).
- `LandingtoFeed` fails fast when the landing partition is empty.

## Credentials

Keep workspace credentials in Databricks CLI profiles locally and in GitHub
Actions secrets in CI; never commit credentials. See `conf/README.md`.

## Continuous integration

The workflows (`pytest` on pull requests, dev deploy on `main`, manual production deploy)
are configured in `.github/workflows/databricks-deploy.yml`:
- **Pull requests**: runs unit tests via `pytest`.
- **Push to main**: runs tests and deploys to `dev`.
- **Workflow dispatch**: manual trigger to deploy to `dev` or `prod`.

Configure GitHub Actions environment secrets and variables (`DATABRICKS_TOKEN`,
`DATABRICKS_HOST_DEV`, `DATABRICKS_HOST_PROD`) before running deployments.
