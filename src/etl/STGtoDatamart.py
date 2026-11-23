# Databricks notebook source
# MAGIC %md
# MAGIC # STG -> Datamart
# MAGIC
# MAGIC Third and final task of the `etl_flow` job.
# MAGIC
# MAGIC Promotes the staged entity into the curated datamart as an SCD Type 1 dimension,
# MAGIC using a Delta `MERGE` so keys are upserted rather than the whole table being rebuilt:
# MAGIC
# MAGIC * `{catalog}.{schema}.dim_{entity}` - one current row per business key
# MAGIC * `{catalog}.{schema}.datamart_{entity}_run_metrics` - row counts per run, read from
# MAGIC   the Delta history of the merge
# MAGIC
# MAGIC Business shaping (derived attributes, aggregations, conformed dimensions) belongs in
# MAGIC `shape_datamart()`.

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# dbutils is injected by the Databricks runtime, so it has no local definition.
# pylint: disable=undefined-variable
PARAMETER_DEFAULTS = {
    "catalog": "dev_catalog",
    "schema": "etl",
    "entity": "customer",
    "business_key": "customer_id",
    "run_date": "",
}

for parameter_name, default_value in PARAMETER_DEFAULTS.items():
    dbutils.widgets.text(parameter_name, default_value)  # noqa: F821 - provided by the runtime

MANAGED_COLUMNS = (
    "_change_hash",
    "_created_at",
    "_updated_at",
    "_run_date",
    "_processed_at",
    "_ingested_at",
    "_source_file",
)

# COMMAND ----------


def read_parameters() -> dict:
    """Return the task parameters trimmed."""
    return {
        name: dbutils.widgets.get(name).strip()  # noqa: F821
        for name in PARAMETER_DEFAULTS
    }


def require_business_key(df: DataFrame, business_key: str) -> None:
    """Fail with an actionable message when the configured business key is not in STG."""
    if business_key not in df.columns:
        raise ValueError(
            f"business_key '{business_key}' is not a column of {df.columns}. "
            "Check the business_key job parameter against the STG schema."
        )

# COMMAND ----------


def shape_datamart(df: DataFrame) -> DataFrame:
    """
    Datamart shaping rules.

    Add derived attributes and conformed dimensions here. The result must stay at one
    row per business key, otherwise the merge below will update a key several times.
    """
    return df


def payload_columns(df: DataFrame) -> list:
    """Columns carried from STG into the dimension, excluding metadata and load-managed columns."""
    return [column for column in df.columns if not column.startswith("_") and column not in MANAGED_COLUMNS]


def change_hash(df: DataFrame) -> DataFrame:
    """Fingerprint the business payload so unchanged keys are skipped by the merge."""
    payload = F.concat_ws(
        "||",
        *[
            F.coalesce(F.col(column).cast("string"), F.lit("<null>"))
            for column in payload_columns(df)
        ],
    )
    return df.withColumn("_change_hash", F.sha2(payload, 256))

# COMMAND ----------


def merge_into_dimension(
    source: DataFrame, dim_table: str, business_key: str, report_date: str
) -> None:
    """Upsert the source into the dimension: insert new keys, update changed ones, keep the rest."""
    payload = payload_columns(source)
    insert_columns = ", ".join([f"`{col}`" for col in payload] + ["`_change_hash`", "`_created_at`", "`_updated_at`", "`_run_date`"])
    insert_values = ", ".join(
        [f"source.`{column}`" for column in payload]
        + [
            "source._change_hash",
            "current_timestamp()",
            "current_timestamp()",
            f"'{report_date}'",
        ]
    )
    update_columns = ", ".join(f"target.`{column}` = source.`{column}`" for column in payload)

    source.createOrReplaceTempView("dim_source")
    spark.sql(
        f"""
        MERGE INTO {dim_table} AS target
        USING dim_source AS source
        ON target.`{business_key}` = source.`{business_key}`
        WHEN MATCHED AND target._change_hash <> source._change_hash THEN UPDATE SET
            {update_columns},
            target._change_hash = source._change_hash,
            target._updated_at = current_timestamp(),
            target._run_date = '{report_date}'
        WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES ({insert_values})
        """
    )
    spark.catalog.dropTempView("dim_source")


def latest_merge_metrics(dim_table: str) -> dict:
    """Read the operation metrics Delta recorded for the most recent merge."""
    history = spark.sql(f"DESCRIBE HISTORY {dim_table} LIMIT 1").collect()
    if not history:
        return {}
    metrics = history[0]["operationMetrics"]
    return {key: int(value) for key, value in (metrics or {}).items()}

# COMMAND ----------


def record_run_metrics(
    metrics_table: str, entity: str, report_date: str, staged_rows: int, metrics: dict
) -> None:
    """Append one metrics row per run so the flow is observable over time."""
    inserted = metrics.get("numTargetRowsInserted", 0)
    updated = metrics.get("numTargetRowsUpdated", 0)
    unchanged = max(staged_rows - inserted - updated, 0)
    row = [
        (entity, report_date, staged_rows, inserted, updated, unchanged),
    ]
    schema = "entity string, run_date string, stg_rows long, inserted long, updated long, unchanged long"
    df = spark.createDataFrame(row, schema=schema).withColumn(
        "_recorded_at", F.current_timestamp()
    )
    (df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(metrics_table))

# COMMAND ----------


def main() -> None:
    parameters = read_parameters()
    catalog = parameters["catalog"]
    schema = parameters["schema"]
    entity = parameters["entity"]
    business_key = parameters["business_key"]
    report_date = parameters["run_date"]

    stg_table = f"{catalog}.{schema}.stg_{entity}"
    dim_table = f"{catalog}.{schema}.dim_{entity}"
    metrics_table = f"{catalog}.{schema}.datamart_{entity}_run_metrics"

    staged = shape_datamart(spark.read.table(stg_table))
    require_business_key(staged, business_key)
    prepared = change_hash(staged)

    if not spark.catalog.tableExists(dim_table):
        payload = payload_columns(prepared)
        initial_dim = (
            prepared.select(*[F.col(c) for c in payload], "_change_hash")
            .withColumn("_created_at", F.current_timestamp())
            .withColumn("_updated_at", F.current_timestamp())
            .withColumn("_run_date", F.lit(report_date))
        )
        (
            initial_dim.write.format("delta")
            .option("overwriteSchema", "true")
            .saveAsTable(dim_table)
        )
        metrics = {
            "numTargetRowsInserted": prepared.count(),
            "numTargetRowsUpdated": 0,
        }
    else:
        merge_into_dimension(prepared, dim_table, business_key, report_date)
        metrics = latest_merge_metrics(dim_table)
    record_run_metrics(metrics_table, entity, report_date, prepared.count(), metrics)
    print(f"stg_to_datamart: {dim_table} upserted={metrics}, metrics logged to {metrics_table}")


main()
