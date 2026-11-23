# Databricks notebook source
# MAGIC %md
# MAGIC # Feed -> STG
# MAGIC
# MAGIC Second task of the `etl_flow` job.
# MAGIC
# MAGIC Takes the feed table, standardises it into the staging model, and enforces the
# MAGIC data-quality gate: rows without a usable business key are quarantined instead of
# MAGIC silently passing downstream. Produces:
# MAGIC
# MAGIC * `{catalog}.{schema}.stg_{entity}` - deduplicated, trimmed, one row per business key
# MAGIC * `{catalog}.{schema}.stg_{entity}_rejected` - quarantined rows with `_reject_reason`
# MAGIC
# MAGIC Business rules belong in `apply_business_rules()`; the plumbing around it is generic.

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession, Window
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

# COMMAND ----------


def read_parameters() -> dict:
    """Return the task parameters trimmed."""
    return {
        name: dbutils.widgets.get(name).strip()  # noqa: F821
        for name in PARAMETER_DEFAULTS
    }


def require_business_key(df: DataFrame, business_key: str) -> None:
    """Fail with an actionable message when the configured business key is not in the feed."""
    if business_key not in df.columns:
        raise ValueError(
            f"business_key '{business_key}' is not a column of the feed table. "
            f"Available columns: {', '.join(sorted(df.columns))}"
        )

# COMMAND ----------


def trim_string_columns(df: DataFrame) -> DataFrame:
    """Trim leading/trailing whitespace and collapse empty strings to null."""
    string_columns = [
        field.name for field in df.schema.fields if field.dataType.typeName() == "string"
    ]
    trimmed = df
    for column in string_columns:
        cleaned = F.trim(F.col(column))
        trimmed = trimmed.withColumn(column, F.when(cleaned == "", F.lit(None)).otherwise(cleaned))
    return trimmed


def split_on_business_key(df: DataFrame, business_key: str) -> tuple:
    """Split the feed into valid rows and quarantined rows, tagging the rejection reason."""
    is_invalid = F.col(business_key).isNull() | (F.trim(F.col(business_key).cast("string")) == "")
    rejected = df.filter(is_invalid).withColumn(
        "_reject_reason", F.lit(f"missing or empty business key: {business_key}")
    )
    return df.filter(~is_invalid), rejected


def apply_business_rules(df: DataFrame) -> DataFrame:
    """
    STG business rules.

    Extend this function with the entity's real rules - value mapping, code decoding,
    reference lookups. It must keep one row per source row and add no joins that change
    the grain.
    """
    return df


def deduplicate(df: DataFrame, business_key: str) -> DataFrame:
    """Keep the newest feed row per business key, so STG holds exactly one row per key."""
    newest_first = Window.partitionBy(business_key).orderBy(F.col("_ingested_at").desc())
    return (
        df.withColumn("_row_number", F.row_number().over(newest_first))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def write_layer(df: DataFrame, table: str) -> None:
    """Replace the target table for this run."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )


def write_rejected(df: DataFrame, table: str) -> None:
    """Append rejected rows so quarantine history is preserved across runs."""
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table)
    )

# COMMAND ----------


def main() -> None:
    parameters = read_parameters()
    catalog = parameters["catalog"]
    schema = parameters["schema"]
    entity = parameters["entity"]
    business_key = parameters["business_key"]

    feed_table = f"{catalog}.{schema}.feed_{entity}"
    stg_table = f"{catalog}.{schema}.stg_{entity}"
    rejected_table = f"{catalog}.{schema}.stg_{entity}_rejected"

    feed = spark.read.table(feed_table)
    require_business_key(feed, business_key)

    standardised = apply_business_rules(trim_string_columns(feed))
    valid, rejected = split_on_business_key(standardised, business_key)
    staged = deduplicate(valid, business_key).withColumn("_processed_at", F.current_timestamp())

    write_layer(staged, stg_table)
    write_rejected(rejected, rejected_table)

    print(
        f"feed_to_stg: {stg_table} rows={staged.count()}, "
        f"{rejected_table} rows={rejected.count()}"
    )


main()
