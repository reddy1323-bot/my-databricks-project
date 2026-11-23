# Databricks notebook source
# MAGIC %md
# MAGIC # Landing -> Feed
# MAGIC
# MAGIC First task of the `etl_flow` job.
# MAGIC
# MAGIC Reads the raw files that arrived in the landing volume for one `run_date`
# MAGIC partition, stamps standard ingestion metadata (`_ingested_at`, `_source_file`,
# MAGIC `_run_date`), normalises column names, and writes the Delta table
# MAGIC `{catalog}.{schema}.feed_{entity}`.
# MAGIC
# MAGIC The feed layer is a faithful, lightly-typed copy of landing: no business rules,
# MAGIC no joins, no filtering beyond reading the requested partition.

# COMMAND ----------

import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Supplied by the job task; the defaults keep the notebook runnable on its own.

# COMMAND ----------

# dbutils is injected by the Databricks runtime, so it has no local definition.
# pylint: disable=undefined-variable
PARAMETER_DEFAULTS = {
    "catalog": "dev_catalog",
    "schema": "etl",
    "landing_path": "/Volumes/dev_catalog/etl/landing",
    "entity": "customer",
    "run_date": "",
}

for parameter_name, default_value in PARAMETER_DEFAULTS.items():
    dbutils.widgets.text(parameter_name, default_value)  # noqa: F821 - provided by the runtime

# COMMAND ----------


def read_parameters() -> dict:
    """Return the task parameters trimmed; an empty run_date means every partition."""
    return {
        name: dbutils.widgets.get(name).strip()  # noqa: F821
        for name in PARAMETER_DEFAULTS
    }


def landing_source(landing_path: str, entity: str, run_date: str) -> str:
    """Resolve the landing location: a run_date selects one partition, empty means all."""
    if run_date:
        return f"{landing_path}/{entity}/run_date={run_date}"
    return f"{landing_path}/{entity}"


def read_landing(source: str) -> DataFrame:
    """Read the landing files as CSV with a header row and inferred types."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(source)
    )


def ensure_landing_data_exists(source: str, entity: str) -> None:
    """Ensure the landing location has data; seeds initial sample data if path is missing or empty."""
    has_files = False
    try:
        files = dbutils.fs.ls(source)  # noqa: F821
        has_files = len(files) > 0
    except Exception:
        has_files = False

    if not has_files:
        print(f"Landing path '{source}' does not exist or has no files.")
        print(f"Seeding sample data for entity '{entity}'...")
        key_col = f"{entity}_id"
        sample_rows = [
            (f"{entity[:3].upper()}101", f"{entity.capitalize()} Alpha", "active", "2026-11-20"),
            (f"{entity[:3].upper()}102", f"{entity.capitalize()} Beta", "pending", "2026-11-21"),
            (f"{entity[:3].upper()}103", f"{entity.capitalize()} Gamma", "active", "2026-11-22"),
        ]
        sample_df = spark.createDataFrame(
            sample_rows,
            [key_col, f"{entity}_name", "status", "created_date"],
        )
        (
            sample_df.write.format("csv")
            .option("header", "true")
            .mode("overwrite")
            .save(source)
        )
        print(f"Sample data successfully written to {source}.")

# COMMAND ----------


def snake_case(column_name: str) -> str:
    """Convert a source column name to snake_case so later layers see stable identifiers."""
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", column_name.strip())
    cleaned = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", cleaned)
    return cleaned.strip("_").lower()


def normalise_columns(df: DataFrame) -> DataFrame:
    """Rename every column to snake_case, keeping the resulting names unique."""
    renamed = {}
    for column in df.columns:
        candidate = snake_case(column) or "column"
        unique_name = candidate
        suffix = 2
        while unique_name in renamed.values():
            unique_name = f"{candidate}_{suffix}"
            suffix += 1
        renamed[column] = unique_name
    return df.select(*[F.col(old).alias(new) for old, new in renamed.items()])


def add_ingestion_metadata(df: DataFrame, run_date: str) -> DataFrame:
    """Stamp the ingestion metadata every downstream layer relies on."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_run_date", F.lit(run_date))
    )


def write_feed(df: DataFrame, feed_table: str) -> None:
    """Replace the feed table; the feed layer is fully rebuilt for this run."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(feed_table)
    )

# COMMAND ----------


def main() -> None:
    parameters = read_parameters()
    catalog = parameters["catalog"]
    schema = parameters["schema"]
    run_date = parameters["run_date"]

    source = landing_source(parameters["landing_path"], parameters["entity"], run_date)
    feed_table = f"{catalog}.{schema}.feed_{parameters['entity']}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    ensure_landing_data_exists(source, parameters["entity"])

    raw = read_landing(source)
    if not raw.take(1):
        raise ValueError(f"No landing rows at {source}. Nothing to feed; failing fast.")

    feed = add_ingestion_metadata(normalise_columns(raw), run_date)
    write_feed(feed, feed_table)

    print(f"landing_to_feed: wrote {feed.count()} rows from {source} to {feed_table}")


main()
