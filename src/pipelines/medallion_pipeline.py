# Databricks notebook source

import dlt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


SALES_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("amount", StringType(), True),
    ]
)


def _required_pipeline_config(key: str) -> str:
    value = spark.conf.get(key, "").strip()
    if not value:
        raise ValueError(f"Required pipeline configuration '{key}' is missing")
    return value


@dlt.table(
    name="bronze_sales_events",
    comment="Raw sales-event JSON records with ingestion metadata.",
)
def bronze_sales_events() -> DataFrame:
    source_path = _required_pipeline_config("pipeline.source_path")
    schema_location = _required_pipeline_config("pipeline.schema_location")

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .schema(SALES_EVENT_SCHEMA)
        .load(source_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name="silver_sales_events",
    comment="Validated, typed sales events with a normalized event date.",
)
@dlt.expect_or_drop(
    "valid_event_id", "event_id IS NOT NULL AND length(trim(event_id)) > 0"
)
@dlt.expect_or_drop("valid_event_timestamp", "event_timestamp IS NOT NULL")
@dlt.expect_or_drop(
    "valid_customer_id", "customer_id IS NOT NULL AND length(trim(customer_id)) > 0"
)
@dlt.expect_or_drop(
    "valid_event_type", "event_type IS NOT NULL AND length(trim(event_type)) > 0"
)
@dlt.expect_or_drop("valid_nonnegative_amount", "amount IS NOT NULL AND amount >= 0")
def silver_sales_events() -> DataFrame:
    bronze = dlt.read_stream("bronze_sales_events")
    return (
        bronze.select(
            F.trim("event_id").alias("event_id"),
            F.to_timestamp("event_timestamp").alias("event_timestamp"),
            F.trim("customer_id").alias("customer_id"),
            F.lower(F.trim("event_type")).alias("event_type"),
            F.col("amount").cast("decimal(18,2)").alias("amount"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
        .withColumn("event_date", F.to_date("event_timestamp"))
    )


@dlt.table(
    name="gold_daily_customer_sales",
    comment="Daily sales metrics grouped by customer and event type.",
)
def gold_daily_customer_sales() -> DataFrame:
    silver = dlt.read("silver_sales_events")
    return silver.groupBy("event_date", "customer_id", "event_type").agg(
        F.sum("amount").alias("gross_sales"),
        F.count("*").alias("event_count"),
    )
