# Databricks notebook source
# MAGIC %md
# MAGIC # Initialize Unity Catalog and Schema
# MAGIC
# MAGIC Utility setup notebook to ensure the required Unity Catalog catalog and schema
# MAGIC exist before executing pipeline jobs.
# MAGIC
# MAGIC Executes:
# MAGIC - `CREATE CATALOG IF NOT EXISTS <catalog>`
# MAGIC - `CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>`
# MAGIC - `CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.<volume>`

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Supplied by job tasks or manual widget execution.

# COMMAND ----------

# dbutils is injected by the Databricks runtime
# pylint: disable=undefined-variable
PARAMETER_DEFAULTS = {
    "catalog": "dev_catalog",
    "schema": "etl",
    "volume": "landing",
}

for parameter_name, default_value in PARAMETER_DEFAULTS.items():
    dbutils.widgets.text(parameter_name, default_value)  # noqa: F821

# COMMAND ----------


def read_parameters() -> dict:
    """Return trimmed parameters passed via widgets."""
    return {
        name: dbutils.widgets.get(name).strip()  # noqa: F821
        for name in PARAMETER_DEFAULTS
    }


def create_catalog_and_schema(catalog: str, schema: str, volume: str) -> None:
    """Create the specified Unity Catalog catalog, schema, and volume if they do not exist."""
    print(f"Ensuring catalog `{catalog}` exists...")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    print(f"Catalog `{catalog}` is ready.")

    print(f"Ensuring schema `{catalog}`.`{schema}` exists...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    print(f"Schema `{catalog}`.`{schema}` is ready.")

    if volume:
        print(f"Ensuring volume `{catalog}`.`{schema}`.`{volume}` exists...")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")
        print(f"Volume `{catalog}`.`{schema}`.`{volume}` is ready at /Volumes/{catalog}/{schema}/{volume}.")


# COMMAND ----------


def main() -> None:
    params = read_parameters()
    catalog = params["catalog"]
    schema = params["schema"]
    volume = params.get("volume", "landing")

    if not catalog:
        raise ValueError("Parameter 'catalog' cannot be empty.")
    if not schema:
        raise ValueError("Parameter 'schema' cannot be empty.")

    create_catalog_and_schema(catalog, schema, volume)
    print(f"Initialization complete for `{catalog}`.`{schema}` with volume `{volume}`.")


main()
