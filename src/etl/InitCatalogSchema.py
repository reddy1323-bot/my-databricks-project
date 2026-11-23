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


def create_catalog_and_schema(catalog: str, schema: str) -> None:
    """Create the specified Unity Catalog catalog and schema if they do not exist."""
    print(f"Ensuring catalog `{catalog}` exists...")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    print(f"Catalog `{catalog}` is ready.")

    print(f"Ensuring schema `{catalog}`.`{schema}` exists...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    print(f"Schema `{catalog}`.`{schema}` is ready.")


# COMMAND ----------


def main() -> None:
    params = read_parameters()
    catalog = params["catalog"]
    schema = params["schema"]

    if not catalog:
        raise ValueError("Parameter 'catalog' cannot be empty.")
    if not schema:
        raise ValueError("Parameter 'schema' cannot be empty.")

    create_catalog_and_schema(catalog, schema)
    print(f"Initialization complete for `{catalog}`.`{schema}`.")


main()
