# Databricks notebook source

# COMMAND ----------

import sys

try:
    dbutils.widgets.text("environment", "dev")
    environment = dbutils.widgets.get("environment")
except NameError:
    environment = "dev"

print(f"Running my-databricks-project in {environment}")
