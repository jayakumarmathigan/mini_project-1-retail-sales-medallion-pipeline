# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC Creates the schema and landing volume, then copies one batch of sample files into it.
# MAGIC Run with `batch = 1` first, and later with `batch = 2` to simulate new data arriving.

# COMMAND ----------

import os
import shutil

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail_lakehouse")
dbutils.widgets.dropdown("batch", "1", ["1", "2"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
batch = dbutils.widgets.get("batch")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.files")

base = f"/Volumes/{catalog}/{schema}/files"
sample_dir = os.path.abspath("../data/sample")

files = {"1": [("customers.csv", "customers"), ("products.csv", "products"),
               ("orders_batch_1.csv", "orders")],
         "2": [("orders_batch_2.csv", "orders")]}

for file_name, folder in files[batch]:
    target_dir = f"{base}/landing/{folder}"
    os.makedirs(target_dir, exist_ok=True)
    target = f"{target_dir}/{file_name}"
    if os.path.exists(target):
        print(f"Already present, skipped: {target}")
    else:
        shutil.copyfile(f"{sample_dir}/{file_name}", target)
        print(f"Copied: {target}")
