# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: clean, validate, deduplicate and MERGE
# MAGIC Invalid orders go to a quarantine table with a reason. Valid orders are upserted.

# COMMAND ----------

import os
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(".."))
from src.transformations import add_rejection_reason, cast_orders, latest_per_key

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail_lakehouse")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
prefix = f"{catalog}.{schema}"

# COMMAND ----------

def upsert(df, table: str, key: str, update_condition: str = None) -> None:
    """Create the table on first run, afterwards MERGE into it."""
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return
    (DeltaTable.forName(spark, table).alias("t")
     .merge(df.alias("s"), f"t.{key} = s.{key}")
     .whenMatchedUpdateAll(condition=update_condition)
     .whenNotMatchedInsertAll()
     .execute())

# COMMAND ----------

# MAGIC %md ## Dimensions (SCD Type 1: the latest value overwrites)

# COMMAND ----------

raw_customers = latest_per_key(spark.table(f"{prefix}.bronze_customers"),
                               "customer_id", "_ingested_at")
customers = raw_customers.select(
    "customer_id",
    F.trim("customer_name").alias("customer_name"),
    F.lower(F.trim("email")).alias("email"),
    F.initcap(F.trim("city")).alias("city"))
upsert(customers, f"{prefix}.silver_customers", "customer_id")

raw_products = latest_per_key(spark.table(f"{prefix}.bronze_products"),
                              "product_id", "_ingested_at")
products = raw_products.select(
    "product_id", "product_name", "category",
    F.expr("try_cast(unit_price AS DECIMAL(12,2))").alias("unit_price"))
upsert(products, f"{prefix}.silver_products", "product_id")

# COMMAND ----------

# MAGIC %md ## Orders

# COMMAND ----------

checked = add_rejection_reason(
    cast_orders(spark.table(f"{prefix}.bronze_orders")),
    spark.table(f"{prefix}.silver_customers"),
    spark.table(f"{prefix}.silver_products"))

# Rebuilt from all of Bronze on each run, so re-running never duplicates rows
(checked.filter("rejection_reason IS NOT NULL")
 .write.mode("overwrite").saveAsTable(f"{prefix}.quarantine_orders"))

valid = checked.filter("rejection_reason IS NULL").drop("rejection_reason")
latest = latest_per_key(valid, "order_id", "updated_at")

# Only a newer version of an order may overwrite the stored one
upsert(latest, f"{prefix}.silver_orders", "order_id", "s.updated_at > t.updated_at")

# COMMAND ----------

print("silver_orders:", spark.table(f"{prefix}.silver_orders").count())
display(spark.table(f"{prefix}.quarantine_orders").groupBy("rejection_reason").count())
