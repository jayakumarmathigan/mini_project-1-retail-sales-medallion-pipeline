# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: business-ready aggregates

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))
from src.transformations import (customer_summary, daily_sales, enrich_sales,
                                 product_performance)

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail_lakehouse")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
prefix = f"{catalog}.{schema}"

# COMMAND ----------

sales = enrich_sales(spark.table(f"{prefix}.silver_orders"),
                     spark.table(f"{prefix}.silver_products"))

gold_tables = {
    "gold_daily_sales": daily_sales(sales),
    "gold_product_performance": product_performance(sales),
    "gold_customer_summary": customer_summary(
        sales, spark.table(f"{prefix}.silver_customers")),
}

for name, df in gold_tables.items():
    (df.write.mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{prefix}.{name}"))
    print(f"{name}: {spark.table(f'{prefix}.{name}').count()} rows")
