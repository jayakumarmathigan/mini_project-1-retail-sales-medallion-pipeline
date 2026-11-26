# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: incremental ingestion with Auto Loader
# MAGIC Raw data is stored as received (all strings) with audit columns.
# MAGIC The checkpoint ensures each file is processed only once.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail_lakehouse")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
base = f"/Volumes/{catalog}/{schema}/files"

# COMMAND ----------

def ingest(entity: str) -> None:
    table = f"{catalog}.{schema}.bronze_{entity}"
    stream = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("cloudFiles.schemaLocation", f"{base}/_schemas/{entity}")
              .option("header", "true")
              .load(f"{base}/landing/{entity}")
              .select("*",
                      F.col("_metadata.file_path").alias("_source_file"),
                      F.current_timestamp().alias("_ingested_at")))
    query = (stream.writeStream
             .option("checkpointLocation", f"{base}/_checkpoints/bronze_{entity}")
             .trigger(availableNow=True)   # process what is new, then stop
             .toTable(table))
    query.awaitTermination()
    print(f"{table}: {spark.table(table).count()} rows")

# COMMAND ----------

for entity in ["customers", "products", "orders"]:
    ingest(entity)
