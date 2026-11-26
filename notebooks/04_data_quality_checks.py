# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Data quality checks
# MAGIC The notebook raises an error if any check fails, so the job is marked as failed.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail_lakehouse")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
p = f"{catalog}.{schema}"

# COMMAND ----------

# Each query returns the number of violations (0 = pass)
checks = {
    "silver_orders has rows":
        f"SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM {p}.silver_orders",
    "order_id is unique":
        f"SELECT COUNT(*) FROM (SELECT order_id FROM {p}.silver_orders "
        "GROUP BY order_id HAVING COUNT(*) > 1)",
    "no null keys":
        f"SELECT COUNT(*) FROM {p}.silver_orders WHERE order_id IS NULL "
        "OR customer_id IS NULL OR product_id IS NULL",
    "quantity is positive":
        f"SELECT COUNT(*) FROM {p}.silver_orders WHERE quantity <= 0",
    "product price is valid":
        f"SELECT COUNT(*) FROM {p}.silver_products "
        "WHERE unit_price IS NULL OR unit_price <= 0",
    "gold revenue reconciles with silver":
        f"""SELECT CASE WHEN
              (SELECT SUM(revenue) FROM {p}.gold_daily_sales) <=>
              (SELECT SUM(o.quantity * pr.unit_price) FROM {p}.silver_orders o
                 JOIN {p}.silver_products pr USING (product_id)
                WHERE o.status = 'COMPLETED')
            THEN 0 ELSE 1 END""",
}

failed = []
for name, query in checks.items():
    violations = spark.sql(query).first()[0]
    print(f"{'PASS' if violations == 0 else 'FAIL'} | {name} | violations={violations}")
    if violations:
        failed.append(name)

if failed:
    raise ValueError(f"Data quality checks failed: {failed}")
