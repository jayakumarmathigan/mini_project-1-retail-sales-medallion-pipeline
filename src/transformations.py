"""Reusable PySpark transformations (no I/O, so they can be unit tested)."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def cast_orders(df: DataFrame) -> DataFrame:
    """Trim and type the raw string columns. Unparseable values become NULL."""
    return df.select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.trim("product_id").alias("product_id"),
        F.expr("try_cast(quantity AS INT)").alias("quantity"),
        F.expr("try_cast(order_date AS DATE)").alias("order_date"),
        F.upper(F.trim("status")).alias("status"),
        F.expr("try_cast(updated_at AS TIMESTAMP)").alias("updated_at"),
    )


def add_rejection_reason(orders: DataFrame, customers: DataFrame,
                         products: DataFrame) -> DataFrame:
    """Add rejection_reason (NULL means the row is valid).

    customers and products must have unique keys.
    """
    known_c = customers.select("customer_id", F.lit(True).alias("_known_customer"))
    known_p = products.select("product_id", F.lit(True).alias("_known_product"))
    joined = orders.join(known_c, "customer_id", "left").join(known_p, "product_id", "left")

    # The first matching rule wins
    reason = (
        F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), "missing_order_id")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
        .when(F.col("order_date").isNull(), "invalid_order_date")
        .when(F.col("updated_at").isNull(), "invalid_updated_at")
        .when(F.col("status").isNull()
              | ~F.col("status").isin("PLACED", "COMPLETED", "CANCELLED"), "invalid_status")
        .when(F.col("_known_customer").isNull(), "unknown_customer")
        .when(F.col("_known_product").isNull(), "unknown_product")
    )
    return (joined.withColumn("rejection_reason", reason)
            .select(*orders.columns, "rejection_reason"))


def latest_per_key(df: DataFrame, key: str, order_col: str) -> DataFrame:
    """Keep one row per key: the one with the greatest order_col."""
    window = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return (df.withColumn("_rn", F.row_number().over(window))
            .filter("_rn = 1").drop("_rn"))


def enrich_sales(orders: DataFrame, products: DataFrame) -> DataFrame:
    """Completed orders joined to products, with sales_amount.

    Uses the current product price (price history is out of scope).
    """
    amount = (F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)")
    return (orders.filter(F.col("status") == "COMPLETED")
            .join(products.select("product_id", "product_name", "category", "unit_price"),
                  "product_id", "inner")
            .withColumn("sales_amount", amount))


def daily_sales(sales: DataFrame) -> DataFrame:
    return sales.groupBy("order_date", "category").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("units_sold"),
        F.sum("sales_amount").alias("revenue"),
    )


def product_performance(sales: DataFrame) -> DataFrame:
    totals = sales.groupBy("product_id", "product_name", "category").agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("sales_amount").alias("revenue"),
    )
    window = Window.partitionBy("category").orderBy(F.col("revenue").desc())
    return totals.withColumn("rank_in_category", F.dense_rank().over(window))


def customer_summary(sales: DataFrame, customers: DataFrame) -> DataFrame:
    totals = sales.groupBy("customer_id").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("sales_amount").alias("lifetime_value"),
        F.max("order_date").alias("last_order_date"),
    )
    return totals.join(customers.select("customer_id", "customer_name", "city"),
                       "customer_id", "left")
