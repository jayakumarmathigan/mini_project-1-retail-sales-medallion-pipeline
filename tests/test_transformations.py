from decimal import Decimal

from src.transformations import (add_rejection_reason, cast_orders, enrich_sales,
                                 latest_per_key)

COLS = ["order_id", "customer_id", "product_id", "quantity",
        "order_date", "status", "updated_at"]


def raw(spark, **changes):
    row = dict(zip(COLS, ["O1", "C1", "P1", "2", "2026-09-01",
                          "completed", "2026-09-01 10:00:00"]))
    row.update(changes)
    return spark.createDataFrame([tuple(row[c] for c in COLS)],
                                 ", ".join(f"{c} string" for c in COLS))


def reason_for(spark, **changes):
    customers = spark.createDataFrame([("C1",)], "customer_id string")
    products = spark.createDataFrame([("P1",)], "product_id string")
    result = add_rejection_reason(cast_orders(raw(spark, **changes)), customers, products)
    return result.first()["rejection_reason"]


def test_valid_order_is_accepted(spark):
    assert reason_for(spark) is None


def test_bad_records_are_rejected(spark):
    cases = [
        ({"order_id": None}, "missing_order_id"),
        ({"quantity": "-1"}, "invalid_quantity"),
        ({"quantity": "abc"}, "invalid_quantity"),
        ({"order_date": "not-a-date"}, "invalid_order_date"),
        ({"updated_at": "bad"}, "invalid_updated_at"),
        ({"status": "unknown"}, "invalid_status"),
        ({"customer_id": "C999"}, "unknown_customer"),
        ({"product_id": "P999"}, "unknown_product"),
    ]
    for changes, expected in cases:
        assert reason_for(spark, **changes) == expected, changes


def test_latest_version_wins(spark):
    df = spark.createDataFrame([("O1", "PLACED", 1), ("O1", "COMPLETED", 2)],
                               "order_id string, status string, version int")
    rows = latest_per_key(df, "order_id", "version").collect()
    assert len(rows) == 1
    assert rows[0]["status"] == "COMPLETED"


def test_only_completed_orders_count_as_sales(spark):
    orders = spark.createDataFrame(
        [("O1", "P1", 2, "COMPLETED"), ("O2", "P1", 5, "CANCELLED")],
        "order_id string, product_id string, quantity int, status string")
    products = spark.createDataFrame(
        [("P1", "Phone", "Electronics", Decimal("100.50"))],
        "product_id string, product_name string, category string, unit_price decimal(12,2)")
    rows = enrich_sales(orders, products).collect()
    assert len(rows) == 1
    assert rows[0]["sales_amount"] == Decimal("201.00")
