"""Generate small synthetic retail data, including deliberately bad records.

Usage:  python data/generate_data.py
The fixed random seed means every run produces the same files.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
OUT = Path(__file__).parent / "sample"
TS = "%Y-%m-%d %H:%M:%S"
ORDER_COLS = ["order_id", "customer_id", "product_id", "quantity",
              "order_date", "status", "updated_at"]


def write(name, cols, rows):
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / name, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)
    print(f"{name}: {len(rows)} rows")


def make_order(number, start, days):
    day = start + timedelta(days=random.randrange(days))
    updated = day + timedelta(hours=random.randint(8, 20))
    return {
        "order_id": f"O{number:05d}",
        "customer_id": f"C{random.randint(1, 50):03d}",
        "product_id": f"P{random.randint(1, 20):03d}",
        "quantity": random.randint(1, 5),
        "order_date": day.strftime("%Y-%m-%d"),
        "status": random.choices(["COMPLETED", "PLACED", "CANCELLED"], [70, 20, 10])[0],
        "updated_at": updated.strftime(TS),
    }


def main():
    cities = ["Bengaluru", "Hyderabad", "Chennai", "Pune", "Mumbai", "Delhi"]
    customers = [{"customer_id": f"C{i:03d}", "customer_name": f"Customer {i}",
                  "email": f"Customer{i}@Example.com", "city": random.choice(cities)}
                 for i in range(1, 51)]
    categories = ["Electronics", "Grocery", "Clothing", "Home"]
    products = [{"product_id": f"P{i:03d}", "product_name": f"Product {i}",
                 "category": categories[i % 4],
                 "unit_price": f"{random.randint(100, 5000)}.00"}
                for i in range(1, 21)]

    # Batch 1: 500 good orders + 15 problem rows
    batch1 = [make_order(i, datetime(2026, 9, 1), 60) for i in range(1, 501)]
    bad = [dict(row) for row in batch1[:5]]                      # 5 exact duplicates
    n = 9000
    for field, value, count in [("order_id", "", 3), ("quantity", -2, 3),
                                ("order_date", "not-a-date", 2),
                                ("customer_id", "C999", 2)]:
        for _ in range(count):
            n += 1
            row = make_order(n, datetime(2026, 9, 1), 60)
            row[field] = value
            bad.append(row)
    all1 = batch1 + bad
    random.shuffle(all1)

    # Batch 2: 100 new orders + 40 status updates + 3 bad rows
    batch2 = [make_order(i, datetime(2026, 11, 1), 10) for i in range(501, 601)]
    placed = [r for r in batch1 if r["status"] == "PLACED"]
    for old in random.sample(placed, 40):
        batch2.append({**old, "status": "COMPLETED", "updated_at": "2026-11-12 09:00:00"})
    for i in range(3):
        row = make_order(9100 + i, datetime(2026, 11, 1), 10)
        row["quantity"] = "abc"
        batch2.append(row)
    random.shuffle(batch2)

    write("customers.csv", list(customers[0]), customers)
    write("products.csv", list(products[0]), products)
    write("orders_batch_1.csv", ORDER_COLS, all1)
    write("orders_batch_2.csv", ORDER_COLS, batch2)


if __name__ == "__main__":
    main()
