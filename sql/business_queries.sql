-- Run in the Databricks SQL editor.
-- Change the next line if you used a different catalog or schema.
USE workspace.retail_lakehouse;

-- 1. Monthly revenue by category
SELECT date_trunc('MONTH', order_date) AS month, category, SUM(revenue) AS revenue
FROM gold_daily_sales
GROUP BY ALL
ORDER BY month, revenue DESC;

-- 2. Top 3 products in each category
SELECT category, product_name, revenue, rank_in_category
FROM gold_product_performance
WHERE rank_in_category <= 3
ORDER BY category, rank_in_category;

-- 3. Top 10 customers by lifetime value
SELECT customer_name, city, total_orders, lifetime_value
FROM gold_customer_summary
ORDER BY lifetime_value DESC
LIMIT 10;

-- 4. Rejected records by reason
SELECT rejection_reason, COUNT(*) AS rows_rejected
FROM quarantine_orders
GROUP BY rejection_reason
ORDER BY rows_rejected DESC;

-- 5. Delta history shows each write and MERGE
DESCRIBE HISTORY silver_orders;
