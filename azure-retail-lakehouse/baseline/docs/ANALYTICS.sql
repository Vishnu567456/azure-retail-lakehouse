-- Replace workspace and vishnu_retail if your configuration differs.
SELECT * FROM workspace.vishnu_retail_gold.daily_sales ORDER BY order_date;

SELECT p.category, SUM(f.revenue) AS revenue_inr
FROM workspace.vishnu_retail_gold.fact_orders f
JOIN workspace.vishnu_retail_gold.dim_products p USING (product_id)
GROUP BY p.category ORDER BY revenue_inr DESC;

SELECT c.region_name, COUNT(*) AS orders, SUM(f.revenue) AS revenue_inr
FROM workspace.vishnu_retail_gold.fact_orders f
JOIN workspace.vishnu_retail_gold.dim_customers c USING (customer_id)
GROUP BY c.region_name;

SELECT * FROM workspace.vishnu_retail_ops.quarantine_orders;
SELECT * FROM workspace.vishnu_retail_ops.audit ORDER BY recorded_at DESC;
SELECT * FROM workspace.vishnu_retail_silver.orders WHERE is_deleted;
DESCRIBE HISTORY workspace.vishnu_retail_silver.orders;
