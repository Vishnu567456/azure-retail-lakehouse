-- Run in a SQL notebook cell when compute is available.
SELECT * FROM workspace.vishnu_retail_silver.advanced_v1_customer_contacts;
SELECT * FROM workspace.vishnu_retail_silver.advanced_v1_customer_profile_history
ORDER BY customer_id, valid_from;
SELECT * FROM workspace.vishnu_retail_ops.advanced_v1_customer_quarantine;
SELECT * FROM workspace.vishnu_retail_ops.advanced_v1_cdf_batches ORDER BY batch_id;
SELECT phase, stream, batch_id, input_rows, watermark, dropped_by_watermark
FROM workspace.vishnu_retail_ops.advanced_v1_stream_progress ORDER BY phase,stream,batch_id;
SELECT * FROM workspace.vishnu_retail_gold.advanced_v1_web_activity_5m ORDER BY window_start;
SELECT * FROM workspace.vishnu_retail_gold.advanced_v1_sales_by_current_region;

-- Compare with order-time region attribution using half-open SCD2 intervals.
-- Our baseline orders have only a DATE; here their effective time is midnight UTC.
SELECT h.region, SUM(f.revenue) AS order_time_region_revenue
FROM workspace.vishnu_retail_gold.fact_orders f
JOIN workspace.vishnu_retail_silver.advanced_v1_customer_profile_history h
  ON f.customer_id=h.customer_id
 AND CAST(f.order_date AS TIMESTAMP)>=h.valid_from
 AND (CAST(f.order_date AS TIMESTAMP)<h.valid_to OR h.valid_to IS NULL)
 AND NOT h.is_deleted
GROUP BY h.region;
