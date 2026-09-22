CREATE OR REPLACE TABLE "scale2026-restcatalog".marts.dlp_demo_orders AS
SELECT
    customer_id, customer_name,
    COUNT(*) AS order_count,
    SUM(quantity) AS total_items,
    SUM(order_total) AS total_amount
FROM "scale2026-restcatalog".dds.dlp_demo_orders
GROUP BY customer_id, customer_name;
