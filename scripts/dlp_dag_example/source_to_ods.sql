CREATE OR REPLACE TABLE "scale2026-restcatalog".ods.dlp_demo_orders AS
SELECT
    CAST(order_id AS BIGINT) AS order_id,
    CAST(customer_id AS BIGINT) AS customer_id,
    TRIM(customer_name) AS customer_name,
    CAST(order_date AS DATE) AS order_date,
    CAST(quantity AS INTEGER) AS quantity,
    CAST(unit_price AS DECIMAL(12, 2)) AS unit_price
FROM "scale2026-restcatalog".source.dlp_demo_orders;
