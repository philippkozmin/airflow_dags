CREATE OR REPLACE TABLE "scale2026-restcatalog".dds.dlp_demo_orders AS
SELECT
    order_id, customer_id, customer_name, order_date, quantity, unit_price,
    CAST(quantity * unit_price AS DECIMAL(14, 2)) AS order_total
FROM "scale2026-restcatalog".ods.dlp_demo_orders;
