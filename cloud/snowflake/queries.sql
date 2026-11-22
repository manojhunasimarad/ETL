-- Questions on the gold star schema in Snowflake (database ECOM, schema GOLD).
-- Unquoted names are case-insensitive, so the lower-case names from Spark work as written.
USE WAREHOUSE ECOM_WH;
USE SCHEMA ECOM.GOLD;

-- 1. Row counts: compare with the export's _manifest.json.
SELECT 'fact_order_line' AS table_name, COUNT(*) AS row_count FROM fact_order_line
UNION ALL SELECT 'fact_payment', COUNT(*) FROM fact_payment
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product;

-- 2. Monthly delivered revenue and growth (sql/analysis/q01 in Postgres).
WITH m AS (
    SELECT DATE_TRUNC('MONTH', purchased_at) AS month, SUM(line_total) AS revenue
    FROM fact_order_line
    WHERE order_status = 'delivered'
    GROUP BY 1
)
SELECT month, ROUND(revenue, 2) AS revenue,
       ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY month))
             / NULLIF(LAG(revenue) OVER (ORDER BY month), 0), 1) AS growth_pct
FROM m
ORDER BY month;

-- 3. The unknown member at work: fact rows whose product was missing.
SELECT COUNT(*) AS lines_with_unknown_product
FROM fact_order_line
WHERE product_sk = -1;

-- 4. What the warehouse cost: credits used today by this warehouse.
SELECT warehouse_name, SUM(credits_used) AS credits
FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(DATEADD('day', -1, CURRENT_TIMESTAMP())))
GROUP BY 1;
