-- Q1. How is revenue trending month over month?
-- Technique: CTE, LAG window function, NULLIF to avoid division by zero
WITH monthly AS (
    SELECT d.year_month,
           count(DISTINCT f.order_id) AS orders,
           sum(f.line_total)          AS revenue
    FROM {gold}.fact_order_line f
    JOIN {gold}.dim_date d ON d.date_key = f.date_key
    WHERE f.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY d.year_month
)
SELECT year_month,
       orders,
       round(revenue, 2)          AS revenue,
       round(revenue / orders, 2) AS avg_order_value,
       round(100.0 * (revenue - lag(revenue) OVER w) / nullif(lag(revenue) OVER w, 0), 1) AS revenue_growth_pct
FROM monthly
WINDOW w AS (ORDER BY year_month)
ORDER BY year_month;
