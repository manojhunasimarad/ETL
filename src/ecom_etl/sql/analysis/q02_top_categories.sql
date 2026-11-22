-- Q2. Which product categories bring the most revenue, and what share of the total?
-- Technique: aggregate inside a window, SUM(SUM(x)) OVER ()
SELECT p.category_name_english AS category,
       count(*)                AS order_lines,
       round(sum(f.price), 2)  AS items_revenue,
       round(100.0 * sum(f.price) / sum(sum(f.price)) OVER (), 2) AS share_pct
FROM {gold}.fact_order_line f
JOIN {gold}.dim_product p ON p.product_sk = f.product_sk
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY p.category_name_english
ORDER BY items_revenue DESC
LIMIT 10;
