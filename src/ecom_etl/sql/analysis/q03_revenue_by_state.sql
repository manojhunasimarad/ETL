-- Q3. Where do our customers spend the most? Rank customer states by revenue.
-- Technique: RANK() over an aggregate; state as it was when the order was placed (SCD2)
SELECT c.state                       AS customer_state,
       count(DISTINCT f.order_id)    AS orders,
       round(sum(f.line_total), 2)   AS revenue,
       rank() OVER (ORDER BY sum(f.line_total) DESC) AS revenue_rank
FROM {gold}.fact_order_line f
JOIN {gold}.dim_customer c ON c.customer_sk = f.customer_sk
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY c.state
ORDER BY revenue_rank;
