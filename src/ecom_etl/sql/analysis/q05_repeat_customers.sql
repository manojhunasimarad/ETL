-- Q5. How many customers come back for a second order?
-- Technique: CTE per person (customer_unique_id, not customer_id), COUNT ... FILTER
WITH per_person AS (
    SELECT c.customer_unique_id,
           count(DISTINCT f.order_id) AS orders
    FROM {gold}.fact_order_line f
    JOIN {gold}.dim_customer c ON c.customer_sk = f.customer_sk
    WHERE f.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY c.customer_unique_id
)
SELECT count(*)                                   AS customers,
       count(*) FILTER (WHERE orders > 1)         AS repeat_customers,
       round(100.0 * count(*) FILTER (WHERE orders > 1) / count(*), 2) AS repeat_rate_pct,
       round(avg(orders), 3)                      AS avg_orders_per_customer
FROM per_person;
