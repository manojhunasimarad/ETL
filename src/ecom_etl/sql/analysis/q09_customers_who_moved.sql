-- Q9. Which customers moved, and what did they buy at each address?
-- Technique: SCD Type 2 history; LEFT JOIN so versions without orders still show
WITH movers AS (
    SELECT customer_unique_id
    FROM {gold}.dim_customer
    WHERE customer_sk <> -1
    GROUP BY customer_unique_id
    HAVING count(*) > 1
)
SELECT c.customer_unique_id,
       c.version,
       c.city,
       c.state,
       c.effective_from,
       c.effective_to,
       c.is_current,
       count(DISTINCT f.order_id)              AS orders_in_version,
       round(coalesce(sum(f.line_total), 0), 2) AS revenue_in_version
FROM {gold}.dim_customer c
JOIN movers m ON m.customer_unique_id = c.customer_unique_id
LEFT JOIN {gold}.fact_order_line f ON f.customer_sk = c.customer_sk
GROUP BY c.customer_unique_id, c.version, c.city, c.state, c.effective_from, c.effective_to, c.is_current
ORDER BY c.customer_unique_id, c.version
LIMIT 30;
