-- Q4. Which states get their deliveries late most often?
-- Technique: conditional aggregation (AVG of a boolean), HAVING to drop tiny groups
SELECT c.state                                  AS customer_state,
       count(*)                                 AS delivered_lines,
       round(avg(f.delivery_days), 1)           AS avg_delivery_days,
       round(100.0 * avg(f.is_late::int), 1)    AS late_pct
FROM {gold}.fact_order_line f
JOIN {gold}.dim_customer c ON c.customer_sk = f.customer_sk
WHERE f.is_delivered
GROUP BY c.state
HAVING count(*) >= 10
ORDER BY late_pct DESC, delivered_lines DESC;
