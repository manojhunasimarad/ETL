-- Q10. Segment customers by Recency, Frequency and Monetary value (RFM).
-- Technique: NTILE(4) scoring in a CTE chain, CASE to name the segments
WITH per_person AS (
    SELECT c.customer_unique_id,
           max(f.purchased_at)        AS last_purchase,
           count(DISTINCT f.order_id) AS frequency,
           sum(f.line_total)          AS monetary
    FROM {gold}.fact_order_line f
    JOIN {gold}.dim_customer c ON c.customer_sk = f.customer_sk
    WHERE f.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY c.customer_unique_id
), scored AS (
    SELECT per_person.*,
           ntile(4) OVER (ORDER BY last_purchase)      AS r_score,   -- 4 = most recent
           ntile(4) OVER (ORDER BY frequency, monetary) AS f_score,
           ntile(4) OVER (ORDER BY monetary)           AS m_score
    FROM per_person
)
SELECT CASE
           WHEN r_score = 4 AND m_score = 4 THEN 'champions'
           WHEN r_score >= 3 AND m_score >= 3 THEN 'loyal, high value'
           WHEN r_score <= 2 AND m_score >= 3 THEN 'at risk: valuable but inactive'
           WHEN r_score >= 3 THEN 'recent, low spend'
           ELSE 'hibernating'
       END                          AS segment,
       count(*)                     AS customers,
       round(avg(monetary), 2)      AS avg_spend,
       round(avg(frequency), 2)     AS avg_orders,
       max(last_purchase)::date     AS latest_purchase
FROM scored
GROUP BY 1
ORDER BY customers DESC;
