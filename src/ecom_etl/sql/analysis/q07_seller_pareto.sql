-- Q7. Do a few sellers make most of the revenue? (Pareto / 80-20)
-- Technique: running total with SUM() OVER (ORDER BY ... ROWS UNBOUNDED PRECEDING)
WITH seller_revenue AS (
    SELECT s.seller_id, sum(f.price) AS revenue
    FROM {gold}.fact_order_line f
    JOIN {gold}.dim_seller s ON s.seller_sk = f.seller_sk
    WHERE f.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY s.seller_id
), ranked AS (
    SELECT seller_id,
           revenue,
           row_number() OVER (ORDER BY revenue DESC) AS seller_rank,
           count(*) OVER ()                          AS sellers,
           sum(revenue) OVER (ORDER BY revenue DESC ROWS UNBOUNDED PRECEDING)
               / sum(revenue) OVER ()                AS cumulative_share
    FROM seller_revenue
)
SELECT seller_rank,
       seller_id,
       round(revenue, 2)                          AS revenue,
       round(100 * cumulative_share, 1)           AS cumulative_revenue_pct,
       round(100.0 * seller_rank / sellers, 1)    AS share_of_sellers_pct
FROM ranked
WHERE seller_rank <= 20
ORDER BY seller_rank;
