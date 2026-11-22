-- Q6. How do customers pay, and how many installments do they use?
-- Technique: the payment fact at its own grain (never joined to order lines)
SELECT payment_type,
       count(*)                       AS payments,
       round(sum(payment_value), 2)   AS payment_value,
       round(100.0 * sum(payment_value) / sum(sum(payment_value)) OVER (), 2) AS value_share_pct,
       round(avg(payment_installments), 2) AS avg_installments
FROM {gold}.fact_payment
GROUP BY payment_type
ORDER BY payment_value DESC;
