-- Q8. On which days of the week do people order?
-- Technique: the date dimension does the calendar work (no date functions in the query)
SELECT d.day_of_week,
       d.day_name,
       count(DISTINCT f.order_id)  AS orders,
       round(sum(f.line_total), 2) AS revenue
FROM {gold}.fact_order_line f
JOIN {gold}.dim_date d ON d.date_key = f.date_key
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY d.day_of_week, d.day_name
ORDER BY d.day_of_week;
