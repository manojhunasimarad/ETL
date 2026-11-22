-- Secondary indexes for the gold schema (placeholder: {gold}).
-- Postgres indexes primary keys automatically but NOT foreign keys.
-- These cover the filters and joins the dashboard and analysts use most;
-- step 7 measures each one with EXPLAIN ANALYZE before and after.

CREATE INDEX IF NOT EXISTS fact_order_line_date_idx     ON {gold}.fact_order_line (date_key);
CREATE INDEX IF NOT EXISTS fact_order_line_customer_idx ON {gold}.fact_order_line (customer_sk);
CREATE INDEX IF NOT EXISTS fact_order_line_product_idx  ON {gold}.fact_order_line (product_sk);
CREATE INDEX IF NOT EXISTS fact_order_line_seller_idx   ON {gold}.fact_order_line (seller_sk);
CREATE INDEX IF NOT EXISTS fact_payment_customer_idx    ON {gold}.fact_payment (customer_sk);
CREATE INDEX IF NOT EXISTS dim_customer_person_idx      ON {gold}.dim_customer (customer_unique_id);
ANALYZE;
