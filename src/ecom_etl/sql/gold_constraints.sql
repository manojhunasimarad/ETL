-- Run after Spark has written the gold tables over JDBC (placeholder: {gold}).
-- Spark creates plain tables; the keys below make the star schema explicit,
-- let the database verify it, and show up as a star in pgAdmin's ERD tool.

ALTER TABLE {gold}.dim_date        ADD PRIMARY KEY (date_key);
ALTER TABLE {gold}.dim_customer    ADD PRIMARY KEY (customer_sk);
ALTER TABLE {gold}.dim_product     ADD PRIMARY KEY (product_sk);
ALTER TABLE {gold}.dim_seller      ADD PRIMARY KEY (seller_sk);
ALTER TABLE {gold}.fact_order_line ADD PRIMARY KEY (order_id, order_item_id);
ALTER TABLE {gold}.fact_payment    ADD PRIMARY KEY (order_id, payment_sequential);
ALTER TABLE {gold}.agg_daily_sales ADD PRIMARY KEY (date_key);

ALTER TABLE {gold}.fact_order_line
    ADD FOREIGN KEY (date_key)           REFERENCES {gold}.dim_date (date_key),
    ADD FOREIGN KEY (delivered_date_key) REFERENCES {gold}.dim_date (date_key),
    ADD FOREIGN KEY (customer_sk)        REFERENCES {gold}.dim_customer (customer_sk),
    ADD FOREIGN KEY (product_sk)         REFERENCES {gold}.dim_product (product_sk),
    ADD FOREIGN KEY (seller_sk)          REFERENCES {gold}.dim_seller (seller_sk);

ALTER TABLE {gold}.fact_payment
    ADD FOREIGN KEY (date_key)    REFERENCES {gold}.dim_date (date_key),
    ADD FOREIGN KEY (customer_sk) REFERENCES {gold}.dim_customer (customer_sk);

ALTER TABLE {gold}.agg_daily_sales
    ADD FOREIGN KEY (date_key) REFERENCES {gold}.dim_date (date_key);

-- One wide, analyst-friendly view over the star: what most BI queries need.
CREATE VIEW {gold}.v_order_lines AS
SELECT f.order_id,
       f.order_item_id,
       f.order_status,
       d.date            AS order_date,
       d.year_month      AS order_month,
       d.day_name,
       c.customer_unique_id,
       c.state           AS customer_state,
       c.city            AS customer_city,
       p.product_id,
       p.category_name_english AS category,
       s.seller_id,
       s.state           AS seller_state,
       f.price,
       f.freight_value,
       f.line_total,
       f.delivery_days,
       f.is_delivered,
       f.is_late
FROM {gold}.fact_order_line f
JOIN {gold}.dim_date     d ON d.date_key    = f.date_key
JOIN {gold}.dim_customer c ON c.customer_sk = f.customer_sk
JOIN {gold}.dim_product  p ON p.product_sk  = f.product_sk
JOIN {gold}.dim_seller   s ON s.seller_sk   = f.seller_sk;

ANALYZE;
