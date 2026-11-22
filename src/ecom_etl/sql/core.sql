-- =============================================================================
-- core: normalized, typed tables built from the raw landing tables (ELT in SQL)
-- =============================================================================
-- Placeholders filled in by ecom_etl.landing.build_core:
--   {raw}           schema with the untouched text copies of the CSV files
--   {core}          schema created by this script
--   {extracted_at}  moment the extract was taken; no event can be later
--
-- Every table gets a primary key, foreign keys and CHECK constraints, so the
-- database itself guarantees the rules of the data contract. Rows that break
-- a rule are filtered out *before* the INSERT (otherwise one bad row would
-- abort the whole statement). The Spark pipeline in steps 3-5 does the same
-- job with an explicit quarantine and a reason for every rejected row.
--
-- Three kinds of handling, as decided in step 1:
--   fix         ' Delivered' -> 'delivered', '129,90' -> 129.90, '1037' -> '01037',
--               categories missing from the translation table are added
--   dedupe      DISTINCT ON (primary key), keeping the first row loaded (_raw_id)
--   reject      everything that breaks a constraint, plus children of rejected parents
-- =============================================================================

DROP SCHEMA IF EXISTS {core} CASCADE;
CREATE SCHEMA {core};

-- Safe conversions: return NULL instead of raising an error.
-- pg_input_is_valid() needs Postgres 16 or newer.
CREATE FUNCTION {core}.to_ts(t TEXT) RETURNS TIMESTAMP LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN pg_input_is_valid(trim(t), 'timestamp') THEN trim(t)::timestamp END
$$;

CREATE FUNCTION {core}.to_num(t TEXT) RETURNS NUMERIC LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN pg_input_is_valid(replace(trim(t), ',', '.'), 'numeric')
                THEN replace(trim(t), ',', '.')::numeric END
$$;

CREATE FUNCTION {core}.to_int(t TEXT) RETURNS INT LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN pg_input_is_valid(trim(t), 'numeric') THEN
                CASE WHEN trim(t)::numeric = trunc(trim(t)::numeric)
                      AND abs(trim(t)::numeric) < 2147483647
                     THEN trim(t)::numeric::int END
           END
$$;


-- -----------------------------------------------------------------------------
-- category_translation
-- -----------------------------------------------------------------------------
CREATE TABLE {core}.category_translation (
    product_category_name         TEXT PRIMARY KEY,
    product_category_name_english TEXT    NOT NULL,
    is_translated                 BOOLEAN NOT NULL
);

INSERT INTO {core}.category_translation
SELECT DISTINCT ON (name) name, english, TRUE
FROM (
    SELECT nullif(trim(product_category_name), '')         AS name,
           nullif(trim(product_category_name_english), '') AS english,
           _raw_id
    FROM {raw}.category_translation
) t
WHERE name IS NOT NULL AND english IS NOT NULL
ORDER BY name, _raw_id;

-- fix: a category used by products but missing from the translation table is
-- added with its Portuguese name, flagged is_translated = FALSE
INSERT INTO {core}.category_translation
SELECT DISTINCT nullif(trim(product_category_name), ''), nullif(trim(product_category_name), ''), FALSE
FROM {raw}.products
WHERE nullif(trim(product_category_name), '') IS NOT NULL
ON CONFLICT (product_category_name) DO NOTHING;


-- -----------------------------------------------------------------------------
-- customers and sellers
-- -----------------------------------------------------------------------------
CREATE TABLE {core}.customers (
    customer_id        TEXT PRIMARY KEY,
    customer_unique_id TEXT NOT NULL,
    zip_code_prefix    TEXT,
    city               TEXT,
    state              TEXT
);

INSERT INTO {core}.customers
SELECT DISTINCT ON (customer_id)
       customer_id,
       customer_unique_id,
       lpad(nullif(trim(customer_zip_code_prefix), ''), 5, '0'),   -- fix: 1037 -> 01037
       lower(nullif(trim(customer_city), '')),
       upper(nullif(trim(customer_state), ''))
FROM {raw}.customers
WHERE nullif(trim(customer_id), '') IS NOT NULL
  AND nullif(trim(customer_unique_id), '') IS NOT NULL
ORDER BY customer_id, _raw_id;

CREATE TABLE {core}.sellers (
    seller_id       TEXT PRIMARY KEY,
    zip_code_prefix TEXT,
    city            TEXT,
    state           TEXT
);

INSERT INTO {core}.sellers
SELECT DISTINCT ON (seller_id)
       seller_id,
       lpad(nullif(trim(seller_zip_code_prefix), ''), 5, '0'),
       lower(nullif(trim(seller_city), '')),
       upper(nullif(trim(seller_state), ''))
FROM {raw}.sellers
WHERE nullif(trim(seller_id), '') IS NOT NULL
ORDER BY seller_id, _raw_id;


-- -----------------------------------------------------------------------------
-- products
-- -----------------------------------------------------------------------------
CREATE TABLE {core}.products (
    product_id         TEXT PRIMARY KEY,
    category_name      TEXT REFERENCES {core}.category_translation (product_category_name),
    name_length        INT CHECK (name_length >= 0),
    description_length INT CHECK (description_length >= 0),
    photos_qty         INT CHECK (photos_qty >= 0),
    weight_g           INT CHECK (weight_g >= 0),
    length_cm          INT CHECK (length_cm >= 0),
    height_cm          INT CHECK (height_cm >= 0),
    width_cm           INT CHECK (width_cm >= 0)
);

INSERT INTO {core}.products
SELECT product_id, category_name, name_length, description_length, photos_qty,
       weight_g, length_cm, height_cm, width_cm
FROM (
    SELECT DISTINCT ON (product_id)
           product_id,
           nullif(trim(product_category_name), '')        AS category_name,
           {core}.to_int(product_name_lenght)        AS name_length,
           {core}.to_int(product_description_lenght) AS description_length,
           {core}.to_int(product_photos_qty)         AS photos_qty,
           {core}.to_int(product_weight_g)           AS weight_g,
           {core}.to_int(product_length_cm)          AS length_cm,
           {core}.to_int(product_height_cm)          AS height_cm,
           {core}.to_int(product_width_cm)           AS width_cm,
           num_nonnulls(product_name_lenght, product_description_lenght, product_photos_qty,
                        product_weight_g, product_length_cm, product_height_cm,
                        product_width_cm)            AS raw_values
    FROM {raw}.products
    WHERE nullif(trim(product_id), '') IS NOT NULL
    ORDER BY product_id, _raw_id
) p
-- every value that was present must have converted
WHERE num_nonnulls(name_length, description_length, photos_qty, weight_g,
                   length_cm, height_cm, width_cm) = raw_values
  AND coalesce(least(name_length, description_length, photos_qty, weight_g,
                     length_cm, height_cm, width_cm), 0) >= 0;


-- -----------------------------------------------------------------------------
-- orders
-- -----------------------------------------------------------------------------
CREATE TABLE {core}.orders (
    order_id              TEXT PRIMARY KEY,
    customer_id           TEXT      NOT NULL REFERENCES {core}.customers,
    order_status          TEXT      NOT NULL CHECK (order_status IN (
                              'delivered', 'shipped', 'canceled', 'unavailable',
                              'invoiced', 'processing', 'created', 'approved')),
    purchased_at          TIMESTAMP NOT NULL,
    approved_at           TIMESTAMP,
    delivered_carrier_at  TIMESTAMP,
    delivered_customer_at TIMESTAMP,
    estimated_delivery_at TIMESTAMP NOT NULL,
    CONSTRAINT delivered_after_purchase
        CHECK (delivered_customer_at IS NULL OR delivered_customer_at >= purchased_at),
    CONSTRAINT no_events_after_extract
        CHECK (greatest(purchased_at, approved_at, delivered_carrier_at, delivered_customer_at)
               <= TIMESTAMP '{extracted_at}')
);

INSERT INTO {core}.orders
SELECT order_id, customer_id, order_status, purchased_at, approved_at,
       delivered_carrier_at, delivered_customer_at, estimated_delivery_at
FROM (
    SELECT DISTINCT ON (order_id)
           order_id,
           nullif(trim(customer_id), '')                AS customer_id,
           lower(trim(order_status))                    AS order_status,   -- fix: ' Delivered'
           {core}.to_ts(order_purchase_timestamp)       AS purchased_at,
           {core}.to_ts(order_approved_at)              AS approved_at,
           {core}.to_ts(order_delivered_carrier_date)   AS delivered_carrier_at,
           {core}.to_ts(order_delivered_customer_date)  AS delivered_customer_at,
           {core}.to_ts(order_estimated_delivery_date)  AS estimated_delivery_at,
           num_nonnulls(order_purchase_timestamp, order_approved_at, order_delivered_carrier_date,
                        order_delivered_customer_date, order_estimated_delivery_date) AS raw_values
    FROM {raw}.orders
    WHERE nullif(trim(order_id), '') IS NOT NULL
    ORDER BY order_id, _raw_id
) o
WHERE customer_id IN (SELECT customer_id FROM {core}.customers)
  AND order_status IN ('delivered', 'shipped', 'canceled', 'unavailable',
                       'invoiced', 'processing', 'created', 'approved')
  AND purchased_at IS NOT NULL
  AND estimated_delivery_at IS NOT NULL
  AND num_nonnulls(purchased_at, approved_at, delivered_carrier_at,
                   delivered_customer_at, estimated_delivery_at) = raw_values
  AND (delivered_customer_at IS NULL OR delivered_customer_at >= purchased_at)
  AND greatest(purchased_at, approved_at, delivered_carrier_at, delivered_customer_at)
      <= TIMESTAMP '{extracted_at}';


-- -----------------------------------------------------------------------------
-- order_items: one row per order line
-- -----------------------------------------------------------------------------
CREATE TABLE {core}.order_items (
    order_id          TEXT NOT NULL REFERENCES {core}.orders,
    order_item_id     INT  NOT NULL CHECK (order_item_id >= 1),
    product_id        TEXT NOT NULL REFERENCES {core}.products,
    seller_id         TEXT NOT NULL REFERENCES {core}.sellers,
    shipping_limit_at TIMESTAMP,
    price             NUMERIC(12, 2) NOT NULL CHECK (price > 0),
    freight_value     NUMERIC(12, 2) NOT NULL CHECK (freight_value >= 0),
    PRIMARY KEY (order_id, order_item_id)
);

INSERT INTO {core}.order_items
SELECT order_id, order_item_id, product_id, seller_id, shipping_limit_at, price, freight_value
FROM (
    SELECT DISTINCT ON (order_id, {core}.to_int(order_item_id))
           order_id,
           {core}.to_int(order_item_id)         AS order_item_id,
           product_id,
           seller_id,
           {core}.to_ts(shipping_limit_date)    AS shipping_limit_at,
           shipping_limit_date                  AS shipping_limit_raw,
           {core}.to_num(price)                 AS price,           -- fix: '129,90'
           {core}.to_num(freight_value)         AS freight_value
    FROM {raw}.order_items
    WHERE nullif(trim(order_id), '') IS NOT NULL
    ORDER BY order_id, {core}.to_int(order_item_id), _raw_id
) i
WHERE order_item_id >= 1
  AND order_id   IN (SELECT order_id FROM {core}.orders)       -- children of rejected orders go too
  AND product_id IN (SELECT product_id FROM {core}.products)
  AND seller_id  IN (SELECT seller_id FROM {core}.sellers)
  AND (shipping_limit_raw IS NULL OR shipping_limit_at IS NOT NULL)
  AND price > 0
  AND freight_value >= 0;


-- -----------------------------------------------------------------------------
-- order_payments: one row per payment
-- -----------------------------------------------------------------------------
CREATE TABLE {core}.order_payments (
    order_id             TEXT NOT NULL REFERENCES {core}.orders,
    payment_sequential   INT  NOT NULL CHECK (payment_sequential >= 1),
    payment_type         TEXT NOT NULL CHECK (payment_type IN
                             ('credit_card', 'boleto', 'voucher', 'debit_card')),
    payment_installments INT  NOT NULL CHECK (payment_installments >= 1),
    payment_value        NUMERIC(12, 2) NOT NULL CHECK (payment_value >= 0),
    PRIMARY KEY (order_id, payment_sequential)
);

INSERT INTO {core}.order_payments
SELECT order_id, payment_sequential, payment_type, payment_installments, payment_value
FROM (
    SELECT DISTINCT ON (order_id, {core}.to_int(payment_sequential))
           order_id,
           {core}.to_int(payment_sequential)    AS payment_sequential,
           lower(trim(payment_type))            AS payment_type,
           {core}.to_int(payment_installments)  AS payment_installments,
           {core}.to_num(payment_value)         AS payment_value
    FROM {raw}.order_payments
    WHERE nullif(trim(order_id), '') IS NOT NULL
    ORDER BY order_id, {core}.to_int(payment_sequential), _raw_id
) p
WHERE payment_sequential >= 1
  AND order_id IN (SELECT order_id FROM {core}.orders)
  AND payment_type IN ('credit_card', 'boleto', 'voucher', 'debit_card')
  AND payment_installments >= 1
  AND payment_value >= 0;

ANALYZE;
