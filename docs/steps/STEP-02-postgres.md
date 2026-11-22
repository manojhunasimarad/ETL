# Step 2: Postgres landing zone and normalization

**Goal:** get the files into a real database without losing anything,
then build a clean, normalized, typed copy whose correctness is enforced
by the database itself (keys and constraints).

**JD lines this answers:** SQL and relational database concepts; design
table structures; data normalization principles; ingest data.

---

## 1. Concepts

### 1.1 Docker and Docker Compose in five sentences

An **image** is a packaged filesystem plus a start command (`postgres:16`).
A **container** is a running instance of an image. A **volume** keeps data
outside the container, so `docker compose down` does not delete your
database (`pgdata` in `docker-compose.yml`). **Ports** map a container
port to your laptop (`5432:5432`). Inside Compose, services reach each
other by **service name**: pgAdmin connects to host `postgres`, while your
laptop connects to `localhost`.

The **healthcheck** (`pg_isready`) lets other services wait until the
database actually accepts connections, not just until the container starts.

### 1.2 Landing zone: copy first, judge later

`<source>_raw` holds every CSV value as `TEXT`, with no constraints. Loading
it cannot fail because of bad data, so nothing is lost, and every later
step can be re-run from it. Three extra columns make each row traceable:

| Column | Why |
|---|---|
| `_raw_id` | load order; used to decide which duplicate to keep |
| `_source_file` | which file the row came from |
| `_loaded_at` | when it was loaded (lineage, freshness) |

### 1.3 Loading fast: COPY, streamed in chunks

| Method | Speed | Memory |
|---|---|---|
| `INSERT` row by row | slowest (one round trip per row) | low |
| `pandas.to_sql` in chunks (Zoomcamp module 1) | medium | one chunk in memory |
| `COPY ... FROM STDIN`, streamed (this project) | fastest | one 1 MB buffer |

`COPY` is Postgres's bulk-load protocol. `landing.load_raw` streams each
file into it in 1 MB pieces, so a 5 GB file uses the same memory as a 5 KB one.

### 1.4 Normalization (1NF, 2NF, 3NF) with our tables

Normalization removes redundancy so each fact is stored once and cannot
contradict itself.

| Normal form | Rule | Example in this data |
|---|---|---|
| **1NF** | atomic values, no repeating groups | an order with 3 products is 3 rows in `order_items`, not columns `product_1, product_2, product_3`; same for multiple payments |
| **2NF** | 1NF + every non-key column depends on the **whole** key | `order_items` has key (`order_id`, `order_item_id`). `customer_id` depends only on `order_id`, so it lives in `orders`, not in `order_items` |
| **3NF** | 2NF + no column depends on another non-key column | `product_category_name_english` depends on the category, not on the product, so it lives in `category_translation` |

Olist is *almost* 3NF. `customers` keeps `zip_code_prefix`, `city` and
`state` together, and city depends on the zip code (a transitive
dependency). Olist accepted that redundancy for convenience; spotting
such trade-offs is a typical interview question.

Normalized models are right for **writing** (OLTP): no update anomalies.
For **reading** (analytics) we deliberately denormalize into a star
schema in step 4. Knowing when to do which is the point.

### 1.5 Keys and constraints: let the database say no

`core.sql` declares the rules of the data contract as constraints:

| Constraint | Example | Guarantees |
|---|---|---|
| `PRIMARY KEY` | (`order_id`, `order_item_id`) | no duplicates |
| `FOREIGN KEY` | `order_items.product_id → products` | no orphans |
| `NOT NULL` | `orders.customer_id` | required values present |
| `CHECK` | `price > 0`, status in the accepted list, delivered ≥ purchased, no event after the extract | business rules |

A constraint violation aborts the **whole** statement, so `core.sql`
filters invalid rows *before* inserting; the constraints are the final
safety net that proves the filter is right. If a filter is wrong, the
build fails loudly instead of loading bad data quietly.

### 1.6 ELT in SQL, and the patterns used

* **Safe casts**: `pg_input_is_valid(text, 'numeric')` (Postgres 16+)
  tests a conversion without raising an error. `to_num`, `to_int`,
  `to_ts` return NULL for invalid input; a row is rejected when a value
  was present but did not convert (`num_nonnulls(raw) = num_nonnulls(typed)`).
* **Dedupe**: `DISTINCT ON (primary key) ... ORDER BY key, _raw_id` keeps
  the first loaded row per key.
* **Fixes**: `lower(trim(order_status))`, `replace(price, ',', '.')`,
  `lpad(zip, 5, '0')`, and inserting missing categories with
  `is_translated = FALSE` instead of rejecting thousands of products.
* **Cascade**: an order line whose order was rejected is rejected too
  (`order_id IN (SELECT order_id FROM core.orders)`).
* **Idempotency**: the script drops and recreates the schema, so running
  it twice gives the same result (there is a test for that).

What this SQL path does **not** give you is *why* a row was rejected.
Steps 3 and 5 rebuild the same logic in Spark with a quarantine table and
a reason per row; comparing both is a good interview story (SQL ELT vs
Spark ETL).

---

## 2. Code tour

1. [`docker-compose.yml`](../../docker-compose.yml): Postgres and pgAdmin.
2. [`db.py`](../../src/ecom_etl/db.py): settings from environment variables, schema naming.
3. [`landing.py`](../../src/ecom_etl/landing.py): `load_raw` (COPY) and `build_core`.
4. [`sql/core.sql`](../../src/ecom_etl/sql/core.sql): read it top to bottom; every table follows the same pattern.
5. [`tests/integration/test_landing.py`](../../tests/integration/test_landing.py): each defect type from step 1 checked against the database.

---

## 3. Run it

Needs Docker Desktop (with WSL 2 on Windows).

```bash
docker compose up -d postgres
pip install -e ".[dev,postgres]"
python -m ecom_etl generate
python -m ecom_etl load-raw --source synthetic
python -m ecom_etl build-core --source synthetic
pytest -q -rs                       # the Postgres tests now run instead of skipping
```

Output of `build-core` for 5,000 synthetic orders (from a CI run); the
`removed` column is duplicates plus rejected rows, including order lines
and payments of rejected orders:

```
  table                        raw      core   removed
  customers                  5,062     5,000        62
  sellers                      156       156         0
  products                   1,666     1,666         0
  orders                     5,061     4,805       256
  order_items                5,656     5,333       323
  order_payments             5,286     4,914       372
```

Browse the tables: `docker compose --profile tools up -d`, open
http://localhost:5050 (no login), server "ecom (docker)", password `ecom`.
Or use any SQL client on `localhost:5432`, user/password/db `ecom`.

For real data: `python -m ecom_etl load-raw --source olist` and
`python -m ecom_etl build-core --source olist`.

---

## 4. Exercises

1. In pgAdmin, compare `synthetic_raw.orders` and `synthetic_core.orders`
   for one `status_unnormalized` order from `data/raw/synthetic/_defects.csv`.
2. Remove the `price > 0` filter from the `order_items` INSERT and run
   `build-core`. What happens, and why is that better than silently loading?
3. Write a query: for each table, how many rows were removed, and how many
   of those were duplicates (hint: `count(*)` vs `count(DISTINCT key)` in raw).
4. On Olist, how many products got `is_translated = FALSE`, and which categories?
5. Time `load-raw` for 1,000,000 synthetic orders. Then think about how
   `pandas.to_sql` would compare (try it on one table if curious).

## 5. Check yourself

1. Why keep a raw copy at all, instead of loading straight into typed tables?
2. Give one example each of a 1NF, 2NF and 3NF violation using these tables.
3. Why does one bad row abort a whole `INSERT ... SELECT`, and how does `core.sql` avoid it?
4. What does `DISTINCT ON` do, and why does the `ORDER BY` matter?
5. What is a cascade rejection here, and when could it be the wrong choice?
6. What does a Docker volume protect you from?
7. Why is `COPY` faster than many `INSERT`s?

## 6. Learning material

**O'Reilly**
- *Fundamentals of Data Engineering*, ch. 6 "Storage" and ch. 7 "Ingestion"; in ch. 8, the data modeling section on normalization.
- *Learning SQL, 3rd Ed.* (Alan Beaulieu): constraints and the chapters on joins and subqueries, if your SQL feels rusty.

**Zoomcamp**
- [Module 01-docker-terraform](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform): Docker, Postgres, pgAdmin and ingesting data. We use COPY instead of `pandas.to_sql`; compare the two.

**Postgres docs**
- `COPY`, `DISTINCT ON`, `pg_input_is_valid`, and "Constraints" in the DDL chapter.

## 7. Done when

- [ ] `docker compose up -d postgres` works and all tests pass with Postgres running
- [ ] you can explain 1NF, 2NF and 3NF with these tables
- [ ] you can explain each WHERE condition in the `orders` INSERT
- [ ] your Step 2 story is in [`STORIES.md`](../STORIES.md)
