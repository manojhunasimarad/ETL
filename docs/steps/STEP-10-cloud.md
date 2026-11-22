# Step 10: Cloud platforms (Databricks, Snowflake) and wrap-up

**Goal:** show that the pipeline is not tied to a laptop: run the same Spark
job on Databricks, load the gold star schema into Snowflake, and finish the
project with numbers and stories you can defend.

**JD lines this answers:** "experience with Databricks and Snowflake"
(preferred); deliver data to the people who use it.

---

## 1. Concepts

### 1.1 What changes in the cloud, and what does not

The transformations do not change. They are plain functions of a Spark
session and a lake location (`run_silver(spark, lake, ...)`), so they run the
same on a laptop, in Docker, in CI and on Databricks. What changes is around
them:

| | Here | Databricks | Snowflake |
|---|---|---|---|
| Compute | local Spark, Docker | clusters or serverless compute | virtual warehouses |
| Files | `data/lake/...` | Unity Catalog **volumes**, `/Volumes/<catalog>/<schema>/<volume>` | internal or external **stages** |
| Tables | Postgres `<source>_gold` | managed tables `<catalog>.<schema>.<table>` (Delta) | tables in `<database>.<schema>` |
| Cost | your machine | compute while it runs | **credits** while a warehouse runs |
| Secrets | `.env`, Compose | secret scopes | key pair, connections config |

### 1.2 Databricks

- **Workspace**: notebooks, jobs and the catalog in one web app.
- **Unity Catalog** names everything with three levels: `catalog.schema.table`.
  Free Edition gives you a catalog (usually `workspace`); the notebook creates
  a schema `ecom` in it.
- A **volume** is governed file storage inside a schema. Python's `open()` and
  Spark's readers and writers both use its path, so the generator and the lake
  need no changes.
- `saveAsTable` on Databricks writes a **Delta** table: Parquet files plus a
  transaction log, which adds ACID writes, schema enforcement and time travel
  (`DESCRIBE HISTORY workspace.ecom.fact_order_line`).
- **Serverless compute** (the only compute in Free Edition) runs Spark Connect:
  the driver is managed for you. Two consequences for this code:
  `DataFrame.persist()` is refused, and `spark.sparkContext` does not exist.
  The pipeline never needs the second, and `spark/compat.py` makes the first
  optional: with `ECOM_NO_CACHE=1` it recomputes instead of caching.

Switching caching off exposed a real bug: silver removed exact duplicates with
`dropDuplicates`, which keeps an *arbitrary* copy. With caching, the choice was
made once; without it, recomputing could choose a different copy and change
which row of a reused key wins. Silver now keeps the first copy loaded
(`row_number` over the columns, ordered by `_row_id`), which is deterministic
however often it is computed. The rule: never rely on a cache for correctness.

### 1.3 Snowflake

- **Storage, compute and services are separate.** Tables sit in storage;
  queries run on a **virtual warehouse** you size (XS, S, M, ...) and pay for
  per second while it runs. `AUTO_SUSPEND = 60` stops it after a minute idle;
  `AUTO_RESUME` starts it on the next query. On a trial this is what keeps
  your credits.
- **Loading** is two steps: put files on a **stage** (an internal stage is
  storage Snowflake manages; `PUT` uploads to it from a client), then
  `COPY INTO <table>` reads them. `FILE_FORMAT = (TYPE = PARQUET)` with
  `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` maps Parquet columns to table
  columns by name, so column order does not matter.
- **Load metadata**: Snowflake remembers which files it loaded into a table
  and skips them on the next `COPY`. Useful for incremental loads, but here
  every run replaces the tables, so the loader also clears the table's stage
  folder (`REMOVE`) before `PUT`, and `PURGE = TRUE` deletes files once
  loaded. Rerunning gives the same tables: the load is idempotent.
- **Types**: Spark `decimal(12,2)` becomes `NUMBER(12,2)`, `bigint`
  `INTEGER`, `string` `VARCHAR`, `timestamp` `TIMESTAMP_NTZ` (the pipeline's
  session time zone is UTC, so the stored values are UTC).
- **Authentication**: Snowflake is phasing out password-only sign-in. For a
  script, use key-pair authentication (`SNOWFLAKE_PRIVATE_KEY_FILE`); for
  yourself at a desk, `SNOWFLAKE_AUTHENTICATOR=externalbrowser` also works.

### 1.4 The export between them

Gold keeps `fact_order_line` partitioned by `order_month`. In a partitioned
folder the month is a directory name, not a column in the files, so a
warehouse reading the files directly would lose it. Spark also writes
timestamps as INT96 by default, an old encoding. `spark/export.py` therefore
rewrites every gold table into `data/lake/<source>/export/snowflake/<table>/`
with every column in the files and timestamps as `TIMESTAMP_MICROS`, and
writes `_manifest.json` with each table's columns and row count. The loader
needs only that folder and the manifest, not Spark.

---

## 2. Code tour

1. [`spark/compat.py`](../../src/ecom_etl/spark/compat.py): `persist` and `unpersist` that respect `ECOM_NO_CACHE`.
2. [`spark/databricks.py`](../../src/ecom_etl/spark/databricks.py): `run_batch` (generate, bronze, silver, gates, gold, gates) and `publish_tables` (`saveAsTable`).
3. [`cloud/databricks/ecom_pipeline.py`](../../cloud/databricks/ecom_pipeline.py): the notebook, in Databricks' source format.
4. [`spark/export.py`](../../src/ecom_etl/spark/export.py): `export_for_snowflake`.
5. [`snowflake.py`](../../src/ecom_etl/snowflake.py): types, DDL, `PUT` and `COPY INTO`, the load with count checks, and the connection from environment variables.
6. Tests: [`tests/test_snowflake.py`](../../tests/test_snowflake.py) (all SQL, and the load against a fake connection) and [`tests/integration/test_cloud.py`](../../tests/integration/test_cloud.py) (`run_batch` with caching off, publishing tables; the export's columns, counts and timestamp encoding).

---

## 3. Run it

### 3.1 Databricks Free Edition

1. Sign up for Databricks Free Edition and open the workspace.
2. Workspace → your folder → Import → File: `cloud/databricks/ecom_pipeline.py`.
3. Connect it to serverless compute and choose **Run all**. The widgets at the
   top set the catalog (default `workspace`), the schema (`ecom`) and the
   number of orders (20,000).
4. Read the printed status, record counts, check summary and seconds; look at
   the tables in Catalog Explorer and at the last cell's query.

Details and troubleshooting: [`cloud/databricks/README.md`](../../cloud/databricks/README.md).

### 3.2 Snowflake trial

```bash
python -m ecom_etl run --source synthetic --no-load      # gold in the lake
python -m ecom_etl export-snowflake --source synthetic   # Parquet + reports/snowflake_synthetic.sql

pip install -e ".[snowflake]"
export SNOWFLAKE_ACCOUNT=<orgname-accountname> SNOWFLAKE_USER=<you>
export SNOWFLAKE_AUTHENTICATOR=externalbrowser           # or SNOWFLAKE_PRIVATE_KEY_FILE=...
python -m ecom_etl load-snowflake --source synthetic     # exit code 1 if any count differs
```

Or run the generated script yourself: `snow sql -f reports/snowflake_synthetic.sql`
(Snowflake CLI) or `snowsql -f ...`. `PUT` uploads local files, so it does not
run in a Snowsight worksheet. Queries to try once loaded:
[`cloud/snowflake/queries.sql`](../../cloud/snowflake/queries.sql). Setup notes:
[`cloud/snowflake/README.md`](../../cloud/snowflake/README.md).

### 3.3 What CI verifies, and what it cannot

| Verified on every push | Needs your account |
|---|---|
| `run_batch` end to end on local Spark with caching off, publishing tables (`test_cloud.py`) | the notebook on serverless compute |
| the same on 20,000 orders in the smoke run, with timings | its timings on Databricks |
| the export: every column, row counts, microsecond timestamps | that Snowflake accepts the files |
| every SQL statement and the load order, against a fake connection | that Snowflake accepts the statements and the counts match |

In CI on 2026-11-22 (4-core runner, Spark 4.1.4), `run_batch` on 20,000 orders with
caching off took 70.6 s and produced the same 91,521 bronze records and the same
gold row counts as the cached `run` (68.9 s without its Postgres load). At this
size caching is worth almost nothing; the deterministic duplicate removal is
what made the uncached run safe.

Until you have run the right-hand column, say so: "written for Databricks and
Snowflake and tested locally" is true; "deployed on Snowflake" is not yet.

---

## 4. Exercises

1. Run the notebook twice. Do the tables change? Why not? Then run `DESCRIBE HISTORY` on a gold table and explain each version.
2. Set the widget to 200,000 orders and compare the Databricks timings with `RESULTS.md`. What differs between a 4-core CI runner and serverless compute?
3. In Snowflake, run `load-snowflake` twice and check the counts. Then remove the `REMOVE` statement from `load_sql` and run twice again. What happens, and why?
4. Size the warehouse to SMALL and time `queries.sql` again. What did the extra credits buy?
5. Replace the full reload with an incremental one: load only new order months, using Snowflake's load metadata instead of `REMOVE` and `CREATE OR REPLACE`. What can go wrong with late-arriving data?

## 5. Check yourself

1. What does the project's code need to change to run on Databricks, and why so little?
2. Why does serverless compute refuse `persist()`, and how does the pipeline cope?
3. Why was `dropDuplicates` a correctness risk without a cache?
4. What is a stage, and why does `PUT` not work in a worksheet?
5. How does `MATCH_BY_COLUMN_NAME` help, and what did the export have to fix before it could work?
6. How is the Snowflake load made idempotent?
7. Where do Snowflake credentials live, and where must they never be?

## 6. Learning material

**O'Reilly**
- *Snowflake Data Engineering* (Manning, on the O'Reilly platform): stages, `COPY INTO`, warehouses and cost.
- *Data Engineering Design Patterns* (Konieczny, 2025): idempotent loads and the patterns used across this project.
- *Fundamentals of Data Engineering*: ch. 3, "Designing Good Data Architecture", and ch. 4, "Choosing Technologies Across the Data Engineering Lifecycle".

**Vendor documentation**
- Databricks: Free Edition, Unity Catalog volumes, and serverless compute limitations.
- Snowflake: "Loading data", `COPY INTO <table>` (Parquet, `MATCH_BY_COLUMN_NAME`), warehouses and auto-suspend, key-pair authentication.

**Zoomcamp**
- The final project brief: the same end-to-end shape, on a cloud of your choice.

## 7. Done when

- [ ] the notebook ran on Databricks and its timings are a row in [`RESULTS.md`](../RESULTS.md)
- [ ] `load-snowflake` printed `ok` for every table, and that is a row too
- [ ] you can answer every question in section 5 without the notes
- [ ] every step has a story in [`STORIES.md`](../STORIES.md)
