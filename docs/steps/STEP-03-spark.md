# Step 3: Batch processing with PySpark (bronze and silver)

**Goal:** a Spark job that turns raw CSV into clean, typed, deduplicated
Parquet, and puts every rejected row in a quarantine together with the
reason it was rejected.

**JD lines this answers:** working knowledge of ETL and Apache Spark;
build pipelines to ingest and transform data; automated data quality.

---

## 1. Concepts

### 1.1 What Spark is, in one paragraph

Spark splits data into **partitions** and processes them in parallel on
**executors** (on your laptop, `local[*]` = one executor thread per CPU
core). You describe *what* you want with DataFrame operations; nothing
runs until an **action** (`count`, `write`, `collect`). Spark then turns
the whole chain into an optimized plan (**lazy evaluation**, Catalyst
optimizer). **Narrow** operations (filter, select, withColumn) work
partition by partition; **wide** ones (join, groupBy, window, dropDuplicates)
need a **shuffle**: data moves between partitions, which is the expensive part.

### 1.2 Medallion layers

| Layer | Content | Here |
|---|---|---|
| bronze | the source as it arrived, all text, plus lineage | `spark/bronze.py` |
| silver | clean, typed, deduplicated, validated | `spark/silver.py` |
| quarantine | rejected rows with `_reasons` | written by `silver.py` |
| gold | modelled for analytics (star schema) | step 4 |

### 1.3 Why an explicit schema

`inferSchema=true` reads the file twice and guesses: zip `"01037"`
becomes the integer 1037, a column with one `"129,90"` becomes all text
or silently null. Bronze declares every column as `StringType` (from the
contract), so nothing is guessed or lost, and typing happens in silver
where failures can be caught and explained.

### 1.4 Lineage columns

| Column | How | Why |
|---|---|---|
| `_row_id` | `monotonically_increasing_id()` | file order: keep the *first* of duplicate keys |
| `_source_file` | `_metadata.file_name` | which file a row came from |
| `_ingested_at` | `current_timestamp()` | when |
| `_run_id` | run timestamp | which run produced it |

### 1.5 Silver, step by step

1. **Standardize** = the *fix* decisions from step 1, each one recorded in
   the `_fixes` array of the row (`normalized:order_status`,
   `decimal_comma:price`, `zero_padded:customer_zip_code_prefix`,
   `added_translation:product_category_name`).
2. **Exact duplicates**: `dropDuplicates` on all business columns.
3. **Safe casts**: Spark 4 runs in **ANSI mode** by default, so
   `cast("abc" as int)` raises an error and kills the job. `try_cast` and
   `try_to_timestamp` return NULL instead, and a rule turns "value present
   but did not convert" into the reason `type:<column>`.
4. **Reasons**: every rule is a `when(condition, 'reason')`; the row's
   reasons are collected with `array_compact(array(...))`. One pass, any
   number of rules, and a row can fail several at once.
5. **Duplicate keys**: `row_number()` over a window partitioned by the
   primary key, ordered by `_row_id`; rows after the first get `duplicate_key`.
6. **Foreign keys**: left-join the parent's valid keys *and* the parent's
   quarantined keys. Missing from both = `fk:` (a real orphan); only in the
   parent's quarantine = `parent_quarantined:` (a cascade). This is what
   lets us measure precision honestly in step 5.
7. **Split and write**: no reasons → silver (typed); any reason →
   quarantine (original text + reasons).

The invariant `bronze = exact duplicates + quarantine + silver` is
checked by a test for every table: nothing is ever silently dropped.

### 1.6 Partitioning on disk

Silver `orders` is written with `partitionBy("_purchase_month")`: one
folder per month. A query for one month reads one folder instead of the
whole table (**partition pruning**). Too many small partitions are as bad
as none; step 7 measures this.

### 1.7 Persist

Silver calls `.persist()` on the validated DataFrame because it is used
several times (silver write, quarantine write, counts, reason tallies).
Without it, Spark would recompute the whole chain for every action.

---

## 2. Code tour

1. [`spark/session.py`](../../src/ecom_etl/spark/session.py): one SparkSession builder, settings from environment.
2. [`spark/bronze.py`](../../src/ecom_etl/spark/bronze.py): about 40 lines; read it fully.
3. [`spark/silver.py`](../../src/ecom_etl/spark/silver.py): `standardize`, `contract_reasons`, `order_rules`, `foreign_key_checks`, then `validate_table` and `split_and_write`, finally `run_silver` (the special handling of categories).
4. [`tests/integration/test_spark_silver.py`](../../tests/integration/test_spark_silver.py): `test_quarantine_matches_ground_truth_exactly` is the key one.
5. [`docker/Dockerfile`](../../docker/Dockerfile): Python + Java 17 + PySpark.

---

## 3. Run it

Spark needs Java 17+. On Windows, Spark cannot write files without extra
Hadoop binaries, so run it **in Docker** (or in WSL):

```bash
docker compose up -d postgres
docker compose --profile pipeline build pipeline
docker compose run --rm pipeline generate
docker compose run --rm pipeline bronze --source synthetic
docker compose run --rm pipeline silver --source synthetic
```

In WSL or Linux with Java 17 installed:

```bash
pip install -e ".[dev,spark]"
python -m ecom_etl bronze --source synthetic
python -m ecom_etl silver --source synthetic
```

`silver` prints rows per table (bronze, duplicates, quarantine, silver)
and the quarantine reasons. Reports are saved in `data/lake/<source>/_reports/`.

Look at the data with PySpark:

```python
from ecom_etl.spark.session import get_spark
spark = get_spark()
q = spark.read.parquet("data/lake/synthetic/quarantine/order_items")
q.select("order_id", "price", "_reasons").show(truncate=False)
```

---

## 4. Exercises

1. Run silver on Olist. Which reasons appear? Compare with your step 1 decisions.
2. Count the `_warnings` on Olist orders (carrier pickup before approval).
   Would you quarantine them? What would that cost?
3. Remove `.persist()` in `validate_table`, run on 1M orders, and compare
   the time. Look at the Spark UI (http://localhost:4040 while it runs).
4. Read silver orders for one month only and call `.explain()`. Find
   `PartitionFilters` in the plan.
5. Why would `inferSchema` be wrong for `customer_zip_code_prefix`? Try it.

## 5. Check yourself

1. What is lazy evaluation, and which operations trigger work?
2. Narrow vs wide transformations: give two of each from `silver.py`.
3. Why safe casts in Spark 4 specifically?
4. How does silver tell an orphan from a child of a rejected parent, and why does it matter?
5. What does `persist()` save here, and when would it hurt?
6. What is partition pruning? How would you choose the partition column?
7. What does `bronze = duplicates + quarantine + silver` protect you from?

## 6. Learning material

**O'Reilly**
- *Learning Spark, 2nd Ed.* (Damji et al.): ch. 1-3 (concepts, DataFrame API), ch. 4 (Spark SQL), ch. 7 (caching, partitioning; preview of step 7).
- *Spark: The Definitive Guide*: chapters on DataFrames, joins and data sources, as reference.

**LinkedIn Learning**
- *Apache Spark Essential Training: Big Data Engineering* (Kumaran Ponnambalam).

**Udemy**
- *Taming Big Data with Apache Spark and Python* (Frank Kane) is a popular hands-on PySpark course; use it for practice exercises.

**Zoomcamp**
- [Module 06-batch](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/06-batch): Spark DataFrames, SQL, GroupBy and join internals.

## 7. Done when

- [ ] bronze and silver run on synthetic and Olist data (Docker or WSL)
- [ ] you can explain each reason code and where it is produced
- [ ] you can draw the silver flow (standardize → dedupe → type → validate → split) from memory
- [ ] your Step 3 story is in [`STORIES.md`](../STORIES.md)
