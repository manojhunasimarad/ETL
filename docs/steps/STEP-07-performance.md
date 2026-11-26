# Step 7: Performance tuning (Spark and SQL)

**Goal:** make the pipeline and the queries faster, and prove it with
measurements on the same data and machine, one change at a time.

**JD lines this answers:** "perform SQL and pipeline tuning to optimize
performance at scale".

---

## 1. Concepts

### 1.1 How Spark runs a job

An action (`write`, `count`) becomes a **job**. The job is cut into
**stages** at every shuffle (wide operation). Each stage runs one **task**
per partition. So the number of partitions decides the number of tasks,
and every shuffle writes data to disk and reads it back over the network
(locally: through files). Tuning Spark mostly means *shuffling less* and
*having the right number of right-sized partitions*.

Open http://localhost:4040 while a job runs: the Spark UI shows jobs,
stages, tasks, shuffle sizes and the SQL plan with the join strategy used.
In Docker, publish the port when you start the job (`docker compose run --rm
-p 4040:4040 pipeline ...`); otherwise the browser cannot reach it.

### 1.2 Shuffle partitions

`spark.sql.shuffle.partitions` (default 200) sets how many partitions
every shuffle produces. On a laptop with 8 cores and a few hundred MB,
200 means 200 tiny tasks per shuffle: scheduling overhead dominates. On a
cluster with terabytes, 200 may be far too few (huge tasks, spills to
disk). Rule of thumb: partitions of roughly 100-200 MB, and at least 2-3x
the number of cores.

### 1.3 Join strategies

| Strategy | How | When |
|---|---|---|
| **Sort-merge join** | shuffle both sides by key, sort, merge | two large tables |
| **Broadcast hash join** | copy the small table to every executor, no shuffle of the big one | one side small (dimensions!) |
| Shuffle hash join | shuffle both, hash the smaller per partition | medium tables |

A star schema is made for broadcast joins: a big fact, small dimensions.
`autoBroadcastJoinThreshold` (default 10 MB) lets Spark decide;
`F.broadcast(df)` forces it (used in `gold.py` when `broadcast_dims=True`).

### 1.4 Adaptive Query Execution (AQE)

With AQE, Spark re-plans *during* the job using real statistics from
finished stages: it **coalesces** small shuffle partitions, **switches**
a sort-merge join to a broadcast join when one side turns out small, and
**splits skewed** partitions. It is on by default since Spark 3.2, which
is why the baseline here explicitly switches it off.

### 1.5 Data skew

The generator gives products power-law popularity: a few products appear
in many order lines. In a shuffle on `product_id`, the task holding the
most popular product gets far more rows than the others and the stage
waits for it. Broadcasting the product dimension avoids that shuffle;
AQE's skew-join handling splits the hot partition when a shuffle cannot
be avoided. In the Spark UI, look for one task much slower than the median.

### 1.6 Data layout: partitioning, pruning, files

* **Partitioning on disk** (`partitionBy("order_month")`): a filter on the
  partition column opens only matching folders (**partition pruning**).
  Choose a column you filter on often with a moderate number of values;
  partitioning by `order_id` would create millions of tiny files.
* **Parquet** helps further: **column pruning** (read only the needed
  columns) and **predicate pushdown** (skip row groups using min/max
  statistics, which works best when data is sorted by the filter column).
* **Small files problem**: thousands of tiny files are slow to list and
  open. Coalesce before writing, or let AQE size the output.

### 1.7 SQL tuning in Postgres

* Postgres indexes primary keys automatically, **not foreign keys**. A
  query that filters a fact by `customer_sk` without an index reads the
  whole table (**Seq Scan**).
* A B-tree index turns a selective filter into an **Index Scan** or
  **Bitmap Heap Scan**. For filters that match a large share of the table,
  a sequential scan is still cheaper, and the planner knows it: an index
  is not always used, and that is correct.
* `EXPLAIN (ANALYZE)` runs the query and shows the real plan and time.
  Read it bottom-up: scans, then joins, then aggregation.
* `ANALYZE` refreshes the statistics the planner relies on; run it after
  big loads (the pipeline does).

### 1.8 How to benchmark honestly

* Same data, same machine, same code; change **one** thing at a time.
* Warm up first (JVM JIT, OS file cache), then take the **median** of
  several runs.
* Report the machine and data size with every number.
* Do not tune the baseline to be bad on purpose: here it is Spark's
  behaviour with AQE and broadcast joins switched off, which is what
  older Spark versions did by default, and every step after it is a
  standard, explainable change.

---

## 2. Code tour

1. [`spark/benchmark.py`](../../src/ecom_etl/spark/benchmark.py): `configs()` (the ablation), `run_spark_benchmark`, `run_layout_benchmark`.
2. [`sql_tuning.py`](../../src/ecom_etl/sql_tuning.py) and [`sql/gold_indexes.sql`](../../src/ecom_etl/sql/gold_indexes.sql).
3. [`.github/workflows/benchmark.yml`](../../.github/workflows/benchmark.yml): the same benchmark on a GitHub runner.

## 3. Run it

On a laptop with 8 GB of RAM (Spark gets 2 GB by default), start at 50,000
orders and go to 200,000 only if that finishes comfortably. Close the browser
tabs you do not need while it runs.

```powershell
docker compose run --rm pipeline generate --orders 50000
docker compose run --rm -p 4040:4040 pipeline benchmark --source synthetic --repeat 2
```

While it runs, open http://localhost:4040, SQL tab: find `SortMergeJoin` in the
baseline run and `BroadcastHashJoin` in the tuned one. `--orders 200000` writes
201,874 order rows, because about 1% are injected duplicates.

The report is written to `reports/benchmark_synthetic.md`. Run it on your
own machine for the numbers you put on your resume, and record them in
[`RESULTS.md`](../RESULTS.md) together with the machine description.

## 4. Results

Measured by the [benchmark workflow](../../.github/workflows/benchmark.yml) on a
GitHub-hosted runner (4 cores, 8 GB driver memory, Spark 4.1.4, Python 3.11):
201,874 synthetic orders, 915,177 bronze records, median of 2 runs after a warm-up.

| config | change | silver (s) | gold (s) | total (s) | speedup |
|---|---|---:|---:|---:|---:|
| baseline | AQE off, broadcast joins off, 200 shuffle partitions | 119.1 | 57.8 | 176.9 | 1.0x |
| +partitions | shuffle partitions sized to the machine (8) | 30.5 | 9.0 | 39.5 | 4.5x |
| +broadcast | broadcast joins for small tables and dimensions | 26.2 | 6.8 | 33.0 | 5.4x |
| tuned | + adaptive query execution | 26.6 | 6.9 | 33.6 | 5.3x |

What it means:

* **Shuffle partitions did most of the work** (176.9 s → 39.5 s). With 200
  partitions per shuffle on 4 cores, most of the time went into scheduling
  hundreds of tiny tasks for every join, `groupBy`, window and `dropDuplicates`.
* **Broadcast joins** removed the shuffle of the large side in the foreign-key
  checks and the fact joins: another 16%.
* **AQE added nothing here**, within noise. Its main effect, coalescing
  small shuffle partitions, had already been done by hand in the row above,
  and the dimensions were already broadcast. AQE earns its keep when you
  *cannot* size partitions by hand (data size varies run to run) or when
  there is skew. Reporting a step that did not help is part of an honest benchmark.

Data layout, one month of revenue by category:

| fact table layout | data scanned (MB) | median (ms) |
|---|---:|---:|
| partitioned by `order_month` | 1.0 | 144.6 |
| not partitioned | 15.8 | 142.9 |

Pruning reads 15x less data, but at 16 MB both finish in about 0.14 s:
fixed overheads (planning, task start-up) dominate. The gap grows with the
data; at billions of rows it is the difference between minutes and seconds.

Postgres, secondary indexes on the fact table (`EXPLAIN ANALYZE`, median of 5):

| query | before (ms) | plan before | after (ms) | plan after | speedup |
|---|---:|---|---:|---|---:|
| one customer's order history | 12.5 | Seq Scan | 0.013 | Index Scan | 962x |
| one product's sales by month | 14.8 | Seq Scan | 5.9 | Bitmap Heap Scan | 2.5x |
| one month's revenue by category | 33.0 | Seq Scan | 16.2 | Bitmap Heap Scan | 2.0x |

The more selective the filter, the bigger the win: one customer is a
handful of rows (index scan), one month is about 5% of the table (bitmap
scan, still reads many pages).

## 5. Exercises

1. Run the benchmark at 50k, 200k and 1M orders. At which size does each optimization start to matter?
2. Open the Spark UI during the baseline and the tuned run of `gold`. Find the join strategy in the SQL tab (`SortMergeJoin` vs `BroadcastHashJoin`).
3. Find the skewed task: in the baseline silver run, which stage has one task much slower than the median?
4. Run `EXPLAIN ANALYZE` on Q3 from step 6 before and after the indexes. Did the plan change? Why or why not?
5. Partition the fact by `date_key` instead of `order_month`. Count the files and time the month query.

## 6. Check yourself

1. What is a shuffle, and which operations cause one?
2. How do you choose `spark.sql.shuffle.partitions`?
3. Broadcast hash join vs sort-merge join: when is each used?
4. What three things does AQE do?
5. What is data skew, how do you spot it, and how do you fix it?
6. Partition pruning vs predicate pushdown vs column pruning?
7. Why doesn't Postgres index foreign keys automatically, and when is an index *not* used?
8. How do you make a before/after performance claim trustworthy?

## 7. Learning material

**O'Reilly**
- *Learning Spark, 2nd Ed.*: ch. 7 "Optimizing and Tuning Spark Applications" (shuffle partitions, caching, join strategies, the Spark UI) and ch. 12 on Spark 3 features (AQE).
- *High Performance Spark* (Karau and Warren): joins and partitioning chapters.
- *SQL Performance Explained* (Markus Winand; also free online as "Use The Index, Luke"): how B-tree indexes work.

**Zoomcamp**
- [Module 06-batch](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/06-batch): GroupBy and join internals.
- [Module 03-data-warehouse](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/03-data-warehouse): partitioning and clustering (BigQuery), the same idea as here.

## 8. Done when

- [ ] you ran the benchmark on your machine and recorded the numbers in `RESULTS.md`
- [ ] you can explain each row of the benchmark table and why it got faster
- [ ] you found a broadcast join and a sort-merge join in the Spark UI
- [ ] your Step 7 story is in [`STORIES.md`](../STORIES.md)
