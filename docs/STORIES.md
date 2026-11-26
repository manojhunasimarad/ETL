# Interview stories

One short story per step, written by me after the step is merged. Each
answers "tell me about a problem you solved in this project" in under a
minute. Numbers must come from [`RESULTS.md`](RESULTS.md).

Format:
- **Problem**: what was wrong or unknown, in one sentence
- **Action**: what I built or decided, and why that way
- **Result**: the measured outcome
- **Trade-off / next time**: what I would change, or the limitation

---

## Step 1: Data sources and profiling

- **Problem**: (fill in after profiling the real Olist data: which issues did it have that a naive pipeline would load silently?)
- **Action**: wrote a source contract for 7 tables, a profiler that reads everything as text, and a synthetic generator with ground-truth defects to test the profiler against.
- **Result**: (fill in: number of real Olist findings; synthetic: all 14 injected defect types detected with exact counts)
- **Trade-off / next time**: (fill in)

## Step 2: Postgres landing zone and normalization

- **Problem**: the CSV files carry duplicates, bad timestamps, orphans and wrong-case statuses; one bad row aborts a whole `INSERT`.
- **Action**: loaded raw files as text with `COPY` (the load cannot fail on a value), then built a 3NF core in SQL: deduplicate first, standardise, validate with `pg_input_is_valid`, drop children of rejected parents, insert under constraints, all in one transaction.
- **Result**: core row counts equal the Spark silver counts exactly (4,805 orders, 5,333 order lines, 4,914 payments for 5,000 generated orders): two implementations of one contract agree.
- **Trade-off / next time**: (fill in)

## Step 3: Batch with PySpark, bronze to silver

- **Problem**: dropping bad rows hides them; casting with ANSI mode on fails the whole job.
- **Action**: every contract rule became a Spark expression returning a reason code; rows with no reasons go to silver, the rest to quarantine with all their reasons; safe casts with `try_cast`.
- **Result**: see the quarantine counts per reason in the silver report; nothing disappears without a reason.
- **Trade-off / next time**: (fill in)

## Step 4: Star schema and SCD Type 2

- **Problem**: an order placed before a customer moved must still report the old city.
- **Action**: `dim_customer` as SCD Type 2 built with `lag`, a running `sum` and `lead`; the fact picks the version with a half-open point-in-time join; `xxhash64` surrogate keys and a `-1` unknown member.
- **Result**: (fill in: how many people have more than one version in your run, from `q09_customers_who_moved.sql`)
- **Trade-off / next time**: (fill in)

## Step 5: Data quality framework

- **Problem**: "the data looks fine" cannot be tested; a quality check also needs to be measured.
- **Action**: 115 checks (80 row-level rules generated from the contract, 35 table-level gates), blocking gates as write-audit-publish, results stored per run, precision and recall against the generator's ground truth.
- **Result**: 20,000 orders at 1% defects: 3,497 rows quarantined, precision and recall 100% (`RESULTS.md`).
- **Trade-off / next time**: (fill in)

## Step 6: SQL analysis and dashboard

- **Problem**: a star schema is only useful if questions are answered on it correctly.
- **Action**: ten business questions in SQL (CTEs, window functions) and a Streamlit dashboard tested in CI.
- **Result**: (fill in: the one finding you would show a stakeholder first)
- **Trade-off / next time**: (fill in)

## Step 7: Performance tuning

- **Problem**: silver and gold took 176.9 s on 201,874 orders on a 4-core runner.
- **Action**: measured one change at a time: shuffle partitions sized to the machine, broadcast joins for dimensions, then AQE; indexes on the fact's foreign keys in Postgres.
- **Result**: 33.0 s (5.4x); AQE added nothing on top; one customer's order history 12.5 ms → 0.013 ms.
- **Trade-off / next time**: (fill in)

## Step 8: Orchestration with Airflow

- **Problem**: a pipeline run by hand is not a pipeline.
- **Action**: an Airflow 3 DAG with the quality gates as their own tasks (no retries), idempotent tasks, params for the source, and `airflow dags test` in CI.
- **Result**: all eight tasks green in CI on every push; a failed gate stops everything downstream.
- **Trade-off / next time**: (fill in)

## Step 9: Streaming with Kafka

- **Problem**: a stream delivers duplicates, late and malformed messages, and a restart replays what was already processed.
- **Action**: event-time windows with watermarks, deduplication by `event_id`, per-query checkpoints and an idempotent `foreachBatch` upsert into Postgres.
- **Result**: 3,063 messages (63 duplicates, 34 late, 32 malformed) → 2,968 silver events; daily revenue equal to the producer's total to the cent.
- **Trade-off / next time**: (fill in)

## Step 10: Databricks and Snowflake

- **Problem**: Databricks Free Edition's serverless compute refuses `persist()`, and switching caching off revealed that `dropDuplicates` keeps an arbitrary copy, which can change results when a DataFrame is recomputed.
- **Action**: made caching optional (`ECOM_NO_CACHE`), made exact-duplicate removal keep the first loaded copy, exported gold with every column and microsecond timestamps, and wrote an idempotent Snowflake load with count checks.
- **Result**: (fill in after running: Databricks timings and Snowflake counts from `RESULTS.md`)
- **Trade-off / next time**: (fill in)

## Step 11: LLM enrichment of review comments

- **Problem**: star scores say how happy customers were but not why; the why is in Portuguese free text, and a model call is the most expensive operation in the pipeline, with a daily quota.
- **Action**: labelled comments with an LLM (sentiment, topics, follow-up) behind one client for Gemini and OpenAI-compatible servers; cut calls with deduplication, a content-hash cache and batching; validated every answer, retried failures in smaller batches and quarantined the rest behind a 2% gate; measured against the star score and a free keyword baseline.
- **Result**: (fill in after the Olist run: comments/s on which GPU, share removed by dedupe, quarantine share, agreement vs baseline, top complaint topics)
- **Trade-off / next time**: (fill in)
