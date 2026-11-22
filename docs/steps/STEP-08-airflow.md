# Step 8: Orchestration with Airflow

**Goal:** run the batch pipeline on a schedule, with retries, visible
history, and quality gates that stop bad data from being published.

**JD lines this answers:** "familiarity with data orchestration/ETL tools
(e.g., Airflow)"; build and maintain data pipelines.

---

## 1. Concepts

### 1.1 What an orchestrator does (and does not do)

An orchestrator decides **when** tasks run, in **which order**, what
happens when one **fails** (retry, alert, stop), and keeps a **history**
of every run. It should not contain the business logic: here every task
is one command of this project (`python -m ecom_etl silver ...`), so the
same code runs from a terminal, from Docker, from CI and from Airflow.

### 1.2 Airflow vocabulary

| Term | Here |
|---|---|
| **DAG** (directed acyclic graph) | `ecom_batch_pipeline` in [`airflow/dags/ecom_pipeline.py`](../../airflow/dags/ecom_pipeline.py) |
| **Task** / **operator** | 8 `BashOperator` tasks, one per stage |
| **Dependencies** | `extract >> bronze >> silver >> silver_gate >> gold >> gold_gate >> load_gold >> analyze` |
| **Schedule** | `@daily`; `catchup=False` (do not run every missed day since the start date) |
| **DAG run** / **logical date** | one execution of the whole DAG for a point in time |
| **Params** | `source` (synthetic or olist) and `orders`, set when triggering |
| **Retries** | 2 per task, 1 minute apart |

Airflow 3 components (all inside one container with `airflow standalone`
for local development): the **API server** (UI and REST API), the
**scheduler** (decides what runs), the **DAG processor** (parses DAG
files), the **triggerer** (deferred tasks), and a **metadata database**
(Postgres here, database `airflow`). In production they run as separate
services, often on Kubernetes.

### 1.3 Design choices worth defending

* **Idempotent tasks.** Every task overwrites its own layer, so re-running
  a task or a whole DAG run gives the same result. This is what makes
  retries and manual re-runs safe.
* **Retries only where they help.** A Spark task can fail for transient
  reasons (memory, a busy laptop), so it retries. A quality gate failure
  is a *data* problem; retrying will fail again, so gates have `retries=0`.
* **Gates as tasks.** `silver_gate` and `gold_gate` exit with code 1 when a
  blocking check fails. Airflow marks the task failed and the downstream
  tasks never run: `load_gold` never publishes a broken star schema. The
  failure is visible in the UI, with the report in the task log.
* **One task per stage** gives per-stage timing, logs and retries. The
  cost: each task starts its own Spark JVM (a few seconds). Running the
  whole pipeline as one task would be faster but opaque.
* **`max_active_runs=1`**: two runs writing the same lake at once would
  corrupt each other.

### 1.4 Backfills and logical dates

A daily pipeline normally processes "the data for its logical date". This
project's sources are full snapshots, so every run reprocesses the whole
extract and the logical date does not select data. With incremental
sources you would pass `{{ ds }}` to the extract and write
date-partitioned outputs; then `airflow dags backfill` re-runs history
day by day, and idempotent tasks make that safe.

### 1.5 Why Airflow and not Kestra

The Zoomcamp's 2026 cohort teaches orchestration with Kestra. The job
description names Airflow, and the concepts (DAGs, retries, schedules,
idempotency, backfills) transfer directly between the two.

---

## 2. Run it

```bash
docker compose --profile airflow build airflow    # first time: a few minutes
docker compose --profile airflow up -d airflow
```

Open http://localhost:8080 (no login in this local setup), find
`ecom_batch_pipeline`, switch it on, and press **Trigger** (optionally
"Trigger with config": `{"source": "olist"}`). Watch the Graph view; open
a task's log to see the CLI output. On Linux, put `AIRFLOW_UID=<your id -u>`
in `.env` first so the files written to `./data` stay yours.

Test a DAG without the UI:

```bash
docker compose --profile airflow exec airflow airflow dags test ecom_batch_pipeline
```

CI runs exactly that on every push (job `airflow`).

## 3. Exercises

1. Make `silver_gate` fail (lower `MAX_QUARANTINE_RATE` to 1%) and trigger the DAG. Which tasks run, which are skipped?
2. Add an `on_failure_callback` that prints which gate failed and the path of the report.
3. Add a sensor task that waits for the Olist files to exist before `extract`.
4. Change the schedule to weekdays at 06:00 IST. What is the cron expression, and in which time zone does Airflow interpret it?
5. Why is `catchup=False` important here? What would happen with `True`?

## 4. Check yourself

1. What does an orchestrator do that a cron job does not?
2. What makes a task idempotent, and why does it matter for retries and backfills?
3. Why do the quality gates have no retries?
4. Name the Airflow 3 components and what each does.
5. What is the logical date of a DAG run?
6. One task per stage vs one big task: trade-offs?

## 5. Learning material

**O'Reilly**
- *Data Pipelines with Apache Airflow* (Manning, on the O'Reilly platform): DAGs, scheduling, templating, testing; the 2nd edition covers Airflow 3.
- *Data Pipelines Pocket Reference* (Densmore): orchestration and DAG design.

**LinkedIn Learning**
- *Hands-On Introduction: Data Engineering*: ETL, tasks, DAGs and dependencies with Airflow.

**Udemy**
- *The Complete Hands-On Introduction to Apache Airflow* (Marc Lamberti); check that the version you pick covers Airflow 3.

**Zoomcamp**
- [Module 02-workflow-orchestration](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/02-workflow-orchestration): the same concepts with Kestra.

## 6. Done when

- [ ] the DAG runs green in the Airflow UI on your machine
- [ ] you have seen a gate stop the DAG and found the reason in the logs
- [ ] you can explain idempotency, retries and catchup with this DAG
- [ ] your Step 8 story is in [`STORIES.md`](../STORIES.md)
