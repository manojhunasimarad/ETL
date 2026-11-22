# E-commerce Batch and Streaming ETL Pipeline

Batch and streaming pipelines that ingest e-commerce orders, customers and
payments into a star schema (grain: one row per order line), with
automated data quality checks, Airflow orchestration and Docker.

Built on two datasets with the same schema:

- **Olist**: the real Brazilian e-commerce dataset from Kaggle (about 100k orders, real data quality problems).
- **Synthetic**: an Olist-shaped generator that scales to millions of rows and records every defect it injects, so the quality checks can be measured against ground truth.

Stack (when complete): Python, PySpark, SQL, Postgres, Streamlit, Airflow, Kafka (Redpanda), Docker, Databricks, Snowflake.

## Status

Built step by step, following the order of the
[DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp).
See the [roadmap](docs/ROADMAP.md).

| Step | Topic | Status |
|---|---|---|
| 1 | [Data sources and profiling](docs/steps/STEP-01-data-sources.md) | **done** |
| 2 | [Postgres landing zone and normalization](docs/steps/STEP-02-postgres.md) | **done** |
| 3 | [Batch with PySpark: bronze to silver](docs/steps/STEP-03-spark.md) | **done** |
| 4 | [Dimensional model (star schema, SCD Type 2)](docs/steps/STEP-04-star-schema.md) | **done** |
| 5 | [Data quality framework](docs/steps/STEP-05-data-quality.md) | **done** |
| 6 | [SQL analysis and dashboard](docs/steps/STEP-06-analysis-dashboard.md) | **done** |
| 7 | [Performance tuning (Spark and SQL)](docs/steps/STEP-07-performance.md) | **done** |
| 8 | Orchestration with Airflow | next |
| 9 | Streaming with Kafka and Spark Structured Streaming | |
| 10 | Databricks, Snowflake, final numbers | |

How each step is learned and built: [docs/WORKFLOW.md](docs/WORKFLOW.md).

## Quickstart (step 1)

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1      # Windows PowerShell; on Linux/WSL: source .venv/bin/activate
pip install -e ".[dev]"
pytest -q

python -m ecom_etl generate                 # synthetic data -> data/raw/synthetic
python -m ecom_etl profile                  # report -> reports/profile_synthetic.md

pip install -e ".[kaggle]"
python -m ecom_etl download-olist           # real data -> data/raw/olist
python -m ecom_etl profile --source olist
```

## Repository layout

```
src/ecom_etl/
  contracts.py    source data contract: tables, types, keys, rules
  generator.py    synthetic Olist-shaped data + ground-truth defects
  profiling.py    data profiler and markdown report
  olist.py        Kaggle download and header validation
  cli.py          python -m ecom_etl <command>
tests/            pytest suite
docs/
  ROADMAP.md      the 10 steps, architecture, learning material
  WORKFLOW.md     how each step is learned, built and merged
  RESULTS.md      measured numbers only
  STORIES.md      interview stories, one per step
  steps/          one study note per step
  samples/        example outputs
```

## Data

Olist data: "Brazilian E-Commerce Public Dataset by Olist", Kaggle,
CC BY-NC-SA 4.0. It is downloaded locally and never committed.
