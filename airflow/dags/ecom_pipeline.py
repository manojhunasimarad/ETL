"""Airflow DAG for the e-commerce batch pipeline (step 8).

    extract -> bronze -> silver -> silver_gate -> gold -> gold_gate -> load_gold -> analyze

Each task is one CLI command of this project, so the DAG holds no
business logic: it only decides order, retries and what happens on failure.
Every task overwrites its own layer, so re-running a task (or the whole
DAG for the same input) gives the same result: tasks are idempotent.

A quality gate exits with code 1 when a blocking check fails; Airflow marks
the task failed and the downstream tasks never run, so a broken star
schema is never loaded into Postgres.

Trigger with parameters from the UI ("Trigger DAG w/ config") or the CLI:
    airflow dags trigger ecom_batch_pipeline --conf '{"source": "olist"}'
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

CLI = "cd ${ECOM_HOME:-/opt/ecom} && python -m ecom_etl"
SOURCE = "{{ params.source }}"

with DAG(
    dag_id="ecom_batch_pipeline",
    description="Olist / synthetic e-commerce: bronze, silver, gold with quality gates",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    params={
        "source": Param("synthetic", enum=["synthetic", "olist"], description="which data set"),
        "orders": Param(20000, type="integer", minimum=1, description="synthetic orders to generate"),
    },
    tags=["ecom", "batch", "spark"],
) as dag:
    extract = BashOperator(
        task_id="extract",
        bash_command=(
            f'if [ "{SOURCE}" = "synthetic" ]; then {CLI} generate --orders {{{{ params.orders }}}}; '
            f"else {CLI} validate --source {SOURCE}; fi"
        ),
        doc_md="Synthetic: generate a fresh extract. Olist: check the downloaded files match the contract.",
    )
    bronze = BashOperator(task_id="bronze", bash_command=f"{CLI} bronze --source {SOURCE}")
    silver = BashOperator(task_id="silver", bash_command=f"{CLI} silver --source {SOURCE}")
    silver_gate = BashOperator(
        task_id="silver_gate",
        bash_command=f"{CLI} quality --source {SOURCE} --layer silver",
        retries=0,  # a failed quality gate is a data problem: retrying will not fix it
    )
    gold = BashOperator(task_id="gold", bash_command=f"{CLI} gold --source {SOURCE}")
    gold_gate = BashOperator(
        task_id="gold_gate",
        bash_command=f"{CLI} quality --source {SOURCE} --layer gold",
        retries=0,
    )
    load_gold = BashOperator(task_id="load_gold", bash_command=f"{CLI} load-gold --source {SOURCE}")
    analyze = BashOperator(task_id="analyze", bash_command=f"{CLI} analyze --source {SOURCE}")

    extract >> bronze >> silver >> silver_gate >> gold >> gold_gate >> load_gold >> analyze
