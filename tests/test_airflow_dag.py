"""Step 8: the DAG imports cleanly and has the expected shape.

Runs only where Airflow is installed (the 'airflow' CI job); skipped elsewhere.
"""

from pathlib import Path

import pytest

DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dag():
    pytest.importorskip("airflow.sdk")  # the repo's airflow/ folder is not Airflow itself
    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:
        from airflow.models.dagbag import DagBag
    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag.dags["ecom_batch_pipeline"]


def test_tasks_run_in_pipeline_order(dag):
    order = ["extract", "bronze", "silver", "silver_gate", "gold", "gold_gate", "load_gold", "analyze"]
    assert set(dag.task_ids) == set(order)
    for upstream, downstream in zip(order, order[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids


def test_quality_gates_are_not_retried(dag):
    assert dag.get_task("silver_gate").retries == 0
    assert dag.get_task("gold_gate").retries == 0
    assert dag.get_task("silver").retries == 2


def test_no_catchup_and_one_run_at_a_time(dag):
    assert dag.catchup is False
    assert dag.max_active_runs == 1
