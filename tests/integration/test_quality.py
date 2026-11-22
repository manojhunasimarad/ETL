"""Step 5: data quality checks, gates and measurement against ground truth."""

import pytest

from ecom_etl.spark.quality import (
    FAIL,
    QualityGateError,
    evaluate_against_ground_truth,
    gate,
    gold_gate_results,
    row_level_checks,
    row_level_results,
    silver_gate_results,
)

pytestmark = pytest.mark.spark

EXPECTED_ROW_COUNTS = {
    "orders.not_null:customer_id": ("orders", "null_customer_id"),
    "orders.accepted_values:order_status": ("orders", "invalid_status"),
    "orders.sequence:delivered_before_purchase": ("orders", "delivered_before_purchase"),
    "orders.future_event": ("orders", "future_purchase"),
    "order_items.range:price": ("order_items", "non_positive_price"),
    "order_payments.accepted_values:payment_type": ("order_payments", "invalid_payment_type"),
    "customers.exact_duplicate": ("customers", "duplicate_row"),
    "orders.exact_duplicate": ("orders", "duplicate_row"),
    "order_payments.exact_duplicate": ("order_payments", "duplicate_row"),
}


def test_every_silver_reason_maps_to_a_registered_check(dirty_lake):
    _, silver, _ = dirty_lake
    names = {c.name for c in row_level_checks()}
    fk_names = {n for n in names if ".fk:" in n}
    for table, report in silver.items():
        for reason in list(report.reasons) + list(report.warnings):
            if reason.startswith("parent_quarantined:"):
                assert any(n.startswith(f"{table}.fk:") and n.endswith(reason.split(":")[1]) for n in fk_names)
            else:
                assert f"{table}.{reason}" in names, reason


def test_row_level_results_match_injected_defects(dirty_lake, dirty_source):
    _, manifest = dirty_source
    _, silver, _ = dirty_lake
    injected = {(d["table"], d["defect_type"]): d["count"] for d in manifest["defects"]}
    results = {r.name: r for r in row_level_results(silver)}
    for check, defect in EXPECTED_ROW_COUNTS.items():
        assert results[check].failed_rows == injected[defect], check


def test_gates_pass_on_synthetic_data(spark, dirty_lake, dirty_source):
    """The fixture injects every defect type at 2%; with cascades about 12% of
    order lines are quarantined, which the default 10% gate rightly blocks.
    (At the CLI default of 1% it is about 6%.) Allow 25% for this fixture."""
    _, manifest = dirty_source
    lake, silver, _ = dirty_lake
    results = row_level_results(silver)
    results += silver_gate_results(spark, lake, silver, manifest["extracted_at"], max_quarantine_rate=0.25)
    results += gold_gate_results(spark, lake)
    gate(results)  # must not raise
    fresh = next(r for r in results if r.name == "orders.freshness")
    assert fresh.status != FAIL


def test_quarantine_is_perfect_against_ground_truth(spark, dirty_lake, dirty_source):
    out, _ = dirty_source
    lake, silver, _ = dirty_lake
    ev = evaluate_against_ground_truth(spark, lake, out, silver)
    assert ev["quarantine"]["precision"] == 1.0
    assert ev["quarantine"]["recall"] == 1.0
    assert all(v["handled"] == v["injected"] for v in ev["fixes"].values()), ev["fixes"]
    assert all(v["removed"] == v["injected"] for v in ev["dedupe"].values()), ev["dedupe"]


def test_gate_blocks_when_quarantine_rate_is_too_high(spark, dirty_lake, dirty_source):
    _, manifest = dirty_source
    lake, silver, _ = dirty_lake
    results = silver_gate_results(spark, lake, silver, manifest["extracted_at"], max_quarantine_rate=0.001)
    with pytest.raises(QualityGateError) as err:
        gate(results)
    assert any(f.name == "orders.quarantine_rate" for f in err.value.failures)


def test_freshness_warns_on_stale_data_without_blocking(spark, dirty_lake):
    lake, silver, _ = dirty_lake
    results = silver_gate_results(spark, lake, silver, "2030-01-01 00:00:00")
    fresh = next(r for r in results if r.name == "orders.freshness")
    assert fresh.status == FAIL and not fresh.blocking


@pytest.mark.postgres
def test_full_pipeline_with_gates_and_load(spark, clean_source, tmp_path_factory):
    from ecom_etl.db import connect, schema_name
    from ecom_etl.spark.lake import Lake
    from ecom_etl.spark.pipeline import run_pipeline

    out, manifest = clean_source
    lake = Lake(tmp_path_factory.mktemp("lake_pipeline"))
    report = run_pipeline(spark, out, "t_pipeline", lake, manifest["extracted_at"])
    assert report["status"] == "success"
    assert report["summary"]["records_quarantined"] == 0
    assert report["summary"]["blocking_failures"] == 0
    with connect() as conn:
        n = conn.execute(f"SELECT count(*) FROM {schema_name('t_pipeline', 'dq')}.check_results").fetchone()[0]
        facts = conn.execute(f"SELECT count(*) FROM {schema_name('t_pipeline', 'gold')}.fact_order_line").fetchone()[0]
    assert n == report["summary"]["checks_total"]
    assert facts == report["loaded_rows"]["fact_order_line"]
