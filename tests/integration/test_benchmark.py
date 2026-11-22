"""Step 7: the benchmark machinery works (timings themselves are not asserted)."""

import pytest

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def bench_lake(spark, dirty_source, tmp_path_factory):
    from ecom_etl.spark.bronze import run_bronze
    from ecom_etl.spark.lake import Lake

    out, manifest = dirty_source
    lake = Lake(tmp_path_factory.mktemp("lake_bench"))
    run_bronze(spark, out, lake, "bench")
    return lake, manifest["extracted_at"]


def test_spark_benchmark_restores_settings(spark, bench_lake):
    from ecom_etl.spark.benchmark import run_spark_benchmark

    lake, extracted_at = bench_lake
    before = spark.conf.get("spark.sql.shuffle.partitions")
    rows = run_spark_benchmark(spark, lake, extracted_at, repeat=1, warmup=False, only=["baseline", "tuned"])
    assert [r["config"] for r in rows] == ["baseline", "tuned"]
    assert all(r["total_s"] > 0 for r in rows)
    assert rows[0]["speedup_vs_baseline"] == 1.0
    assert spark.conf.get("spark.sql.shuffle.partitions") == before


def test_partition_pruning_scans_less_data(spark, bench_lake):
    from ecom_etl.spark.benchmark import run_layout_benchmark

    lake, _ = bench_lake
    layout = run_layout_benchmark(spark, lake, repeat=1)
    assert layout["partitioned_mb_scanned"] < layout["unpartitioned_mb_scanned"]


@pytest.mark.postgres
def test_sql_benchmark_reports_every_query(spark, bench_lake):
    from ecom_etl.spark.warehouse import load_gold
    from ecom_etl.sql_tuning import run_sql_benchmark

    lake, _ = bench_lake
    load_gold(spark, lake, "t_bench")
    rows = run_sql_benchmark("t_bench", repeat=2)
    assert len(rows) == 3
    assert all(r["before_ms"] > 0 and r["after_ms"] > 0 for r in rows)
    assert all("Scan" in r["before_scan"] for r in rows)
