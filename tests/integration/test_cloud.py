"""Step 10: the code paths the cloud platforms use, run on local Spark.

* Databricks serverless refuses persist(): run_batch must work with caching
  off (ECOM_NO_CACHE=1) and publish gold as tables.
* Snowflake loads the export: every column in the files (including the fact's
  partition column), timestamps as microseconds, a type for every column.
"""
import json

import pytest

from ecom_etl import config
from ecom_etl.snowflake import Target, script, snowflake_type

pytestmark = pytest.mark.spark


def test_run_batch_with_caching_off_publishes_gold(spark, tmp_path, monkeypatch):
    from ecom_etl.spark.databricks import run_batch

    monkeypatch.setenv("ECOM_NO_CACHE", "1")
    monkeypatch.setattr(config, "REPORTS_DIR", tmp_path / "reports")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS ecom_cloud_test LOCATION '{(tmp_path / 'warehouse').as_posix()}'")
    result = run_batch(spark, tmp_path / "volume", orders=2000, seed=11, defect_rate=0.01,
                       publish_to="ecom_cloud_test")
    assert result["status"] == "success"
    assert result["published"] == result["gold_rows"]
    assert result["gold_rows"]["fact_order_line"] > 0
    assert result["checks"]["blocking_failures"] == 0
    assert result["evaluation"]["quarantine"]["precision"] == 1.0
    assert result["evaluation"]["quarantine"]["recall"] == 1.0
    assert set(result["seconds"]) >= {"generate", "bronze", "silver", "gold", "publish", "total"}


def test_export_for_snowflake_keeps_every_column(spark, dirty_lake, tmp_path):
    from ecom_etl.spark.export import export_for_snowflake, read_manifest
    from ecom_etl.spark.gold import GOLD_TABLES

    lake, _, gold = dirty_lake
    before = spark.conf.get("spark.sql.parquet.outputTimestampType")
    manifest = export_for_snowflake(spark, lake, tmp_path / "export")
    assert spark.conf.get("spark.sql.parquet.outputTimestampType") == before   # restored afterwards
    assert list(manifest) == list(GOLD_TABLES)
    assert read_manifest(tmp_path / "export") == json.loads(json.dumps(manifest))
    for table, info in manifest.items():
        assert info["rows"] == gold[table], table
        for _, dtype in info["columns"]:
            snowflake_type(dtype)  # raises if a column has no Snowflake type
    fact_columns = [c for c, _ in manifest["fact_order_line"]["columns"]]
    assert "order_month" in fact_columns
    files = list((tmp_path / "export" / "fact_order_line").glob("*.parquet"))
    assert files and not list((tmp_path / "export" / "fact_order_line").glob("order_month=*"))
    text = script(manifest, Target())
    assert text.count("COPY INTO") == len(GOLD_TABLES)

    pq = pytest.importorskip("pyarrow.parquet")
    schema = pq.read_schema(files[0])
    stamps = [f for f in schema if str(f.type).startswith("timestamp")]
    assert stamps and all(f.type.unit == "us" for f in stamps), schema
