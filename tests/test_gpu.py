"""GPU support without a GPU: settings, version checks, plan accounting, the report.

The GPU run itself needs an NVIDIA GPU and is done on a workstation
(scripts/gpu/run_gpu_benchmark.sh); CI checks everything around it, and runs
the benchmark harness with the CPU engine.
"""
import sys
import types

import pytest

from ecom_etl.spark import gpu
from ecom_etl.spark.gpu_benchmark import compare, render_markdown, same_result


def test_spark_version_check_names_the_fix():
    gpu.check_spark_version("4.1.3")
    gpu.check_spark_version("4.0.4")
    with pytest.raises(RuntimeError, match="pyspark==4.1.3"):
        gpu.check_spark_version("4.1.4")


def test_rapids_conf_defaults_and_overrides(monkeypatch):
    conf = gpu.rapids_conf()
    assert conf["spark.plugins"] == "com.nvidia.spark.SQLPlugin"
    assert conf["spark.rapids.sql.concurrentGpuTasks"] == "1"
    assert conf["spark.rapids.sql.batchSizeBytes"] == "256m"
    assert "spark.rapids.memory.pinnedPool.size" not in conf   # WSL2 limits pinned memory
    monkeypatch.setenv("ECOM_RAPIDS_CONCURRENT_TASKS", "2")
    monkeypatch.setenv("ECOM_RAPIDS_PINNED_POOL", "256m")
    conf = gpu.rapids_conf()
    assert conf["spark.rapids.sql.concurrentGpuTasks"] == "2"
    assert conf["spark.rapids.memory.pinnedPool.size"] == "256m"


@pytest.fixture
def fake_pyspark(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyspark", types.SimpleNamespace(__version__="4.1.3"))


def test_add_rapids_uses_a_local_jar_and_keeps_other_jars(fake_pyspark, monkeypatch, tmp_path):
    jar = tmp_path / "rapids.jar"
    jar.write_bytes(b"x")
    monkeypatch.setenv("ECOM_RAPIDS_JAR", str(jar))
    settings, packages = {"spark.jars": "/opt/postgresql.jar"}, []
    gpu.add_rapids(settings, packages)
    assert settings["spark.jars"] == f"/opt/postgresql.jar,{jar}"
    assert packages == [] and settings["spark.plugins"] == "com.nvidia.spark.SQLPlugin"


def test_add_rapids_falls_back_to_maven(fake_pyspark, monkeypatch, tmp_path):
    monkeypatch.delenv("ECOM_RAPIDS_JAR", raising=False)
    monkeypatch.setattr(gpu, "default_jar_path", lambda version=gpu.RAPIDS_VERSION: tmp_path / "missing.jar")
    settings, packages = {}, []
    gpu.add_rapids(settings, packages)
    assert packages == [f"com.nvidia:rapids-4-spark_2.13:{gpu.RAPIDS_VERSION}"]
    assert "spark.jars" not in settings


def test_unsupported_spark_is_refused_before_starting(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyspark", types.SimpleNamespace(__version__="4.1.4"))
    with pytest.raises(RuntimeError):
        gpu.add_rapids({}, [])


def test_operator_share_counts_work_not_wrappers():
    names = ["AdaptiveSparkPlan", "WholeStageCodegen (1)", "GpuScan parquet ", "GpuFilter", "GpuProject",
             "GpuHashAggregate", "GpuColumnarToRow", "HashAggregate", "Execute GpuInsertIntoHadoopFsRelationCommand",
             "Window", "Window", "InputAdapter", "ShuffleQueryStage", "GpuShuffleCoalesce"]
    share = gpu.operator_share(names)
    assert share["gpu_operators"] == 5 and share["cpu_operators"] == 3
    assert share["gpu_share"] == round(5 / 8, 3)
    assert share["cpu_operator_names"] == {"Window": 2, "HashAggregate": 1}
    assert gpu.operator_share([])["gpu_share"] is None


def _result(engine, seconds, fp):
    return {"engine": engine, "bronze_records": 100, "seconds": seconds, "fingerprint": fp,
            "settings": {"spark.sql.shuffle.partitions": "8"},
            "machine": {"spark": "4.1.3", "cores": 12, "driver_memory": "2g", "platform": "Linux"}}


def test_compare_and_report():
    fp = {"gold_rows": {"fact_order_line": 10}, "revenue": 100.0, "payments": 99.0, "quarantined": 3}
    cpu = _result("cpu", {"bronze": 4.0, "silver": 10.0, "gold": 6.0, "total": 20.0}, fp)
    gpu_r = _result("gpu", {"bronze": 2.0, "silver": 5.0, "gold": 3.0, "total": 10.0}, dict(fp, revenue=100.004))
    gpu_r["gpu"] = {"name": "NVIDIA GeForce GTX 1650", "memory": "4096 MiB", "driver": "581.15",
                    "compute_capability": "7.5"}
    gpu_r["operators"] = {"gpu_operators": 9, "cpu_operators": 1, "gpu_share": 0.9, "cpu_operator_names": {"Window": 1}}
    summary = compare({"cpu": cpu, "gpu": gpu_r})
    assert summary["same_result"] is True and summary["speedup"]["total"] == 2.0
    md = render_markdown("synthetic", 100, {"cpu": cpu, "gpu": gpu_r}, summary)
    assert "| total | 20.0 | 10.0 | 2.0x |" in md
    assert "GTX 1650" in md and "9 of 10 (90%)" in md and "Window (1)" in md
    assert not same_result(fp, dict(fp, quarantined=4))
    assert "speedup" not in compare({"cpu": cpu})
