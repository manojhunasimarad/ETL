"""Run the Spark batch on an NVIDIA GPU with the RAPIDS Accelerator for Apache Spark.

The RAPIDS Accelerator (spark-rapids, NVIDIA) is a Spark plugin: it replaces
physical plan operators (scans, filters, projections, joins, aggregations,
sorts, window functions, Parquet and CSV reading and writing) with GPU versions
built on cuDF, and leaves anything it cannot run on the CPU. The DataFrame
code does not change; set ``ECOM_GPU=1`` and every Spark command of this
project (``bronze``, ``silver``, ``gold``, ``run``, ``benchmark``,
``gpu-benchmark``) starts Spark with the plugin.

Where a GPU helps in this project, and where it does not:

* helps: the Spark batch at scale, i.e. bronze (CSV parsing), silver
  (casts, rules, window de-duplication, anti joins) and gold (joins,
  aggregations, the SCD Type 2 windows), once the data is large enough that
  the work outweighs moving it to the GPU (hundreds of thousands of orders,
  better millions);
* helps a little: profiling, which is pandas, through ``cudf.pandas``
  (``python -m ecom_etl profile --gpu``);
* does not help: the Postgres load and queries (database and network bound),
  Airflow (scheduling), the Kafka stream (small micro-batches, latency bound),
  the generator (pure Python) and the dashboard.

What stays on the CPU by design: reading the source CSV files. They need
``escape='"'`` (RFC 4180 quoting) and the ``_metadata`` file column for lineage,
which the GPU CSV reader does not support; everything after the read runs on
the GPU.

Requirements (RAPIDS Accelerator 26.10): Linux (on Windows: WSL2), an NVIDIA
GPU of the Volta generation or newer (compute capability 7.0+, so RTX 20xx,
GTX 16xx and later), driver R525+ (CUDA 12), and a supported Spark version.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Optional

RAPIDS_VERSION = os.environ.get("ECOM_RAPIDS_VERSION", "26.10.0")
SCALA = "2.13"
# Spark versions the RAPIDS Accelerator 26.10 supports (it refuses others at start-up).
SUPPORTED_SPARK = {"4.0.0", "4.0.1", "4.0.2", "4.0.3", "4.0.4", "4.1.1", "4.1.2", "4.1.3", "4.2.0"}
PINNED_SPARK = "4.1.3"


def gpu_requested() -> bool:
    return os.environ.get("ECOM_GPU", "") in ("1", "true", "yes")


def rapids_coordinate(version: str = RAPIDS_VERSION) -> str:
    return f"com.nvidia:rapids-4-spark_{SCALA}:{version}"


def rapids_jar_url(version: str = RAPIDS_VERSION) -> str:
    name = f"rapids-4-spark_{SCALA}-{version}.jar"
    return f"https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_{SCALA}/{version}/{name}"


def default_jar_path(version: str = RAPIDS_VERSION) -> Path:
    return Path.home() / ".cache" / "ecom-etl" / f"rapids-4-spark_{SCALA}-{version}.jar"


def check_spark_version(version: str) -> None:
    if version not in SUPPORTED_SPARK:
        raise RuntimeError(
            f"PySpark {version} is not supported by the RAPIDS Accelerator {RAPIDS_VERSION} "
            f"(supported: {', '.join(sorted(SUPPORTED_SPARK))}). "
            f"In the GPU environment run: pip install 'pyspark=={PINNED_SPARK}'")


def rapids_jar() -> Optional[str]:
    """A local plugin jar (ECOM_RAPIDS_JAR, or the one scripts/gpu/setup_wsl.sh downloads), if any."""
    configured = os.environ.get("ECOM_RAPIDS_JAR")
    if configured:
        if not Path(configured).exists():
            raise FileNotFoundError(f"ECOM_RAPIDS_JAR={configured} does not exist")
        return configured
    cached = default_jar_path()
    return str(cached) if cached.exists() else None


def rapids_conf(concurrent_tasks: Optional[int] = None) -> Dict[str, str]:
    """Spark settings that turn the plugin on for local mode on one GPU.

    ``concurrentGpuTasks`` is how many tasks share the GPU at once: 1 is safe
    on a laptop GPU with little memory, 2 often faster with 8 GB or more.
    ``batchSizeBytes`` is kept at 256 MB for the same reason (ECOM_RAPIDS_BATCH_BYTES).
    Pinned host memory is left at the plugin's default: WSL2 limits pinned
    memory, so it is only set when ECOM_RAPIDS_PINNED_POOL asks for it.
    """
    tasks = concurrent_tasks or int(os.environ.get("ECOM_RAPIDS_CONCURRENT_TASKS", "1"))
    conf = {
        "spark.plugins": "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.enabled": "true",
        "spark.rapids.sql.concurrentGpuTasks": str(tasks),
        # NOT_ON_GPU prints every operator that stayed on the CPU, and why.
        "spark.rapids.sql.explain": os.environ.get("ECOM_RAPIDS_EXPLAIN", "NONE"),
        # GPU batch size: smaller than the 1 GB default so joins fit a 4 GB laptop GPU
        "spark.rapids.sql.batchSizeBytes": os.environ.get("ECOM_RAPIDS_BATCH_BYTES", "256m"),
        "spark.sql.files.maxPartitionBytes": "256m",
        "spark.locality.wait": "0s",
        # Cached DataFrames (silver and gold persist a few) in a GPU-readable format; with
        # Spark's default serializer every scan of the cache falls back to the CPU.
        "spark.sql.cache.serializer": "com.nvidia.spark.ParquetCachedBatchSerializer",
    }
    if os.environ.get("ECOM_RAPIDS_PINNED_POOL"):
        conf["spark.rapids.memory.pinnedPool.size"] = os.environ["ECOM_RAPIDS_PINNED_POOL"]
    return conf


def add_rapids(settings: Dict[str, str], packages: List[str]) -> None:
    """Add the plugin to the settings ``get_spark`` is about to use (in place)."""
    import pyspark

    check_spark_version(pyspark.__version__.split("+")[0].split(".dev")[0])
    jar = rapids_jar()
    if jar:
        settings["spark.jars"] = ",".join(filter(None, [settings.get("spark.jars"), jar]))
    else:  # downloaded once by Spark into ~/.ivy2 (the jar is large)
        packages.append(rapids_coordinate())
    settings.update(rapids_conf())


def gpu_info() -> Optional[dict]:
    """Name, memory and driver of the first GPU, from nvidia-smi (None without one)."""
    if not shutil.which("nvidia-smi"):
        return None
    try:
        out = subprocess.run(["nvidia-smi", "--query-gpu=name,memory.total,driver_version,compute_cap",
                              "--format=csv,noheader"], capture_output=True, text=True, timeout=20, check=True)
    except (subprocess.SubprocessError, OSError):
        return None
    first = out.stdout.strip().splitlines()[0] if out.stdout.strip() else ""
    parts = [p.strip() for p in first.split(",")]
    if len(parts) < 4:
        return None
    return {"name": parts[0], "memory": parts[1], "driver": parts[2], "compute_capability": parts[3]}


# ---------------------------------------------------------------- what ran on the GPU

# Plan nodes that are wrappers or data movement between CPU and GPU, not work.
_STRUCTURAL = ("WholeStageCodegen", "InputAdapter", "AdaptiveSparkPlan", "ResultQueryStage", "ShuffleQueryStage",
               "BroadcastQueryStage", "TableCacheQueryStage", "AQEShuffleRead", "GpuShuffleCoalesce", "ReusedExchange",
               "Subquery", "SubqueryBroadcast", "ReusedSubquery", "ColumnarToRow", "RowToColumnar",
               "GpuColumnarToRow", "GpuRowToColumnar", "GpuCoalesceBatches", "CustomShuffleReader",
               "GpuBringBackToHost", "GpuColumnarExchange", "LocalTableScan", "CommandResult",
               # adaptive execution's placeholders, e.g. for a join side it found empty
               "LogicalQueryStage", "EmptyRelation")


def operator_share(node_names: Iterable[str]) -> dict:
    """Count plan operators that ran on the GPU (names containing 'Gpu') against the rest.

    Structural nodes (code-generation stages, query-stage wrappers, CPU/GPU
    transitions) are left out, so the share is about real work.
    """
    gpu, cpu = 0, Counter()
    for name in node_names:
        base = name.strip()
        if not base or any(base.startswith(s) for s in _STRUCTURAL):
            continue
        if "Gpu" in base:
            gpu += 1
        else:
            cpu[base.split(" (")[0]] += 1
    total = gpu + sum(cpu.values())
    return {"gpu_operators": gpu, "cpu_operators": sum(cpu.values()),
            "gpu_share": round(gpu / total, 3) if total else None,
            "cpu_operator_names": dict(cpu.most_common(10))}


def executed_plan_nodes(spark) -> List[str]:
    """Every plan node of every SQL execution so far, from the Spark UI's REST API."""
    url = spark.sparkContext.uiWebUrl
    if not url:
        return []
    app = spark.sparkContext.applicationId
    names: List[str] = []
    offset = 0
    while True:
        query = f"{url}/api/v1/applications/{app}/sql?details=true&planDescription=false&offset={offset}&length=500"
        with urllib.request.urlopen(query, timeout=30) as response:  # noqa: S310 (local Spark UI)
            batch = json.loads(response.read().decode("utf-8"))
        for execution in batch:
            names += [n.get("nodeName", "") for n in execution.get("nodes", [])]
        if len(batch) < 500:
            return names
        offset += 500
