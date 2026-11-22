"""Step 7: measure what each optimization is worth, on the same data and machine.

Spark (silver + gold), one optimization added at a time (an ablation):

    baseline     AQE off, broadcast joins off, 200 shuffle partitions
    +partitions  shuffle partitions sized to the machine (2 x cores)
    +broadcast   broadcast small tables (10 MB threshold + hints on dimensions)
    tuned        + adaptive query execution (AQE)

The baseline is what Spark does with AQE and broadcast joins switched off
(the Spark 2 era defaults); every later row adds exactly one change, so
the report shows where the time goes instead of one before/after number.

Query layout: the same month-filter query on the fact table written with
and without ``partitionBy("order_month")`` (partition pruning).
"""

from __future__ import annotations

import os
import platform
import shutil
import statistics
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from .gold import run_gold
from .lake import Lake
from .silver import run_silver

TUNABLE = ("spark.sql.adaptive.enabled", "spark.sql.autoBroadcastJoinThreshold", "spark.sql.shuffle.partitions")


@dataclass
class Config:
    name: str
    description: str
    conf: Dict[str, str]
    broadcast_dims: bool


def configs(cores: int) -> List[Config]:
    sized = str(max(4, cores * 2))
    ten_mb = str(10 * 1024 * 1024)
    return [
        Config("baseline", "AQE off, broadcast joins off, 200 shuffle partitions",
               {"spark.sql.adaptive.enabled": "false", "spark.sql.autoBroadcastJoinThreshold": "-1",
                "spark.sql.shuffle.partitions": "200"}, False),
        Config("+partitions", f"shuffle partitions sized to the machine ({sized})",
               {"spark.sql.adaptive.enabled": "false", "spark.sql.autoBroadcastJoinThreshold": "-1",
                "spark.sql.shuffle.partitions": sized}, False),
        Config("+broadcast", "broadcast joins for small tables and dimensions",
               {"spark.sql.adaptive.enabled": "false", "spark.sql.autoBroadcastJoinThreshold": ten_mb,
                "spark.sql.shuffle.partitions": sized}, True),
        Config("tuned", "adaptive query execution on (all optimizations)",
               {"spark.sql.adaptive.enabled": "true", "spark.sql.autoBroadcastJoinThreshold": ten_mb,
                "spark.sql.shuffle.partitions": sized}, True),
    ]


def _apply(spark, conf: Dict[str, str]) -> None:
    for key, value in conf.items():
        spark.conf.set(key, value)


def run_spark_benchmark(spark, lake: Lake, extracted_at: str, repeat: int = 1, warmup: bool = True,
                        only: Optional[Sequence[str]] = None) -> List[dict]:
    """Time silver + gold under each config. Bronze must already exist."""
    cores = spark.sparkContext.defaultParallelism
    plans = [c for c in configs(cores) if not only or c.name in only]
    original = {k: spark.conf.get(k) for k in TUNABLE}
    results = []
    try:
        if warmup:  # JIT compilation and class loading happen once, outside the measurements
            _apply(spark, plans[-1].conf)
            run_silver(spark, lake, extracted_at)
        for cfg in plans:
            _apply(spark, cfg.conf)
            silver_s, gold_s = [], []
            for _ in range(repeat):
                spark.catalog.clearCache()
                t0 = time.perf_counter()
                run_silver(spark, lake, extracted_at)
                silver_s.append(time.perf_counter() - t0)
                t0 = time.perf_counter()
                run_gold(spark, lake, broadcast_dims=cfg.broadcast_dims, partition_facts=True)
                gold_s.append(time.perf_counter() - t0)
            s, g = statistics.median(silver_s), statistics.median(gold_s)
            results.append({"config": cfg.name, "description": cfg.description, "conf": cfg.conf,
                            "silver_s": round(s, 2), "gold_s": round(g, 2), "total_s": round(s + g, 2),
                            "runs": repeat})
    finally:
        _apply(spark, {k: v for k, v in original.items() if v is not None})
    base = results[0]["total_s"]
    for r in results:
        r["speedup_vs_baseline"] = round(base / r["total_s"], 2) if r["total_s"] else None
    return results


def run_layout_benchmark(spark, lake: Lake, repeat: int = 3) -> dict:
    """One month of revenue by category: partitioned vs unpartitioned fact table."""
    from pyspark.sql import functions as F

    partitioned = lake.path("gold", "fact_order_line")
    flat = str(lake.root / "_bench" / "fact_order_line_unpartitioned")
    shutil.rmtree(flat, ignore_errors=True)
    spark.read.parquet(partitioned).write.mode("overwrite").parquet(flat)
    month = spark.read.parquet(partitioned).agg(F.max("order_month")).first()[0]
    products = spark.read.parquet(lake.path("gold", "dim_product"))

    def query(path):
        fact = spark.read.parquet(path).filter(F.col("order_month") == month)
        return (fact.join(F.broadcast(products), "product_sk")
                .groupBy("category_name_english").agg(F.sum("line_total")).collect())

    def timed(path):
        query(path)  # warm-up
        times = []
        for _ in range(repeat):
            t0 = time.perf_counter()
            query(path)
            times.append(time.perf_counter() - t0)
        return round(statistics.median(times) * 1000, 1)

    def megabytes(path, sub=""):
        root = os.path.join(path, sub) if sub else path
        size = sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(root)
                   for f in fs if f.endswith(".parquet"))
        return round(size / 1e6, 2)

    return {
        "query": f"revenue by category for {month}",
        "month": month,
        "partitioned_ms": timed(partitioned),
        "unpartitioned_ms": timed(flat),
        # pruning: only the month's folder is opened; unpartitioned, every file is
        "partitioned_mb_scanned": megabytes(partitioned, f"order_month={month}"),
        "unpartitioned_mb_scanned": megabytes(flat),
    }


def machine_info(spark) -> dict:
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "spark": spark.version,
        "cores": spark.sparkContext.defaultParallelism,
        "driver_memory": spark.conf.get("spark.driver.memory", "default"),
    }


def render_markdown(source: str, report: dict) -> str:
    m, rows = report["machine"], report["spark"]
    out = [f"# Performance benchmark: `{source}`", "",
           f"- Machine: {m['platform']}, {m['cores']} cores, driver memory {m['driver_memory']}, "
           f"Spark {m['spark']}, Python {m['python']}",
           f"- Data: {report['bronze_records']:,} bronze records ({report['bronze_rows'].get('orders', 0):,} orders)",
           f"- Each Spark number is the median of {rows[0]['runs']} run(s) after a warm-up run.", "",
           "## Spark: silver + gold, one optimization added at a time", "",
           "| config | change | silver (s) | gold (s) | total (s) | speedup vs baseline |",
           "|---|---|---:|---:|---:|---:|"]
    out += [f"| {r['config']} | {r['description']} | {r['silver_s']} | {r['gold_s']} | {r['total_s']} | "
            f"{r['speedup_vs_baseline']}x |" for r in rows]
    lay = report["layout"]
    out += ["", f"## Data layout: {lay['query']}", "",
            "| fact table layout | data scanned (MB) | median (ms) |", "|---|---:|---:|",
            f"| partitioned by order_month | {lay['partitioned_mb_scanned']} | {lay['partitioned_ms']} |",
            f"| not partitioned | {lay['unpartitioned_mb_scanned']} | {lay['unpartitioned_ms']} |"]
    if report.get("sql"):
        out += ["", "## Postgres: secondary indexes (EXPLAIN ANALYZE, median)", "",
                "| query | before (ms) | fact scan before | after (ms) | fact scan after | speedup |",
                "|---|---:|---|---:|---|---:|"]
        out += [f"| {r['query']} | {r['before_ms']} | {r['before_scan']} | {r['after_ms']} | {r['after_scan']} | "
                f"{r['speedup']}x |" for r in report["sql"]]
    return "\n".join(out) + "\n"
