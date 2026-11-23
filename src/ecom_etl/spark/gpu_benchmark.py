"""CPU against GPU on the same data, the same machine and the same Spark settings.

``python -m ecom_etl gpu-benchmark`` runs each engine in its own process,
because the RAPIDS plugin is loaded when the JVM starts and cannot be switched
on or off inside a running session. Each worker:

1. starts Spark with the tuned settings of step 7 (shuffle partitions sized
   to the machine, broadcast joins, AQE), plus the plugin for ``gpu``;
2. runs bronze, silver and gold once to warm up (JIT compilation, GPU
   initialisation and kernel loading happen outside the measurements);
3. times each stage ``repeat`` times and keeps the median;
4. fingerprints the result (gold row counts and revenue totals, quarantined
   rows) so the two engines can be checked to produce the same data;
5. on the GPU, counts which plan operators ran there and which stayed on the CPU.

The parent compares the two and writes ``reports/gpu_benchmark_<source>.md``.
"""

from __future__ import annotations

import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List

from .benchmark import TUNABLE, configs, machine_info
from .bronze import run_bronze
from .gold import GOLD_TABLES, run_gold
from .lake import Lake, new_run_id
from .silver import run_silver

STAGES = ("bronze", "silver", "gold")


def fingerprint(spark, lake: Lake) -> dict:
    """What the run produced, in a form two engines can be compared on."""
    from pyspark.sql import functions as F

    rows = {t: spark.read.parquet(lake.path("gold", t)).count() for t in GOLD_TABLES}
    lines = spark.read.parquet(lake.path("gold", "fact_order_line")).agg(F.sum("line_total")).first()[0]
    paid = spark.read.parquet(lake.path("gold", "fact_payment")).agg(F.sum("payment_value")).first()[0]
    quarantine_root = lake.root / "quarantine"
    quarantined = sum(spark.read.parquet(str(p)).count() for p in sorted(quarantine_root.iterdir())
                      if p.is_dir() and any(p.rglob("*.parquet"))) if quarantine_root.exists() else 0
    return {"gold_rows": rows, "revenue": round(float(lines or 0), 2), "payments": round(float(paid or 0), 2),
            "quarantined": quarantined}


def run_engine(spark, source_dir: Path, lake: Lake, extracted_at: str, engine: str,
               repeat: int = 2, warmup: bool = True) -> dict:
    tuned = configs(spark.sparkContext.defaultParallelism)[-1]
    original = {k: spark.conf.get(k) for k in TUNABLE}
    for key, value in tuned.conf.items():
        spark.conf.set(key, value)
    try:
        return _timed(spark, source_dir, lake, extracted_at, engine, repeat, warmup, tuned)
    finally:
        for key, value in original.items():
            spark.conf.set(key, value)


def _timed(spark, source_dir, lake, extracted_at, engine, repeat, warmup, tuned) -> dict:
    timings: Dict[str, List[float]] = {s: [] for s in STAGES}
    bronze_rows: Dict[str, int] = {}

    def one_pass(record: bool) -> None:
        run_id = new_run_id()
        steps = (("bronze", lambda: bronze_rows.update(run_bronze(spark, source_dir, lake, run_id))),
                 ("silver", lambda: run_silver(spark, lake, extracted_at, run_id)),
                 ("gold", lambda: run_gold(spark, lake, broadcast_dims=tuned.broadcast_dims)))
        for stage, step in steps:
            t0 = time.perf_counter()
            step()
            if record:
                timings[stage].append(round(time.perf_counter() - t0, 2))

    if warmup:
        one_pass(record=False)
    for _ in range(max(1, repeat)):
        one_pass(record=True)
    seconds = {s: statistics.median(v) for s, v in timings.items()}
    seconds["total"] = round(sum(seconds.values()), 2)
    result = {"engine": engine, "bronze_records": sum(bronze_rows.values()), "seconds": seconds, "runs": timings,
              "warmup": warmup, "repeat": max(1, repeat), "settings": tuned.conf,
              "machine": machine_info(spark), "fingerprint": fingerprint(spark, lake)}
    if engine == "gpu":
        from .gpu import executed_plan_nodes, gpu_info, operator_share

        result["gpu"] = gpu_info()
        result["operators"] = operator_share(executed_plan_nodes(spark))
    return result


def same_result(a: dict, b: dict, tolerance: float = 0.01) -> bool:
    """Same gold row counts and quarantine count; totals equal within ``tolerance``
    (floating-point sums can differ in the last digits when added in another order)."""
    return (a["gold_rows"] == b["gold_rows"] and a["quarantined"] == b["quarantined"]
            and abs(a["revenue"] - b["revenue"]) <= tolerance and abs(a["payments"] - b["payments"]) <= tolerance)


def compare(results: Dict[str, dict]) -> dict:
    summary: dict = {"engines": list(results)}
    if "cpu" in results and "gpu" in results:
        cpu, gpu = results["cpu"], results["gpu"]
        summary["same_result"] = same_result(cpu["fingerprint"], gpu["fingerprint"])
        summary["speedup"] = {s: round(cpu["seconds"][s] / gpu["seconds"][s], 2) if gpu["seconds"][s] else None
                              for s in (*STAGES, "total")}
    return summary


def timing_note(result: dict) -> str:
    n = result.get("repeat", 1)
    passes = f"{n} timed pass" + ("es" if n > 1 else "")
    when = ("after a warm-up pass" if result.get("warmup", True)
            else "without a warm-up pass, so start-up costs are included")
    return f"Median seconds per stage over {passes}, {when}:"


def render_markdown(source: str, bronze_records: int, results: Dict[str, dict], summary: dict) -> str:
    lines = [f"# CPU and GPU: `{source}` ({bronze_records:,} bronze records)", ""]
    any_result = next(iter(results.values()))
    m = any_result["machine"]
    lines += [f"Spark {m['spark']}, {m['cores']} cores, driver memory {m['driver_memory']}, {m['platform']}."]
    if "gpu" in results and results["gpu"].get("gpu"):
        g = results["gpu"]["gpu"]
        lines += [f"GPU: {g['name']}, {g['memory']}, driver {g['driver']}, compute capability {g['compute_capability']}."]
    lines += ["Settings: " + ", ".join(f"`{k}={v}`" for k, v in any_result["settings"].items()) + ".", "",
              timing_note(any_result), "",
              "| stage | " + " | ".join(results) + (" | speed-up |" if "speedup" in summary else " |"),
              "|---|" + "---:|" * (len(results) + ("speedup" in summary))]
    for stage in (*STAGES, "total"):
        row = f"| {stage} | " + " | ".join(f"{r['seconds'][stage]:.1f}" for r in results.values())
        if "speedup" in summary:
            row += f" | {summary['speedup'][stage]}x"
        lines.append(row + " |")
    if "same_result" in summary:
        lines += ["", "Same gold row counts, quarantine and revenue totals on both engines: "
                  + ("**yes**." if summary["same_result"] else "**NO**, see the JSON report.")]
    if "gpu" in results and results["gpu"].get("operators"):
        o = results["gpu"]["operators"]
        share = f"{o['gpu_share']:.0%}" if o["gpu_share"] is not None else "n/a"
        lines += ["", f"Plan operators on the GPU: {o['gpu_operators']} of {o['gpu_operators'] + o['cpu_operators']} "
                  f"({share}). Stayed on the CPU: "
                  + (", ".join(f"{k} ({v})" for k, v in o["cpu_operator_names"].items()) or "none") + "."]
    return "\n".join(lines) + "\n"


def run_workers(source: str, engines: List[str], repeat: int, warmup: bool, work_dir: Path) -> Dict[str, dict]:
    """Run each engine in a fresh Python process and collect their JSON results."""
    results = {}
    for engine in engines:
        out = work_dir / f"gpu_benchmark_{engine}.json"
        env = dict(os.environ, ECOM_GPU="1" if engine == "gpu" else "0")
        cmd = [sys.executable, "-m", "ecom_etl", "gpu-benchmark", "--source", source, "--worker", engine,
               "--repeat", str(repeat), "--out", str(out)] + ([] if warmup else ["--no-warmup"])
        print(f"[{engine}] {' '.join(cmd[1:])}", flush=True)
        subprocess.run(cmd, env=env, check=True)
        results[engine] = json.loads(out.read_text(encoding="utf-8"))
        print(f"[{engine}] total {results[engine]['seconds']['total']:.1f}s", flush=True)
    return results

