"""The whole batch pipeline with quality gates between the stages.

    bronze -> silver -> [silver gates] -> gold -> [gold gates] -> load Postgres

A failed blocking gate stops the run *before* the next layer is published,
so analysts never see a half-broken star schema. The data quality report
is written either way (that is when you need it most).
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List, Optional

from .. import config
from .bronze import run_bronze
from .gold import run_gold
from .lake import Lake, new_run_id
from .quality import (
    MAX_QUARANTINE_RATE,
    CheckResult,
    QualityGateError,
    evaluate_against_ground_truth,
    gate,
    gold_gate_results,
    render_markdown,
    results_as_dicts,
    row_level_results,
    save_results,
    silver_gate_results,
    summarize,
)
from .silver import TableReport, run_silver


def latest_silver_reports(lake: Lake) -> Dict[str, TableReport]:
    reports = sorted((lake.root / "_reports").glob("silver_*.json"))
    if not reports:
        raise FileNotFoundError(f"no silver report in {lake.root / '_reports'}; run silver first")
    data = json.loads(reports[-1].read_text(encoding="utf-8"))
    return {name: TableReport(**fields) for name, fields in data["tables"].items()}


def _finish(source: str, run_id: str, lake: Lake, source_dir: Path, spark, silver_reports, results,
            extra: dict, save_dq: bool) -> dict:
    evaluation = evaluate_against_ground_truth(spark, lake, source_dir, silver_reports)
    summary = summarize(results, silver_reports)
    report = {"source": source, "run_id": run_id, **extra, "summary": summary,
              "evaluation": evaluation, "checks": results_as_dicts(results)}
    lake.write_report(f"quality_{run_id}", report)
    md = config.REPORTS_DIR / f"dq_report_{source}.md"
    md.parent.mkdir(parents=True, exist_ok=True)
    md.write_text(render_markdown(source, run_id, summary, results, evaluation), encoding="utf-8")
    report["markdown_report"] = str(md)
    if save_dq:
        save_results(source, run_id, results, silver_reports)
    return report


def run_quality(spark, lake: Lake, source: str, source_dir: Path, extracted_at: str,
                save_dq: bool = True, include_gold: bool = True) -> dict:
    """Re-check an existing lake (after `silver` / `gold` were run separately)."""
    run_id = new_run_id()
    silver_reports = latest_silver_reports(lake)
    results: List[CheckResult] = row_level_results(silver_reports)
    results += silver_gate_results(spark, lake, silver_reports, extracted_at)
    if include_gold and (lake.root / "gold" / "fact_order_line").exists():
        results += gold_gate_results(spark, lake)
    return _finish(source, run_id, lake, source_dir, spark, silver_reports, results, {}, save_dq)


def run_pipeline(spark, source_dir: Path, source: str, lake: Lake, extracted_at: str, load: bool = True,
                 save_dq: bool = True, max_quarantine_rate: float = MAX_QUARANTINE_RATE) -> dict:
    run_id = new_run_id()
    seconds: Dict[str, float] = {}
    results: List[CheckResult] = []
    silver_reports: Dict[str, TableReport] = {}
    extra: dict = {"seconds": seconds, "status": "running"}

    def timed(stage, fn, *args, **kwargs):
        t0 = time.perf_counter()
        out = fn(*args, **kwargs)
        seconds[stage] = round(time.perf_counter() - t0, 2)
        return out

    try:
        extra["bronze_rows"] = timed("bronze", run_bronze, spark, source_dir, lake, run_id)
        silver_reports = timed("silver", run_silver, spark, lake, extracted_at, run_id)
        results += row_level_results(silver_reports)
        results += timed("silver_checks", silver_gate_results, spark, lake, silver_reports, extracted_at,
                         max_quarantine_rate)
        gate(results)
        extra["gold_rows"] = timed("gold", run_gold, spark, lake)
        results += timed("gold_checks", gold_gate_results, spark, lake)
        gate(results)
        if load:
            from .warehouse import load_gold
            extra["loaded_rows"] = timed("load", load_gold, spark, lake, source)
        extra["status"] = "success"
    except QualityGateError:
        extra["status"] = "blocked_by_quality_gate"
        raise
    finally:
        seconds["total"] = round(sum(v for k, v in seconds.items() if k != "total"), 2)
        if silver_reports:
            extra["report"] = _finish(source, run_id, lake, source_dir, spark, silver_reports, results,
                                      {k: v for k, v in extra.items() if k != "report"}, save_dq)
    return extra["report"]
