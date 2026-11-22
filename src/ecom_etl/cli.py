"""Command line entry point: ``python -m ecom_etl <command>``.

Commands:
  generate        write synthetic Olist-shaped CSVs with injected defects
  download-olist  fetch the real Olist dataset from Kaggle (or a local zip)
  validate        check files and headers against the source contract
  profile         profile a source and write a markdown report
  load-raw        (step 2) copy the CSV files into Postgres <source>_raw
  build-core      (step 2) build the normalized <source>_core schema with SQL
  bronze          (step 3) CSV -> bronze Parquet with lineage columns (Spark)
  silver          (step 3) bronze -> silver + quarantine (Spark)
  gold            (step 4) silver -> star schema (Spark)
  load-gold       (step 4) star schema -> Postgres <source>_gold (JDBC)
  quality         (step 5) all data quality checks + report (+ history in Postgres)
  run             (step 5) the whole batch pipeline with quality gates
  analyze         (step 6) 10 business questions in SQL -> reports/analysis_<source>.md
  benchmark       (step 7) baseline vs tuned Spark, partition pruning, Postgres indexes
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path

from . import config
from .generator import GeneratorConfig, generate, load_manifest


def _cmd_generate(args: argparse.Namespace) -> int:
    cfg = GeneratorConfig(orders=args.orders, seed=args.seed, defect_rate=args.defect_rate,
                          start=args.start, end=args.end)
    out = Path(args.out) if args.out else config.source_dir("synthetic")
    t0 = time.perf_counter()
    manifest = generate(cfg, out)
    elapsed = time.perf_counter() - t0
    print(f"Wrote synthetic data to {out} in {elapsed:.1f}s")
    for table, rows in manifest["row_counts"].items():
        print(f"  {table:<22}{rows:>12,} rows")
    print(f"  injected defects: {manifest['total_defects']:,} (see _defects.csv)")
    return 0


def _cmd_download_olist(args: argparse.Namespace) -> int:
    from .olist import download_olist, validate_source

    out = Path(args.out) if args.out else config.source_dir("olist")
    try:
        files = download_olist(out, Path(args.zip) if args.zip else None)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1
    print(f"Extracted {len(files)} files to {out}")
    problems = validate_source(out)
    for p in problems:
        print(f"  problem: {p}", file=sys.stderr)
    return 1 if problems else 0


def _cmd_validate(args: argparse.Namespace) -> int:
    from .olist import validate_source

    directory = config.source_dir(args.source)
    problems = validate_source(directory)
    if problems:
        for p in problems:
            print(f"  problem: {p}")
        return 1
    print(f"{directory}: all files present and headers match the contract")
    return 0


def _cmd_profile(args: argparse.Namespace) -> int:
    from .profiling import compare_with_manifest, profile_source, render_markdown

    directory = config.source_dir(args.source)
    if not directory.exists():
        print(f"{directory} does not exist. Run 'generate' or 'download-olist' first.", file=sys.stderr)
        return 1
    name = args.source if args.source in config.KNOWN_SOURCES else directory.name
    t0 = time.perf_counter()
    profile = profile_source(directory, name)
    manifest = load_manifest(directory)
    comparison = compare_with_manifest(profile, manifest) if manifest else None
    report = Path(args.report) if args.report else config.REPORTS_DIR / f"profile_{name}.md"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(render_markdown(profile, comparison), encoding="utf-8")

    print(f"Profiled {directory} in {time.perf_counter() - t0:.1f}s")
    for tp in profile.tables.values():
        print(f"  {tp.name:<22}{tp.rows:>12,} rows")
    print(f"\n{len(profile.findings)} findings:")
    for f in profile.findings:
        print(f"  [{f.severity:<5}] {f.table:<15} {f.check:<58} {f.count:>9,}")
    if comparison:
        caught = sum(r["match"] for r in comparison)
        print(f"\nInjected vs detected: {caught}/{len(comparison)} defect types match exactly")
        for r in comparison:
            if not r["match"]:
                print(f"  MISMATCH {r['table']}.{r['defect_type']}: injected {r['injected']}, "
                      f"detected {r['detected']}")
    print(f"\nReport: {report}")
    return 0


def _source_extracted_at(directory: Path):
    """Extraction moment: from the synthetic manifest, otherwise "now"."""
    from datetime import datetime

    from .contracts import TIMESTAMP_FORMAT

    manifest = load_manifest(directory)
    return datetime.strptime(manifest["extracted_at"], TIMESTAMP_FORMAT) if manifest else datetime.now()


def _source_name(source: str) -> str:
    return source if source in config.KNOWN_SOURCES else Path(source).name


def _cmd_load_raw(args: argparse.Namespace) -> int:
    from .landing import load_raw

    directory = config.source_dir(args.source)
    t0 = time.perf_counter()
    counts = load_raw(directory, _source_name(args.source))
    print(f"Loaded {directory} into schema {_source_name(args.source)}_raw in {time.perf_counter() - t0:.1f}s")
    for table, rows in counts.items():
        print(f"  {table:<22}{rows:>12,} rows")
    return 0


def _cmd_build_core(args: argparse.Namespace) -> int:
    from .landing import build_core

    directory = config.source_dir(args.source)
    name = _source_name(args.source)
    t0 = time.perf_counter()
    result = build_core(name, _source_extracted_at(directory))
    print(f"Built schema {name}_core in {time.perf_counter() - t0:.1f}s")
    print(f"  {'table':<22}{'raw':>10}{'core':>10}{'removed':>10}")
    for table, c in result.items():
        print(f"  {table:<22}{c['raw']:>10,}{c['core']:>10,}{c['raw'] - c['core']:>10,}")
    return 0


def _spark_and_lake(source: str, jdbc: bool = False):
    """SparkSession + lake for a source. ``jdbc`` adds the Postgres driver
    (downloaded once from Maven, unless ECOM_SPARK_JARS points at a local jar)."""
    import os

    from .spark.lake import Lake
    from .spark.session import get_spark
    from .spark.warehouse import PG_JDBC_PACKAGE

    packages = (PG_JDBC_PACKAGE,) if jdbc and not os.environ.get("ECOM_SPARK_JARS") else ()
    return (get_spark(f"ecom_etl-{_source_name(source)}", packages=packages),
            Lake.for_source(_source_name(source)))


def _cmd_gold(args: argparse.Namespace) -> int:
    from .spark.gold import run_gold

    spark, lake = _spark_and_lake(args.source)
    t0 = time.perf_counter()
    counts = run_gold(spark, lake)
    print(f"Gold (star schema) written to {lake.root / 'gold'} in {time.perf_counter() - t0:.1f}s")
    for table, rows in counts.items():
        print(f"  {table:<22}{rows:>12,} rows")
    return 0


def _cmd_load_gold(args: argparse.Namespace) -> int:
    from .spark.warehouse import load_gold

    spark, lake = _spark_and_lake(args.source, jdbc=True)
    t0 = time.perf_counter()
    counts = load_gold(spark, lake, _source_name(args.source))
    print(f"Loaded gold into Postgres schema {_source_name(args.source)}_gold "
          f"in {time.perf_counter() - t0:.1f}s")
    for table, rows in counts.items():
        print(f"  {table:<22}{rows:>12,} rows")
    return 0


def _cmd_bronze(args: argparse.Namespace) -> int:
    from .spark.bronze import run_bronze
    from .spark.lake import new_run_id

    spark, lake = _spark_and_lake(args.source)
    run_id = new_run_id()
    t0 = time.perf_counter()
    counts = run_bronze(spark, config.source_dir(args.source), lake, run_id)
    elapsed = time.perf_counter() - t0
    lake.write_report(f"bronze_{run_id}", {"run_id": run_id, "seconds": round(elapsed, 2), "rows": counts})
    print(f"Bronze written to {lake.root / 'bronze'} in {elapsed:.1f}s")
    for table, rows in counts.items():
        print(f"  {table:<22}{rows:>12,} rows")
    return 0


def _cmd_silver(args: argparse.Namespace) -> int:
    from .contracts import TIMESTAMP_FORMAT
    from .spark.lake import new_run_id
    from .spark.silver import run_silver

    spark, lake = _spark_and_lake(args.source)
    extracted_at = _source_extracted_at(config.source_dir(args.source)).strftime(TIMESTAMP_FORMAT)
    t0 = time.perf_counter()
    reports = run_silver(spark, lake, extracted_at, new_run_id())
    elapsed = time.perf_counter() - t0
    print(f"Silver written to {lake.root / 'silver'} in {elapsed:.1f}s")
    print(f"  {'table':<22}{'bronze':>10}{'dupes':>8}{'quarantine':>12}{'silver':>10}")
    for name, r in reports.items():
        print(f"  {name:<22}{r.bronze_rows:>10,}{r.exact_duplicates:>8,}{r.quarantined:>12,}{r.silver_rows:>10,}")
    reasons = {}
    for r in reports.values():
        for code, n in r.reasons.items():
            reasons[f"{r.table}: {code}"] = n
    if reasons:
        print("\n  quarantine reasons:")
        for code, n in sorted(reasons.items(), key=lambda kv: -kv[1]):
            print(f"    {code:<60}{n:>8,}")
    return 0


def _print_quality(report: dict) -> None:
    s = report["summary"]
    print(f"Data quality: {s['checks_total']} checks ({s['row_level_checks']} row-level, "
          f"{s['table_level_checks']} table-level), {s['checks_failed']} flagged, "
          f"{s['blocking_failures']} blocking")
    print(f"  bronze records {s['bronze_records']:,}: quarantined {s['records_quarantined']:,}, "
          f"duplicates removed {s['duplicates_removed']:,}, fixed {s['records_fixed']:,}, "
          f"warnings {s['records_with_warnings']:,}")
    ev = report.get("evaluation")
    if ev:
        q = ev["quarantine"]
        print(f"  vs injected defects: quarantine precision {q['precision']:.1%}, recall {q['recall']:.1%}")
    for c in report["checks"]:
        if c["status"] == "fail" and c["kind"] == "table":
            flag = "BLOCKING" if c["blocking"] else "warn"
            print(f"  [{flag}] {c['name']}: {c['detail'] or c['failed_rows']}")
    print(f"  report: {report['markdown_report']}")


def _cmd_quality(args: argparse.Namespace) -> int:
    from .contracts import TIMESTAMP_FORMAT
    from .spark.pipeline import run_quality

    spark, lake = _spark_and_lake(args.source)
    directory = config.source_dir(args.source)
    extracted_at = _source_extracted_at(directory).strftime(TIMESTAMP_FORMAT)
    report = run_quality(spark, lake, _source_name(args.source), directory, extracted_at, save_dq=not args.no_db)
    _print_quality(report)
    return 1 if report["summary"]["blocking_failures"] else 0


def _cmd_run(args: argparse.Namespace) -> int:
    from .contracts import TIMESTAMP_FORMAT
    from .spark.pipeline import run_pipeline
    from .spark.quality import QualityGateError

    spark, lake = _spark_and_lake(args.source, jdbc=not args.no_load)
    directory = config.source_dir(args.source)
    extracted_at = _source_extracted_at(directory).strftime(TIMESTAMP_FORMAT)
    try:
        report = run_pipeline(spark, directory, _source_name(args.source), lake, extracted_at,
                              load=not args.no_load, save_dq=not args.no_load)
    except QualityGateError as exc:
        print(f"STOPPED: {exc}", file=sys.stderr)
        return 2
    print(f"Pipeline {report['status']} in {report['seconds']['total']}s: "
          + ", ".join(f"{k} {v}s" for k, v in report["seconds"].items() if k != "total"))
    _print_quality(report)
    return 0


def _cmd_analyze(args: argparse.Namespace) -> int:
    from .analysis import render_markdown, run_analyses

    name = _source_name(args.source)
    analyses = run_analyses(name)
    report = config.REPORTS_DIR / f"analysis_{name}.md"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(render_markdown(name, analyses), encoding="utf-8")
    for a in analyses:
        print(f"{a.question}  [{len(a.rows)} rows]")
        if a.rows:
            print("    " + " | ".join(f"{c}={v}" for c, v in zip(a.columns, a.rows[0])))
    print(f"\nReport: {report}")
    return 0


def _cmd_benchmark(args: argparse.Namespace) -> int:
    from .contracts import TIMESTAMP_FORMAT
    from .spark.benchmark import machine_info, render_markdown, run_layout_benchmark, run_spark_benchmark
    from .spark.bronze import run_bronze
    from .spark.lake import new_run_id

    name = _source_name(args.source)
    spark, lake = _spark_and_lake(args.source, jdbc=not args.no_sql)
    directory = config.source_dir(args.source)
    extracted_at = _source_extracted_at(directory).strftime(TIMESTAMP_FORMAT)
    run_id = new_run_id()
    bronze = run_bronze(spark, directory, lake, run_id)
    print(f"Bronze ready: {sum(bronze.values()):,} records. Benchmarking Spark configurations...")
    report = {"source": name, "run_id": run_id, "machine": machine_info(spark), "bronze_rows": bronze,
              "bronze_records": sum(bronze.values())}
    report["spark"] = run_spark_benchmark(spark, lake, extracted_at, repeat=args.repeat)
    report["layout"] = run_layout_benchmark(spark, lake)
    if not args.no_sql:
        from .spark.warehouse import load_gold
        from .sql_tuning import run_sql_benchmark

        load_gold(spark, lake, name)
        report["sql"] = run_sql_benchmark(name)
    lake.write_report(f"benchmark_{run_id}", report)
    md = config.REPORTS_DIR / f"benchmark_{name}.md"
    md.parent.mkdir(parents=True, exist_ok=True)
    md.write_text(render_markdown(name, report), encoding="utf-8")
    print(md.read_text(encoding="utf-8"))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ecom_etl", description="E-commerce batch and streaming ETL")
    sub = parser.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="write synthetic Olist-shaped data")
    g.add_argument("--orders", type=int, default=10_000, help="number of orders (default 10,000)")
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--defect-rate", type=float, default=0.01,
                   help="probability of each defect type per eligible row (0 to 0.05, default 0.01)")
    g.add_argument("--start", type=date.fromisoformat, default=date(2017, 1, 1))
    g.add_argument("--end", type=date.fromisoformat, default=date(2018, 8, 31))
    g.add_argument("--out", help="output folder (default data/raw/synthetic)")
    g.set_defaults(func=_cmd_generate)

    d = sub.add_parser("download-olist", help="download the real Olist dataset")
    d.add_argument("--zip", help="use an already downloaded Kaggle zip instead of the Kaggle CLI")
    d.add_argument("--out", help="output folder (default data/raw/olist)")
    d.set_defaults(func=_cmd_download_olist)

    v = sub.add_parser("validate", help="check files and headers against the contract")
    v.add_argument("--source", default="synthetic", help="synthetic, olist, or a folder path")
    v.set_defaults(func=_cmd_validate)

    p = sub.add_parser("profile", help="profile a source and write a markdown report")
    p.add_argument("--source", default="synthetic", help="synthetic, olist, or a folder path")
    p.add_argument("--report", help="report path (default reports/profile_<source>.md)")
    p.set_defaults(func=_cmd_profile)

    lr = sub.add_parser("load-raw", help="step 2: copy the CSV files into Postgres <source>_raw")
    lr.add_argument("--source", default="synthetic", help="synthetic, olist, or a folder path")
    lr.set_defaults(func=_cmd_load_raw)

    bc = sub.add_parser("build-core", help="step 2: build the normalized <source>_core schema with SQL")
    bc.add_argument("--source", default="synthetic", help="synthetic, olist, or a folder path")
    bc.set_defaults(func=_cmd_build_core)

    br = sub.add_parser("bronze", help="step 3: CSV -> bronze Parquet (Spark)")
    br.add_argument("--source", default="synthetic", help="synthetic, olist, or a folder path")
    br.set_defaults(func=_cmd_bronze)

    sv = sub.add_parser("silver", help="step 3: bronze -> silver + quarantine (Spark)")
    sv.add_argument("--source", default="synthetic", help="synthetic, olist, or a folder path")
    sv.set_defaults(func=_cmd_silver)

    gd = sub.add_parser("gold", help="step 4: silver -> star schema (Spark)")
    gd.add_argument("--source", default="synthetic", help="synthetic, olist, or a folder path")
    gd.set_defaults(func=_cmd_gold)

    lg = sub.add_parser("load-gold", help="step 4: load the star schema into Postgres <source>_gold")
    lg.add_argument("--source", default="synthetic", help="synthetic, olist, or a folder path")
    lg.set_defaults(func=_cmd_load_gold)

    qa = sub.add_parser("quality", help="step 5: run all data quality checks on the current lake")
    qa.add_argument("--source", default="synthetic", help="synthetic, olist, or a folder path")
    qa.add_argument("--no-db", action="store_true", help="do not save results to Postgres")
    qa.set_defaults(func=_cmd_quality)

    rn = sub.add_parser("run", help="step 5: whole batch pipeline with quality gates")
    rn.add_argument("--source", default="synthetic", help="synthetic, olist, or a folder path")
    rn.add_argument("--no-load", action="store_true", help="stop after gold; do not touch Postgres")
    rn.set_defaults(func=_cmd_run)

    an = sub.add_parser("analyze", help="step 6: run the 10 business questions in SQL on <source>_gold")
    an.add_argument("--source", default="synthetic", help="synthetic, olist, or a folder path")
    an.set_defaults(func=_cmd_analyze)

    bm = sub.add_parser("benchmark", help="step 7: Spark configs, data layout and Postgres indexes, measured")
    bm.add_argument("--source", default="synthetic", help="synthetic, olist, or a folder path")
    bm.add_argument("--repeat", type=int, default=1, help="runs per Spark config (median is reported)")
    bm.add_argument("--no-sql", action="store_true", help="skip the Postgres index benchmark")
    bm.set_defaults(func=_cmd_benchmark)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
