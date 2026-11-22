"""Command line entry point: ``python -m ecom_etl <command>``.

Commands added so far (Step 1):
  generate        write synthetic Olist-shaped CSVs with injected defects
  download-olist  fetch the real Olist dataset from Kaggle (or a local zip)
  validate        check files and headers against the source contract
  profile         profile a source and write a markdown report
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
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
