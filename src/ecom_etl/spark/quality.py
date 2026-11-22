"""Data quality framework: named checks, gates, reports and measurement.

Two kinds of checks:

* **Row-level rules** (``kind="row"``) run inside silver. Each rule
  flags individual rows; flagged rows go to quarantine with the rule's
  reason code. They never stop the pipeline: bad rows are isolated,
  good rows flow on. Their result is "how many rows did this rule catch".
* **Table-level gates** (``kind="table"``) look at a whole table after a
  layer is built: unique keys, completeness, reconciliation, freshness,
  quarantine rate. A failed ``blocking`` gate stops the pipeline before
  bad data is published (the *write-audit-publish* pattern).

On synthetic data the quarantine can also be **measured** against the
generator's ground truth (``_defects.csv``): precision and recall, like
a classifier.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from ..contracts import FLOAT, INT, ORDER_TIMESTAMP_SEQUENCE, STRING, TABLES, TABLES_BY_NAME
from .lake import Lake
from .silver import FIX_INSTEAD_OF_FK, HARD_SEQUENCE, TableReport, quarantine_schema, read_layer

ERROR, WARN = "error", "warn"
PASS, FAIL = "pass", "fail"
MAX_QUARANTINE_RATE = 0.10
FRESHNESS_DAYS = 2
RECONCILIATION_TOLERANCE = 1.00
VOLUME_CHANGE_LIMIT = 0.5


@dataclass(frozen=True)
class Check:
    name: str
    table: str
    layer: str          # silver | gold
    kind: str           # row | table
    severity: str       # error | warn
    blocking: bool
    description: str


@dataclass
class CheckResult:
    name: str
    table: str
    layer: str
    kind: str
    severity: str
    blocking: bool
    status: str
    failed_rows: int = 0
    total_rows: int = 0
    detail: str = ""


class QualityGateError(RuntimeError):
    def __init__(self, failures: List[CheckResult]):
        self.failures = failures
        names = ", ".join(f.name for f in failures)
        super().__init__(f"{len(failures)} blocking data quality check(s) failed: {names}")


# --------------------------------------------------------------------------- registry
def row_level_checks() -> List[Check]:
    """One check per rule that silver applies (generated from the contract)."""
    checks: List[Check] = []
    for t in TABLES:
        checks.append(Check(f"{t.name}.exact_duplicate", t.name, "silver", "row", WARN, False,
                            "exact duplicate rows are dropped (dedupe)"))
        checks.append(Check(f"{t.name}.duplicate_key", t.name, "silver", "row", ERROR, False,
                            f"primary key {t.primary_key} reused with different values"))
        for c in t.columns:
            if not c.nullable:
                checks.append(Check(f"{t.name}.not_null:{c.name}", t.name, "silver", "row", ERROR, False,
                                    f"{c.name} is required"))
            if c.dtype != STRING:
                checks.append(Check(f"{t.name}.type:{c.name}", t.name, "silver", "row", ERROR, False,
                                    f"{c.name} must be a valid {c.dtype}"))
            if c.accepted_values:
                checks.append(Check(f"{t.name}.accepted_values:{c.name}", t.name, "silver", "row", ERROR, False,
                                    f"{c.name} in {list(c.accepted_values)}"))
            if c.min_value is not None and c.dtype in (INT, FLOAT):
                op = ">=" if c.min_inclusive else ">"
                checks.append(Check(f"{t.name}.range:{c.name}", t.name, "silver", "row", ERROR, False,
                                    f"{c.name} {op} {c.min_value:g}"))
        for fk in t.foreign_keys:
            col = fk.columns[0]
            if (t.name, col) in FIX_INSTEAD_OF_FK:
                continue
            checks.append(Check(f"{t.name}.fk:{col}->{fk.ref_table}", t.name, "silver", "row", ERROR, False,
                                f"{col} must exist in a valid {fk.ref_table} row"))
    checks.append(Check("orders.sequence:delivered_before_purchase", "orders", "silver", "row", ERROR, False,
                        "delivered to the customer before it was purchased"))
    checks.append(Check("orders.future_event", "orders", "silver", "row", ERROR, False,
                        "an event timestamp is later than the extraction time"))
    for a, b in ORDER_TIMESTAMP_SEQUENCE:
        if (a, b) != HARD_SEQUENCE:
            checks.append(Check(f"orders.sequence:{a}<{b}", "orders", "silver", "row", WARN, False,
                                f"{a} earlier than {b} (kept, flagged in _warnings)"))
    return checks


def table_level_checks() -> List[Check]:
    checks: List[Check] = []
    for t in TABLES:
        checks += [
            Check(f"{t.name}.silver_not_empty", t.name, "silver", "table", ERROR, True, "silver has rows"),
            Check(f"{t.name}.silver_key_unique", t.name, "silver", "table", ERROR, True,
                  f"primary key {t.primary_key} unique in silver"),
            Check(f"{t.name}.quarantine_rate", t.name, "silver", "table", ERROR, True,
                  f"quarantined / bronze <= {MAX_QUARANTINE_RATE:.0%}"),
        ]
    checks += [
        Check("orders.freshness", "orders", "silver", "table", WARN, False,
              f"latest purchase within {FRESHNESS_DAYS} days of the extraction time"),
        Check("orders.volume_change", "orders", "silver", "table", WARN, False,
              f"silver orders changed by less than {VOLUME_CHANGE_LIMIT:.0%} since the previous run"),
        Check("orders.payment_reconciliation", "orders", "silver", "table", WARN, False,
              f"payments match items + freight within {RECONCILIATION_TOLERANCE:.2f} per order"),
        Check("fact_order_line.grain_unique", "fact_order_line", "gold", "table", ERROR, True,
              "one row per (order_id, order_item_id)"),
        Check("fact_order_line.complete", "fact_order_line", "gold", "table", ERROR, True,
              "every silver order line is in the fact"),
        Check("fact_order_line.revenue_reconciles", "fact_order_line", "gold", "table", ERROR, True,
              "sum(line_total) equals silver sum(price + freight)"),
        Check("fact_order_line.no_unknown_members", "fact_order_line", "gold", "table", ERROR, True,
              "no fact row points at a -1 dimension member"),
        Check("fact_order_line.dates_in_dim_date", "fact_order_line", "gold", "table", ERROR, True,
              "every date_key exists in dim_date"),
        Check("fact_payment.complete", "fact_payment", "gold", "table", ERROR, True,
              "every silver payment is in the fact"),
        Check("dim_customer.one_current_version", "dim_customer", "gold", "table", ERROR, True,
              "exactly one is_current row per customer_unique_id (SCD2)"),
    ]
    for dim, key in (("dim_date", "date_key"), ("dim_customer", "customer_sk"),
                     ("dim_product", "product_sk"), ("dim_seller", "seller_sk")):
        checks.append(Check(f"{dim}.key_unique", dim, "gold", "table", ERROR, True, f"{key} is unique"))
    return checks


def all_checks() -> List[Check]:
    return row_level_checks() + table_level_checks()


def _result(check: Check, failed: int, total: int = 0, detail: str = "") -> CheckResult:
    return CheckResult(check.name, check.table, check.layer, check.kind, check.severity, check.blocking,
                       FAIL if failed else PASS, int(failed), int(total), detail)


# --------------------------------------------------------------------------- row level
def row_level_results(silver_reports: Dict[str, TableReport]) -> List[CheckResult]:
    """Turn the reason counts of a silver run into one result per row-level check."""
    counts: Dict[str, int] = {}
    for table, report in silver_reports.items():
        counts[f"{table}.exact_duplicate"] = report.exact_duplicates
        fk_by_parent = {fk.ref_table: f"{table}.fk:{fk.columns[0]}->{fk.ref_table}"
                        for fk in TABLES_BY_NAME[table].foreign_keys}
        for reason, n in list(report.reasons.items()) + list(report.warnings.items()):
            if reason.startswith("parent_quarantined:"):
                name = fk_by_parent[reason.split(":", 1)[1]]
            else:
                name = f"{table}.{reason}"
            counts[name] = counts.get(name, 0) + n
    results = []
    for check in row_level_checks():
        total = silver_reports[check.table].bronze_rows if check.table in silver_reports else 0
        results.append(_result(check, counts.get(check.name, 0), total))
    return results


# --------------------------------------------------------------------------- table level
def _key_dupes(df, keys: Iterable[str]) -> int:
    keys = list(keys)
    return df.count() - df.select(*keys).distinct().count()


def silver_gate_results(spark, lake: Lake, silver_reports: Dict[str, TableReport], extracted_at: str,
                        max_quarantine_rate: float = MAX_QUARANTINE_RATE) -> List[CheckResult]:
    from pyspark.sql import functions as F

    checks = {c.name: c for c in table_level_checks()}
    results = []
    for t in TABLES:
        df = read_layer(spark, lake, "silver", t.name)
        rows = df.count()
        rep = silver_reports[t.name]
        results.append(_result(checks[f"{t.name}.silver_not_empty"], 0 if rows else 1, rows))
        results.append(_result(checks[f"{t.name}.silver_key_unique"], _key_dupes(df, t.primary_key), rows))
        rate = rep.quarantined / rep.bronze_rows if rep.bronze_rows else 0.0
        results.append(_result(checks[f"{t.name}.quarantine_rate"],
                               rep.quarantined if rate > max_quarantine_rate else 0, rep.bronze_rows,
                               f"{rate:.2%} quarantined (limit {max_quarantine_rate:.0%})"))

    orders = read_layer(spark, lake, "silver", "orders")
    latest = orders.agg(F.max("order_purchase_timestamp")).first()[0]
    cutoff = spark.sql(f"SELECT to_timestamp('{extracted_at}') AS t").first()["t"]
    lag_days = (cutoff - latest).total_seconds() / 86400 if latest else None
    stale = lag_days is None or lag_days > FRESHNESS_DAYS
    results.append(_result(checks["orders.freshness"], 1 if stale else 0, 1,
                           f"latest purchase {latest}, {lag_days:.1f} days before extraction" if latest
                           else "no orders"))

    state_path = lake.root / "_state" / "last_silver_counts.json"
    previous = json.loads(state_path.read_text()) if state_path.exists() else {}
    current = silver_reports["orders"].silver_rows
    before = previous.get("orders")
    change = abs(current - before) / before if before else 0.0
    results.append(_result(checks["orders.volume_change"], 1 if change > VOLUME_CHANGE_LIMIT else 0, current,
                           f"{before} -> {current} rows" if before is not None else "first run, no baseline"))
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps({k: r.silver_rows for k, r in silver_reports.items()}))

    items = read_layer(spark, lake, "silver", "order_items")
    pays = read_layer(spark, lake, "silver", "order_payments")
    item_tot = items.groupBy("order_id").agg(F.sum(F.col("price") + F.col("freight_value")).alias("items"))
    pay_tot = pays.groupBy("order_id").agg(F.sum("payment_value").alias("paid"))
    both = item_tot.join(pay_tot, "order_id")
    mismatched = both.filter(F.abs(F.col("items") - F.col("paid")) > RECONCILIATION_TOLERANCE).count()
    results.append(_result(checks["orders.payment_reconciliation"], mismatched, both.count(),
                           f"{mismatched} orders differ by more than {RECONCILIATION_TOLERANCE:.2f}"))
    return results


def gold_gate_results(spark, lake: Lake) -> List[CheckResult]:
    from pyspark.sql import functions as F

    checks = {c.name: c for c in table_level_checks()}
    g = {t: read_layer(spark, lake, "gold", t) for t in
         ("fact_order_line", "fact_payment", "dim_customer", "dim_product", "dim_seller", "dim_date")}
    items = read_layer(spark, lake, "silver", "order_items")
    pays = read_layer(spark, lake, "silver", "order_payments")
    fact = g["fact_order_line"]
    fact_rows, item_rows = fact.count(), items.count()

    results = [
        _result(checks["fact_order_line.grain_unique"], _key_dupes(fact, ("order_id", "order_item_id")), fact_rows),
        _result(checks["fact_order_line.complete"], abs(fact_rows - item_rows), item_rows,
                f"fact {fact_rows} rows, silver {item_rows} rows"),
    ]
    fact_total = fact.agg(F.sum("line_total")).first()[0] or 0
    silver_total = items.agg(F.sum(F.col("price") + F.col("freight_value"))).first()[0] or 0
    results.append(_result(checks["fact_order_line.revenue_reconciles"], 0 if fact_total == silver_total else 1, 1,
                           f"fact {fact_total} vs silver {silver_total}"))
    unknown = fact.filter((F.col("customer_sk") == -1) | (F.col("product_sk") == -1)
                          | (F.col("seller_sk") == -1)).count()
    results.append(_result(checks["fact_order_line.no_unknown_members"], unknown, fact_rows))
    missing_dates = fact.select("date_key").join(g["dim_date"].select("date_key"), "date_key", "left_anti").count()
    results.append(_result(checks["fact_order_line.dates_in_dim_date"], missing_dates, fact_rows))
    pay_rows, silver_pays = g["fact_payment"].count(), pays.count()
    results.append(_result(checks["fact_payment.complete"], abs(pay_rows - silver_pays), silver_pays))
    people = (g["dim_customer"].filter(F.col("customer_sk") != -1)
              .groupBy("customer_unique_id").agg(F.sum(F.col("is_current").cast("int")).alias("current")))
    results.append(_result(checks["dim_customer.one_current_version"],
                           people.filter(F.col("current") != 1).count(), people.count()))
    for dim, key in (("dim_date", "date_key"), ("dim_customer", "customer_sk"),
                     ("dim_product", "product_sk"), ("dim_seller", "seller_sk")):
        results.append(_result(checks[f"{dim}.key_unique"], _key_dupes(g[dim], [key]), g[dim].count()))
    return results


def gate(results: List[CheckResult]) -> None:
    """Raise if any blocking check failed."""
    failures = [r for r in results if r.blocking and r.status == FAIL]
    if failures:
        raise QualityGateError(failures)


# --------------------------------------------------------------------------- ground truth
QUARANTINE_DEFECTS = {
    ("orders", "null_customer_id"), ("orders", "invalid_status"), ("orders", "delivered_before_purchase"),
    ("orders", "future_purchase"), ("order_items", "orphan_product"), ("order_items", "non_positive_price"),
    ("order_payments", "invalid_payment_type"), ("order_payments", "orphan_order"),
}
FIX_TAGS = {
    ("orders", "status_unnormalized"): "normalized:order_status",
    ("order_items", "price_comma_decimal"): "decimal_comma:price",
    ("products", "category_missing_translation"): "added_translation:product_category_name",
}
KEY_COLUMNS = {
    "orders": ("order_id",), "order_items": ("order_id", "order_item_id"),
    "order_payments": ("order_id", "payment_sequential"), "products": ("product_id",),
}


def _keys(rows, table) -> set:
    return {"|".join(str(r[c]) for c in KEY_COLUMNS[table]) for r in rows}


def evaluate_against_ground_truth(spark, lake: Lake, source_dir: Path,
                                  silver_reports: Dict[str, TableReport]) -> Optional[dict]:
    """Precision and recall of the quarantine, recall of fixes and dedupe (synthetic data only)."""
    from pyspark.sql import functions as F

    path = Path(source_dir) / "_defects.csv"
    if not path.exists():
        return None
    truth: Dict[tuple, set] = {}
    with path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            truth.setdefault((r["table"], r["defect_type"]), set()).add(r["key"])

    tp = fp = fn = 0
    by_table = {}
    for table in ("orders", "order_items", "order_payments"):
        q = read_layer(spark, lake, "quarantine", table, quarantine_schema(TABLES_BY_NAME[table]))
        primary = q.filter(F.exists("_reasons", lambda r: ~r.startswith("parent_quarantined:")))
        flagged = _keys(primary.select(*KEY_COLUMNS[table]).collect(), table)
        expected = set().union(*[truth.get(k, set()) for k in QUARANTINE_DEFECTS if k[0] == table])
        t_tp, t_fp, t_fn = len(flagged & expected), len(flagged - expected), len(expected - flagged)
        by_table[table] = {"true_positive": t_tp, "false_positive": t_fp, "false_negative": t_fn}
        tp, fp, fn = tp + t_tp, fp + t_fp, fn + t_fn

    fixes = {}
    for (table, defect), tag in FIX_TAGS.items():
        keys = truth.get((table, defect), set())
        silver = read_layer(spark, lake, "silver", table)
        tagged = _keys(silver.filter(F.array_contains("_fixes", tag)).select(*KEY_COLUMNS[table]).collect(), table)
        cascaded = set()
        if table != "products":  # a fixable row whose parent order was rejected never reaches silver
            q = read_layer(spark, lake, "quarantine", table, quarantine_schema(TABLES_BY_NAME[table]))
            cascaded = _keys(q.select(*KEY_COLUMNS[table]).collect(), table)
        handled = len(keys & (tagged | cascaded))
        fixes[f"{table}.{defect}"] = {"injected": len(keys), "handled": handled}

    dedupe = {}
    for (table, defect), keys in truth.items():
        if defect == "duplicate_row":
            dedupe[table] = {"injected": len(keys), "removed": silver_reports[table].exact_duplicates}

    return {
        "quarantine": {"precision": tp / (tp + fp) if tp + fp else 1.0,
                       "recall": tp / (tp + fn) if tp + fn else 1.0,
                       "true_positive": tp, "false_positive": fp, "false_negative": fn, "by_table": by_table},
        "fixes": fixes,
        "dedupe": dedupe,
    }


# --------------------------------------------------------------------------- reporting
def summarize(results: List[CheckResult], silver_reports: Dict[str, TableReport]) -> dict:
    row = [r for r in results if r.kind == "row"]
    table = [r for r in results if r.kind == "table"]
    return {
        "checks_total": len(results),
        "row_level_checks": len(row),
        "table_level_checks": len(table),
        "checks_failed": sum(r.status == FAIL for r in results),
        "blocking_failures": sum(r.status == FAIL and r.blocking for r in results),
        "records_quarantined": sum(r.quarantined for r in silver_reports.values()),
        "duplicates_removed": sum(r.exact_duplicates for r in silver_reports.values()),
        "records_fixed": sum(sum(r.fixes.values()) for r in silver_reports.values()),
        "records_with_warnings": sum(sum(r.warnings.values()) for r in silver_reports.values()),
        "bronze_records": sum(r.bronze_rows for r in silver_reports.values()),
    }


def render_markdown(source: str, run_id: str, summary: dict, results: List[CheckResult],
                    evaluation: Optional[dict]) -> str:
    out = [f"# Data quality report: `{source}`", "", f"- Run: `{run_id}`",
           f"- Checks: **{summary['checks_total']}** ({summary['row_level_checks']} row-level rules, "
           f"{summary['table_level_checks']} table-level gates)",
           f"- Failed: {summary['checks_failed']} (blocking: {summary['blocking_failures']})",
           f"- Bronze records: {summary['bronze_records']:,}",
           f"- Flagged: **{summary['records_quarantined']:,} quarantined**, "
           f"{summary['duplicates_removed']:,} duplicates removed, {summary['records_fixed']:,} fixed, "
           f"{summary['records_with_warnings']:,} warnings", ""]
    if evaluation:
        q = evaluation["quarantine"]
        out += ["## Measured against injected defects", "",
                f"Quarantine precision **{q['precision']:.1%}**, recall **{q['recall']:.1%}** "
                f"(TP {q['true_positive']}, FP {q['false_positive']}, FN {q['false_negative']}).", "",
                "| fix | injected | handled |", "|---|---:|---:|"]
        out += [f"| {k} | {v['injected']} | {v['handled']} |" for k, v in evaluation["fixes"].items()]
        out += ["", "| dedupe | injected | removed |", "|---|---:|---:|"]
        out += [f"| {k} | {v['injected']} | {v['removed']} |" for k, v in evaluation["dedupe"].items()]
        out.append("")
    out += ["## Checks that flagged something", "",
            "| check | kind | severity | blocking | rows | detail |", "|---|---|---|---|---:|---|"]
    out += [f"| `{r.name}` | {r.kind} | {r.severity} | {'yes' if r.blocking else 'no'} | {r.failed_rows:,} | "
            f"{r.detail} |" for r in results if r.status == FAIL]
    out += ["", f"All other {sum(r.status == PASS for r in results)} checks passed.", ""]
    return "\n".join(out)


def save_results(source: str, run_id: str, results: List[CheckResult], silver_reports: Dict[str, TableReport],
                 settings=None) -> None:
    """Append this run's results to Postgres <source>_dq (history for the dashboard)."""
    from ..db import connect, schema_name

    schema = schema_name(source, "dq")
    with connect(settings) as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.check_results (
            run_id TEXT, checked_at TIMESTAMPTZ DEFAULT now(), check_name TEXT, table_name TEXT,
            layer TEXT, kind TEXT, severity TEXT, blocking BOOLEAN, status TEXT,
            failed_rows BIGINT, total_rows BIGINT, detail TEXT)""")
        conn.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.quarantine_summary (
            run_id TEXT, checked_at TIMESTAMPTZ DEFAULT now(), table_name TEXT, reason TEXT, rows BIGINT)""")
        with conn.cursor() as cur:
            cur.executemany(
                f"""INSERT INTO {schema}.check_results (run_id, check_name, table_name, layer, kind, severity,
                    blocking, status, failed_rows, total_rows, detail) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                [(run_id, r.name, r.table, r.layer, r.kind, r.severity, r.blocking, r.status,
                  r.failed_rows, r.total_rows, r.detail) for r in results])
            cur.executemany(
                f"INSERT INTO {schema}.quarantine_summary (run_id, table_name, reason, rows) VALUES (%s,%s,%s,%s)",
                [(run_id, t, reason, n) for t, rep in silver_reports.items() for reason, n in rep.reasons.items()])
        conn.commit()


def results_as_dicts(results: List[CheckResult]) -> List[dict]:
    return [asdict(r) for r in results]
