"""Data profiling: look at the raw files *before* writing any transformation.

Profiling answers "what does this data actually look like?":
row counts, nulls, distinct values, values that do not parse as their
declared type, duplicate keys, orphaned foreign keys, impossible
timestamps. Every finding here becomes a candidate data-quality check in
step 5, so profiling is where the rules come from.

Everything is read as text (``dtype=str``) on purpose. If pandas guessed
types, a price like "129,90" would silently become NaN and a zip code like
"01037" would silently become 1037; you would never see the problem.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .contracts import (
    FLOAT,
    INT,
    ORDER_TIMESTAMP_SEQUENCE,
    TABLES,
    TABLES_BY_NAME,
    TIMESTAMP,
    TIMESTAMP_FORMAT,
    Table,
)
from .generator import load_manifest

ERROR, WARN, INFO = "error", "warn", "info"
_SEVERITY_ORDER = {ERROR: 0, WARN: 1, INFO: 2}
RECONCILIATION_TOLERANCE = 1.00  # currency units (BRL)


@dataclass
class Finding:
    severity: str
    table: str
    check: str
    count: int
    detail: str


@dataclass
class TableProfile:
    name: str
    file_name: str
    present: bool
    rows: int = 0
    missing_columns: List[str] = field(default_factory=list)
    extra_columns: List[str] = field(default_factory=list)
    columns: List[dict] = field(default_factory=list)


@dataclass
class Profile:
    source: str
    path: str
    reference_time: str
    tables: Dict[str, TableProfile]
    findings: List[Finding]

    def errors(self) -> List[Finding]:
        return [f for f in self.findings if f.severity == ERROR]

    def count(self, table: str, check: str) -> int:
        return sum(f.count for f in self.findings if f.table == table and f.check == check)


def read_raw(path: Path) -> pd.DataFrame:
    """Read a CSV with every value as text; only empty fields become null."""
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""], encoding="utf-8-sig")


def parse_typed(series: pd.Series, dtype: str):
    """Return (parsed values, mask of non-null values that failed to parse)."""
    if dtype in (INT, FLOAT):
        parsed = pd.to_numeric(series, errors="coerce")
        failed = series.notna() & parsed.isna()
        if dtype == INT:
            failed |= parsed.notna() & (parsed % 1 != 0)
        return parsed, failed
    if dtype == TIMESTAMP:
        parsed = pd.to_datetime(series, format=TIMESTAMP_FORMAT, errors="coerce")
        return parsed, series.notna() & parsed.isna()
    return series, pd.Series(False, index=series.index)


def _column_stats(table: Table, df: pd.DataFrame, findings: List[Finding]) -> List[dict]:
    rows = len(df)
    stats = []
    for col in table.columns:
        if col.name not in df.columns:
            continue
        s = df[col.name]
        nulls = int(s.isna().sum())
        entry = {
            "column": col.name,
            "type": col.dtype,
            "nulls": nulls,
            "null_pct": round(100 * nulls / rows, 2) if rows else 0.0,
            "distinct": int(s.nunique(dropna=True)),
            "min": "",
            "max": "",
            "top_values": "",
        }
        if nulls and not col.nullable:
            findings.append(Finding(ERROR, table.name, f"not_null:{col.name}", nulls,
                                    f"{col.name} is required but empty in {nulls} rows"))

        parsed, failed = parse_typed(s, col.dtype)
        n_failed = int(failed.sum())
        if n_failed:
            examples = ", ".join(repr(v) for v in s[failed].unique()[:3])
            findings.append(Finding(ERROR, table.name, f"type:{col.name}", n_failed,
                                    f"{n_failed} values are not a valid {col.dtype} (e.g. {examples})"))
        if col.dtype != "string":
            valid = parsed[~failed].dropna()
            if len(valid):
                entry["min"], entry["max"] = str(valid.min()), str(valid.max())
            if col.min_value is not None and col.dtype in (INT, FLOAT):
                bad = (valid <= col.min_value) if not col.min_inclusive else (valid < col.min_value)
                n_bad = int(bad.sum())
                if n_bad:
                    op = ">" if not col.min_inclusive else ">="
                    findings.append(Finding(ERROR, table.name, f"range:{col.name}", n_bad,
                                            f"{n_bad} values violate {col.name} {op} {col.min_value:g}"))

        if col.accepted_values:
            present = s.dropna()
            invalid = present[~present.isin(col.accepted_values)]
            fixable = invalid[invalid.str.strip().str.lower().isin(col.accepted_values)]
            if len(fixable):
                findings.append(Finding(WARN, table.name, f"accepted_values_fixable:{col.name}", len(fixable),
                                        f"valid after trim/lowercase, e.g. {fixable.iloc[0]!r}"))
            n_invalid = len(invalid) - len(fixable)
            if n_invalid:
                values = sorted(set(invalid) - set(fixable))[:5]
                findings.append(Finding(ERROR, table.name, f"accepted_values_invalid:{col.name}", n_invalid,
                                        f"values outside the accepted set: {values}"))

        if 0 < entry["distinct"] <= 12:
            top = s.value_counts(dropna=True).head(6)
            entry["top_values"] = ", ".join(f"{k} ({v})" for k, v in top.items())
        stats.append(entry)
    return stats


def _key_checks(table: Table, df: pd.DataFrame, findings: List[Finding]) -> None:
    exact = int(df.duplicated(keep="first").sum())
    if exact:
        findings.append(Finding(ERROR, table.name, "duplicate_rows", exact,
                                f"{exact} rows are exact copies of an earlier row"))
    pk = list(table.primary_key)
    if all(c in df.columns for c in pk):
        dup_keys = int(df.duplicated(subset=pk, keep="first").sum()) - exact
        if dup_keys > 0:
            findings.append(Finding(ERROR, table.name, "duplicate_key", dup_keys,
                                    f"{dup_keys} rows reuse a primary key {pk} with different values"))


def _foreign_key_checks(frames: Dict[str, pd.DataFrame], findings: List[Finding]) -> None:
    for table in TABLES:
        child = frames.get(table.name)
        if child is None:
            continue
        for fk in table.foreign_keys:
            parent = frames.get(fk.ref_table)
            if parent is None:
                continue
            col, ref = fk.columns[0], fk.ref_columns[0]
            values = child[col].dropna()
            orphans = values[~values.isin(set(parent[ref].dropna()))]
            if len(orphans):
                findings.append(Finding(ERROR, table.name, f"fk:{col}->{fk.ref_table}", len(orphans),
                                        f"{len(orphans)} rows point to a {fk.ref_table}.{ref} that does not exist "
                                        f"(e.g. {orphans.iloc[0]!r})"))


def _order_checks(frames: Dict[str, pd.DataFrame], reference: datetime, findings: List[Finding]) -> None:
    orders = frames.get("orders")
    if orders is None:
        return
    table = TABLES_BY_NAME["orders"]
    ts = {c.name: parse_typed(orders[c.name], TIMESTAMP)[0] for c in table.columns if c.dtype == TIMESTAMP}

    for later, earlier in ORDER_TIMESTAMP_SEQUENCE:
        bad = int((ts[later] < ts[earlier]).sum())  # NaT comparisons are False
        if bad:
            findings.append(Finding(ERROR, "orders", f"sequence:{later}>={earlier}", bad,
                                    f"{bad} orders have {later} earlier than {earlier}"))

    event_cols = [c.name for c in table.columns if c.event_time]
    future = pd.concat([ts[c] > pd.Timestamp(reference) for c in event_cols], axis=1).any(axis=1)
    if int(future.sum()):
        findings.append(Finding(ERROR, "orders", "future_timestamps", int(future.sum()),
                                f"{int(future.sum())} orders have events after {reference:%Y-%m-%d %H:%M}"))

    items, payments = frames.get("order_items"), frames.get("order_payments")
    order_ids = set(orders["order_id"].dropna())
    if items is not None:
        no_items = len(order_ids - set(items["order_id"].dropna()))
        if no_items:
            findings.append(Finding(INFO, "orders", "orders_without_items", no_items,
                                    f"{no_items} orders have no order lines (often canceled/unavailable)"))
    if payments is not None:
        no_pay = len(order_ids - set(payments["order_id"].dropna()))
        if no_pay:
            findings.append(Finding(WARN, "orders", "orders_without_payments", no_pay,
                                    f"{no_pay} orders have no payment"))
    if items is not None and payments is not None:
        line_total = (pd.to_numeric(items["price"], errors="coerce")
                      + pd.to_numeric(items["freight_value"], errors="coerce"))
        item_sum = line_total.groupby(items["order_id"]).sum(min_count=1)
        pay_sum = pd.to_numeric(payments["payment_value"], errors="coerce").groupby(payments["order_id"]).sum()
        both = item_sum.index.intersection(pay_sum.index)
        diff = (pay_sum[both] - item_sum[both]).abs()
        mismatched = int((diff > RECONCILIATION_TOLERANCE).sum() + item_sum[both].isna().sum())
        if mismatched:
            findings.append(Finding(WARN, "orders", "payment_reconciliation", mismatched,
                                    f"{mismatched} orders: payments differ from items+freight by more than "
                                    f"{RECONCILIATION_TOLERANCE:.2f}"))


def profile_source(source_dir: Path, source_name: Optional[str] = None,
                   reference_time: Optional[datetime] = None) -> Profile:
    """Profile every contract table found in ``source_dir``."""
    source_dir = Path(source_dir)
    manifest = load_manifest(source_dir)
    if reference_time is None:
        reference_time = (datetime.strptime(manifest["extracted_at"], TIMESTAMP_FORMAT)
                          if manifest else datetime.now())

    findings: List[Finding] = []
    frames: Dict[str, pd.DataFrame] = {}
    tables: Dict[str, TableProfile] = {}
    for table in TABLES:
        path = source_dir / table.file_name
        tp = TableProfile(table.name, table.file_name, path.exists())
        tables[table.name] = tp
        if not path.exists():
            findings.append(Finding(ERROR, table.name, "file_missing", 1, f"{table.file_name} not found"))
            continue
        df = read_raw(path)
        frames[table.name] = df
        tp.rows = len(df)
        tp.missing_columns = [c for c in table.column_names if c not in df.columns]
        tp.extra_columns = [c for c in df.columns if c not in table.column_names]
        if tp.missing_columns:
            findings.append(Finding(ERROR, table.name, "schema_missing_columns", len(tp.missing_columns),
                                    f"missing columns: {tp.missing_columns}"))
        if tp.extra_columns:
            findings.append(Finding(WARN, table.name, "schema_extra_columns", len(tp.extra_columns),
                                    f"unexpected columns: {tp.extra_columns}"))
        tp.columns = _column_stats(table, df, findings)
        _key_checks(table, df, findings)

    _foreign_key_checks(frames, findings)
    _order_checks(frames, reference_time, findings)
    findings.sort(key=lambda f: (_SEVERITY_ORDER[f.severity], f.table, f.check))
    return Profile(source_name or source_dir.name, str(source_dir), f"{reference_time:%Y-%m-%d %H:%M:%S}",
                   tables, findings)


# Which profiling check should detect each injected defect type.
DEFECT_TO_CHECK = {
    ("customers", "duplicate_row"): ("customers", "duplicate_rows"),
    ("products", "category_missing_translation"): ("products", "fk:product_category_name->category_translation"),
    ("orders", "duplicate_row"): ("orders", "duplicate_rows"),
    ("orders", "null_customer_id"): ("orders", "not_null:customer_id"),
    ("orders", "status_unnormalized"): ("orders", "accepted_values_fixable:order_status"),
    ("orders", "invalid_status"): ("orders", "accepted_values_invalid:order_status"),
    ("orders", "delivered_before_purchase"): (
        "orders", "sequence:order_delivered_customer_date>=order_purchase_timestamp"),
    ("orders", "future_purchase"): ("orders", "future_timestamps"),
    ("order_items", "orphan_product"): ("order_items", "fk:product_id->products"),
    ("order_items", "non_positive_price"): ("order_items", "range:price"),
    ("order_items", "price_comma_decimal"): ("order_items", "type:price"),
    ("order_payments", "duplicate_row"): ("order_payments", "duplicate_rows"),
    ("order_payments", "invalid_payment_type"): ("order_payments", "accepted_values_invalid:payment_type"),
    ("order_payments", "orphan_order"): ("order_payments", "fk:order_id->orders"),
}


def compare_with_manifest(profile: Profile, manifest: dict) -> List[dict]:
    """Injected (ground truth) vs detected (profiling) for each defect type."""
    rows = []
    for d in manifest["defects"]:
        table, check = DEFECT_TO_CHECK[(d["table"], d["defect_type"])]
        detected = profile.count(table, check)
        rows.append({"table": d["table"], "defect_type": d["defect_type"], "injected": d["count"],
                     "detected": detected, "check": check, "match": detected == d["count"]})
    return rows


def render_markdown(profile: Profile, comparison: Optional[List[dict]] = None) -> str:
    out = [f"# Data profile: `{profile.source}`", "",
           f"- Path: `{profile.path}`",
           f"- Reference time (extraction moment): `{profile.reference_time}`",
           f"- Generated: `{datetime.now():%Y-%m-%d %H:%M:%S}`", "",
           "## Tables", "", "| table | file | rows | columns ok |", "|---|---|---:|---|"]
    for tp in profile.tables.values():
        ok = "missing file" if not tp.present else ("yes" if not tp.missing_columns else "NO")
        out.append(f"| {tp.name} | `{tp.file_name}` | {tp.rows:,} | {ok} |")

    sev = {s: sum(1 for f in profile.findings if f.severity == s) for s in (ERROR, WARN, INFO)}
    out += ["", f"## Findings ({sev[ERROR]} error, {sev[WARN]} warn, {sev[INFO]} info)", ""]
    if profile.findings:
        out += ["| severity | table | check | rows | detail |", "|---|---|---|---:|---|"]
        for f in profile.findings:
            out.append(f"| {f.severity} | {f.table} | `{f.check}` | {f.count:,} | {f.detail.replace('|', '/')} |")
    else:
        out.append("No findings.")

    if comparison:
        caught = sum(r["match"] for r in comparison)
        out += ["", f"## Injected vs detected ({caught}/{len(comparison)} defect types match exactly)", "",
                "| table | defect | injected | detected | check | match |", "|---|---|---:|---:|---|---|"]
        for r in comparison:
            out.append(f"| {r['table']} | {r['defect_type']} | {r['injected']:,} | {r['detected']:,} | "
                       f"`{r['check']}` | {'yes' if r['match'] else 'NO'} |")

    out += ["", "## Columns"]
    for tp in profile.tables.values():
        if not tp.present:
            continue
        out += ["", f"### {tp.name} ({tp.rows:,} rows)", "",
                "| column | type | nulls | null % | distinct | min | max | top values |",
                "|---|---|---:|---:|---:|---|---|---|"]
        for c in tp.columns:
            out.append(f"| {c['column']} | {c['type']} | {c['nulls']:,} | {c['null_pct']} | {c['distinct']:,} | "
                       f"{c['min']} | {c['max']} | {c['top_values']} |")
    return "\n".join(out) + "\n"
