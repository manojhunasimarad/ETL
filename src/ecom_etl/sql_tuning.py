"""Step 7: measure Postgres indexes with EXPLAIN (ANALYZE) before and after.

For each query: execution time (median of several runs, after a warm-up)
and how Postgres read the fact table (Seq Scan vs Index / Bitmap scan),
first without secondary indexes, then with ``sql/gold_indexes.sql``.
"""

from __future__ import annotations

import re
import statistics
from typing import List, Optional

from .db import DbSettings, connect, read_sql, schema_name

INDEX_NAMES = re.compile(r"CREATE INDEX IF NOT EXISTS (\w+)")


def _queries(gold: str, customer: int, product: int, month_start: int, month_end: int) -> dict:
    return {
        "one customer's order history": f"""
            SELECT f.order_id, f.order_item_id, f.line_total, f.purchased_at
            FROM {gold}.fact_order_line f WHERE f.customer_sk = {customer}""",
        "one product's sales by month": f"""
            SELECT d.year_month, sum(f.line_total)
            FROM {gold}.fact_order_line f JOIN {gold}.dim_date d ON d.date_key = f.date_key
            WHERE f.product_sk = {product} GROUP BY d.year_month""",
        "one month's revenue by category": f"""
            SELECT p.category_name_english, sum(f.line_total)
            FROM {gold}.fact_order_line f JOIN {gold}.dim_product p ON p.product_sk = f.product_sk
            WHERE f.date_key BETWEEN {month_start} AND {month_end} GROUP BY p.category_name_english""",
    }


def _scans(plan: dict, table: str) -> List[str]:
    found = []
    if plan.get("Relation Name") == table:
        found.append(plan["Node Type"])
    for child in plan.get("Plans", []):
        found += _scans(child, table)
    return found


def _measure(conn, sql: str, repeat: int) -> dict:
    explain = f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}"
    conn.execute(explain)  # warm the cache
    times, scans = [], []
    for _ in range(repeat):
        result = conn.execute(explain).fetchone()[0][0]
        times.append(result["Execution Time"])
        scans = _scans(result["Plan"], "fact_order_line")
    return {"ms": round(statistics.median(times), 3), "fact_scan": ", ".join(sorted(set(scans)))}


def run_sql_benchmark(source: str, repeat: int = 5, settings: Optional[DbSettings] = None) -> List[dict]:
    gold = schema_name(source, "gold")
    index_sql = read_sql("gold_indexes.sql", gold=gold)
    with connect(settings, autocommit=True) as conn:
        customer = conn.execute(
            f"SELECT customer_sk FROM {gold}.fact_order_line GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT 1"
        ).fetchone()[0]
        product = conn.execute(
            f"SELECT product_sk FROM {gold}.fact_order_line GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT 1"
        ).fetchone()[0]
        month = conn.execute(f"SELECT max(date_key) / 100 FROM {gold}.fact_order_line").fetchone()[0]
        queries = _queries(gold, int(customer), int(product), int(month) * 100 + 1, int(month) * 100 + 31)

        for name in INDEX_NAMES.findall(index_sql):
            conn.execute(f"DROP INDEX IF EXISTS {gold}.{name}")
        conn.execute("ANALYZE")
        before = {name: _measure(conn, q, repeat) for name, q in queries.items()}
        conn.execute(index_sql)
        after = {name: _measure(conn, q, repeat) for name, q in queries.items()}

    rows = []
    for name in queries:
        b, a = before[name], after[name]
        rows.append({"query": name, "before_ms": b["ms"], "before_scan": b["fact_scan"],
                     "after_ms": a["ms"], "after_scan": a["fact_scan"],
                     "speedup": round(b["ms"] / a["ms"], 1) if a["ms"] else None})
    return rows
