"""Silver: standardize, deduplicate, type, validate and quarantine.

For every table, in this order:

1. **Standardize** (the "fix" rules from step 1): trim, empty string to
   null, lower-case statuses and payment types, decimal comma to point,
   zero-pad zip codes. Every fix applied to a row is recorded in ``_fixes``.
2. **Exact duplicates** are dropped (``dropDuplicates`` on all business
   columns) and counted.
3. **Type**: safe casts (``try_cast``, ``try_to_timestamp``) that give NULL
   instead of failing. Spark 4 runs in ANSI mode, where a plain ``cast``
   of "abc" to a number raises an error and kills the job.
4. **Validate**: each rule produces a reason code. Rules come from the
   contract (not_null, type, accepted_values, range), from the business
   (delivered before purchase, events after the extract), from keys
   (``duplicate_key``) and from foreign keys (``fk:...`` for orphans,
   ``parent_quarantined:...`` when the parent row itself was rejected).
5. **Split**: rows with no reason go to silver (typed), rows with at least
   one reason go to quarantine (original text + ``_reasons``). Nothing is
   silently dropped: bronze = duplicates + quarantine + silver.

Order-timestamp anomalies that are common in real Olist data (carrier
pickup before approval) are kept but flagged in ``_warnings``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..contracts import (
    FLOAT,
    INT,
    ORDER_TIMESTAMP_SEQUENCE,
    STRING,
    TABLES_BY_NAME,
    TIMESTAMP,
    Table,
)
from .bronze import LINEAGE_COLUMNS
from .lake import Lake

SPARK_TS_PATTERN = "yyyy-MM-dd HH:mm:ss"
ZIP_COLUMNS = {"customer_zip_code_prefix", "seller_zip_code_prefix"}
LOWER_COLUMNS = {"customer_city", "seller_city"}
UPPER_COLUMNS = {"customer_state", "seller_state"}
# products -> category_translation is handled by a fix, not a rejection
FIX_INSTEAD_OF_FK = {("products", "product_category_name")}
HARD_SEQUENCE = ("order_delivered_customer_date", "order_purchase_timestamp")
# dependency order: parents before children
SILVER_ORDER = ("customers", "sellers", "category_translation", "products",
                "orders", "order_items", "order_payments")


@dataclass
class TableReport:
    table: str
    bronze_rows: int = 0
    exact_duplicates: int = 0
    quarantined: int = 0
    silver_rows: int = 0
    reasons: Dict[str, int] = field(default_factory=dict)
    fixes: Dict[str, int] = field(default_factory=dict)
    warnings: Dict[str, int] = field(default_factory=dict)

    def balanced(self) -> bool:
        return self.bronze_rows == self.exact_duplicates + self.quarantined + self.silver_rows


def _bt(name: str) -> str:
    return f"`{name}`"


def typed_column(name: str, dtype: str):
    """Safe cast of a text column to its contract type (NULL when invalid)."""
    from pyspark.sql import functions as F

    if dtype == INT:
        d = F.expr(f"try_cast({_bt(name)} AS DOUBLE)")
        return F.when(F.abs(d) < 2147483647, F.when(d == F.floor(d), d.cast("int")))
    if dtype == FLOAT:  # every FLOAT in the contract is money
        return F.expr(f"try_cast({_bt(name)} AS DECIMAL(12,2))")
    if dtype == TIMESTAMP:
        return F.expr(f"try_to_timestamp({_bt(name)}, '{SPARK_TS_PATTERN}')")
    return F.col(name)


def standardize(df, table: Table):
    """Apply the fix rules; record which fixes happened in ``_fixes``."""
    from pyspark.sql import functions as F

    fixes = []
    for col in table.columns:
        raw = F.col(col.name)
        clean = F.trim(raw)
        clean = F.when(clean == "", None).otherwise(clean)
        if col.accepted_values:
            fixes.append(F.when(~raw.isin(*col.accepted_values) & F.lower(clean).isin(*col.accepted_values),
                                F.lit(f"normalized:{col.name}")))
            clean = F.lower(clean)
        if col.dtype == FLOAT:
            fixes.append(F.when(clean.contains(","), F.lit(f"decimal_comma:{col.name}")))
            clean = F.regexp_replace(clean, ",", ".")
        if col.name in ZIP_COLUMNS:
            fixes.append(F.when(F.length(clean) < 5, F.lit(f"zero_padded:{col.name}")))
            clean = F.when(F.length(clean) < 5, F.lpad(clean, 5, "0")).otherwise(clean)
        if col.name in LOWER_COLUMNS:
            clean = F.lower(clean)
        if col.name in UPPER_COLUMNS:
            clean = F.upper(clean)
        df = df.withColumn(col.name, clean)
    return df.withColumn("_fixes", F.array_compact(F.array(*fixes)) if fixes
                         else F.array().cast("array<string>"))


def contract_reasons(table: Table) -> list:
    from pyspark.sql import functions as F

    reasons = []
    for col in table.columns:
        raw, typed = F.col(col.name), F.col(f"__t_{col.name}")
        if not col.nullable:
            reasons.append(F.when(raw.isNull(), F.lit(f"not_null:{col.name}")))
        if col.dtype != STRING:
            reasons.append(F.when(raw.isNotNull() & typed.isNull(), F.lit(f"type:{col.name}")))
        if col.accepted_values:
            reasons.append(F.when(raw.isNotNull() & ~raw.isin(*col.accepted_values),
                                  F.lit(f"accepted_values:{col.name}")))
        if col.min_value is not None and col.dtype in (INT, FLOAT):
            bad = typed < F.lit(col.min_value) if col.min_inclusive else typed <= F.lit(col.min_value)
            reasons.append(F.when(bad, F.lit(f"range:{col.name}")))
    return reasons


def order_rules(extracted_at: str) -> Tuple[list, list]:
    """Business rules for orders: (reasons that quarantine, warnings that only flag)."""
    from pyspark.sql import functions as F

    t = lambda c: F.col(f"__t_{c}")  # noqa: E731
    events = [c.name for c in TABLES_BY_NAME["orders"].columns if c.event_time]
    later, earlier = HARD_SEQUENCE
    reasons = [
        F.when(t(later) < t(earlier), F.lit("sequence:delivered_before_purchase")),
        F.when(F.greatest(*[t(c) for c in events]) > F.to_timestamp(F.lit(extracted_at)),
               F.lit("future_event")),
    ]
    warnings = [F.when(t(a) < t(b), F.lit(f"sequence:{a}<{b}"))
                for a, b in ORDER_TIMESTAMP_SEQUENCE if (a, b) != HARD_SEQUENCE]
    return reasons, warnings


def _has_parquet(path: str) -> bool:
    return any(Path(path).rglob("*.parquet"))


def read_layer(spark, lake: Lake, layer: str, table: str, schema=None):
    """Read a lake table; an empty quarantine (no files) gives an empty DataFrame."""
    path = lake.path(layer, table)
    if not _has_parquet(path):
        if schema is None:
            raise FileNotFoundError(f"{path} has no parquet files")
        return spark.createDataFrame([], schema)
    return spark.read.parquet(path)


def quarantine_schema(table: Table):
    from pyspark.sql import types as T

    fields = [T.StructField(c, T.StringType()) for c in table.column_names]
    fields += [T.StructField("_reasons", T.ArrayType(T.StringType())),
               T.StructField("_fixes", T.ArrayType(T.StringType())),
               T.StructField("_row_id", T.LongType()),
               T.StructField("_source_file", T.StringType()),
               T.StructField("_ingested_at", T.TimestampType()),
               T.StructField("_run_id", T.StringType())]
    return T.StructType(fields)


def foreign_key_checks(spark, lake: Lake, table: Table, df):
    """Left-join each parent's valid and quarantined keys; return (df, reasons)."""
    from pyspark.sql import functions as F

    reasons = []
    for i, fk in enumerate(table.foreign_keys):
        col, ref, parent = fk.columns[0], fk.ref_columns[0], fk.ref_table
        if (table.name, col) in FIX_INSTEAD_OF_FK:
            continue
        ok = (read_layer(spark, lake, "silver", parent).select(F.col(ref).alias(f"__fk{i}"))
              .distinct().withColumn(f"__fk{i}_ok", F.lit(True)))
        rejected = (read_layer(spark, lake, "quarantine", parent, quarantine_schema(TABLES_BY_NAME[parent]))
                    .select(F.col(ref).alias(f"__fkq{i}")).distinct().withColumn(f"__fk{i}_q", F.lit(True)))
        df = (df.join(ok, df[col] == ok[f"__fk{i}"], "left")
                .join(rejected, df[col] == rejected[f"__fkq{i}"], "left"))
        missing = F.col(col).isNotNull() & F.col(f"__fk{i}_ok").isNull()
        reasons.append(F.when(missing & F.col(f"__fk{i}_q"), F.lit(f"parent_quarantined:{parent}")))
        reasons.append(F.when(missing & F.col(f"__fk{i}_q").isNull(), F.lit(f"fk:{col}->{parent}")))
    return df, reasons


def _tally(df, column: str) -> Dict[str, int]:
    from pyspark.sql import functions as F

    rows = df.select(F.explode(column).alias("v")).groupBy("v").count().collect()
    return {r["v"]: r["count"] for r in sorted(rows, key=lambda r: r["v"])}


def validate_table(spark, lake: Lake, table: Table, extracted_at: str, extra_fixes=None):
    """Return (checked DataFrame with _reasons/_warnings, report). The DataFrame is persisted."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    report = TableReport(table.name)
    bronze = read_layer(spark, lake, "bronze", table.name)
    report.bronze_rows = bronze.count()

    std = standardize(bronze, table)
    if extra_fixes is not None:  # (column expression giving a fix label or NULL, joined DataFrame)
        std = extra_fixes(std)
    deduped = std.dropDuplicates(table.column_names).persist()
    report.exact_duplicates = report.bronze_rows - deduped.count()

    df = deduped.select("*", *[typed_column(c.name, c.dtype).alias(f"__t_{c.name}") for c in table.columns])
    reasons = contract_reasons(table)
    warnings: List = []
    if table.name == "orders":
        r, w = order_rules(extracted_at)
        reasons += r
        warnings += w

    key_window = Window.partitionBy(*table.primary_key).orderBy("_row_id")
    df = df.withColumn("__rn", F.row_number().over(key_window))
    reasons.append(F.when(F.col("__rn") > 1, F.lit("duplicate_key")))

    df, fk_reasons = foreign_key_checks(spark, lake, table, df)
    reasons += fk_reasons

    checked = (df.withColumn("_reasons", F.array_compact(F.array(*reasons)))
                 .withColumn("_warnings", F.array_compact(F.array(*warnings)) if warnings
                             else F.array().cast("array<string>"))
                 .persist())
    deduped.unpersist()
    return checked, report


def split_and_write(lake: Lake, table: Table, checked, report: TableReport, extra_silver=None) -> None:
    from pyspark.sql import functions as F

    good = checked.filter(F.size("_reasons") == 0)
    bad = checked.filter(F.size("_reasons") > 0)
    silver = good.select(*[F.col(f"__t_{c.name}").alias(c.name) for c in table.columns],
                         "_fixes", "_warnings", *LINEAGE_COLUMNS)
    if extra_silver is not None:
        silver = extra_silver(silver)
    quarantine = bad.select(*table.column_names, "_reasons", "_fixes", *LINEAGE_COLUMNS)

    if table.name == "orders":  # partitioned by month: queries on a date range read fewer files
        (silver.withColumn("_purchase_month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
         .write.mode("overwrite").partitionBy("_purchase_month").parquet(lake.path("silver", table.name)))
    else:
        silver.write.mode("overwrite").parquet(lake.path("silver", table.name))
    quarantine.write.mode("overwrite").parquet(lake.path("quarantine", table.name))

    report.silver_rows = good.count()
    report.quarantined = bad.count()
    report.reasons = _tally(bad, "_reasons")
    report.fixes = _tally(good, "_fixes")
    report.warnings = _tally(good, "_warnings")
    checked.unpersist()


def run_silver(spark, lake: Lake, extracted_at: str) -> Dict[str, TableReport]:
    """Build every silver and quarantine table. ``extracted_at``: 'yyyy-MM-dd HH:mm:ss'."""
    from pyspark.sql import functions as F

    reports: Dict[str, TableReport] = {}

    def process(name: str, extra_fixes=None, extra_silver=None) -> None:
        table = TABLES_BY_NAME[name]
        checked, report = validate_table(spark, lake, table, extracted_at, extra_fixes)
        split_and_write(lake, table, checked, report, extra_silver)
        reports[name] = report

    process("customers")
    process("sellers")

    # Categories used by products but missing from the translation table are
    # added (english name = portuguese name, is_translated = false), and the
    # products that use them are tagged with a fix.
    translations = (read_layer(spark, lake, "bronze", "category_translation")
                    .select(F.trim("product_category_name").alias("c")).dropna().distinct())
    used = (read_layer(spark, lake, "bronze", "products")
            .select(F.trim("product_category_name").alias("c"))
            .filter(F.col("c").isNotNull() & (F.col("c") != "")).distinct())
    missing = used.join(translations, "c", "left_anti").persist()

    def add_missing_categories(silver):
        added = missing.select(
            F.col("c").alias("product_category_name"),
            F.col("c").alias("product_category_name_english"),
            F.array(F.lit("added_translation:product_category_name")).alias("_fixes"),
            F.array().cast("array<string>").alias("_warnings"),
            F.lit(None).cast("long").alias("_row_id"),
            F.lit("derived:products").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
            F.lit(None).cast("string").alias("_run_id"))
        return (silver.withColumn("is_translated", F.lit(True))
                .unionByName(added.withColumn("is_translated", F.lit(False))))

    process("category_translation", extra_silver=add_missing_categories)
    reports["category_translation"].fixes["added_translation:product_category_name"] = missing.count()

    def tag_untranslated(std):
        flagged = missing.select(F.col("c").alias("__missing_cat"))
        joined = std.join(flagged, std["product_category_name"] == flagged["__missing_cat"], "left")
        tag = F.when(F.col("__missing_cat").isNotNull(), F.lit("added_translation:product_category_name"))
        return joined.withColumn("_fixes", F.array_compact(F.concat("_fixes", F.array(tag)))).drop("__missing_cat")

    process("products", extra_fixes=tag_untranslated)
    missing.unpersist()

    for name in ("orders", "order_items", "order_payments"):
        process(name)
    return reports
