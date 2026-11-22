"""Bronze: the raw files as Parquet, every value still text, plus lineage.

Why bronze exists when the CSV files are already there:
* Parquet is columnar and compressed: later jobs read only the columns
  they need, many times faster than re-parsing CSV.
* An explicit all-string schema means nothing is guessed. ``inferSchema``
  would turn zip "01037" into 1037 and silently null "129,90".
* Lineage columns say where every row came from and in which run.

Full-refresh semantics: each run replaces bronze with the latest snapshot.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from ..contracts import TABLES, Table
from .lake import Lake

LINEAGE_COLUMNS = ("_row_id", "_source_file", "_ingested_at", "_run_id")


def bronze_schema(table: Table):
    from pyspark.sql import types as T

    return T.StructType([T.StructField(c, T.StringType(), True) for c in table.column_names])


def read_csv(spark, source_dir: Path, table: Table, run_id: str):
    from pyspark.sql import functions as F

    df = (spark.read
          .schema(bronze_schema(table))
          .option("header", True)
          .option("escape", '"')       # RFC 4180: a quote inside quotes is written ""
          .option("mode", "PERMISSIVE")
          .csv(str(Path(source_dir) / table.file_name)))
    return df.select(
        *table.column_names,
        # increasing in file order: used later to keep the *first* of duplicates
        F.monotonically_increasing_id().alias("_row_id"),
        F.col("_metadata.file_name").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(run_id).alias("_run_id"),
    )


def run_bronze(spark, source_dir: Path, lake: Lake, run_id: str) -> Dict[str, int]:
    """Write every contract table to bronze. Returns row counts."""
    counts = {}
    for table in TABLES:
        path = lake.path("bronze", table.name)
        read_csv(spark, source_dir, table, run_id).write.mode("overwrite").parquet(path)
        counts[table.name] = spark.read.parquet(path).count()
    return counts
