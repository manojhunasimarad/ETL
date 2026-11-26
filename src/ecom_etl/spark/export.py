"""Step 10: write the gold tables in a form a cloud warehouse loads directly.

Gold keeps ``fact_order_line`` partitioned by ``order_month``: the month is a
folder name, not a column inside the Parquet files, so a warehouse reading the
files would lose it. Spark also writes timestamps as INT96 by default, an old
encoding that warehouses read with caveats. The export rewrites every gold
table into ``<lake>/export/snowflake/<table>/`` with every column in the files
and timestamps as TIMESTAMP_MICROS, plus ``_manifest.json`` with each table's
columns and row count, which the Snowflake loader uses (no Spark needed there).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional

from .gold import GOLD_TABLES
from .lake import Lake

TIMESTAMP_CONF = "spark.sql.parquet.outputTimestampType"


def export_dir(lake: Lake) -> Path:
    return lake.root / "export" / "snowflake"


def export_for_snowflake(spark, lake: Lake, out_dir: Optional[Path] = None) -> Dict[str, dict]:
    out = Path(out_dir) if out_dir else export_dir(lake)
    previous = spark.conf.get(TIMESTAMP_CONF, None)
    spark.conf.set(TIMESTAMP_CONF, "TIMESTAMP_MICROS")
    manifest: Dict[str, dict] = {}
    try:
        for table in GOLD_TABLES:
            df = spark.read.parquet(lake.path("gold", table))
            df.write.mode("overwrite").parquet(str(out / table))
            manifest[table] = {"columns": [list(c) for c in df.dtypes], "rows": df.count(),
                               "files": str((out / table).resolve())}
    finally:
        if previous is None:
            spark.conf.unset(TIMESTAMP_CONF)
        else:
            spark.conf.set(TIMESTAMP_CONF, previous)
    out.mkdir(parents=True, exist_ok=True)
    (out / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def read_manifest(out_dir: Path) -> Dict[str, dict]:
    """The export's manifest, with each table's files taken from the folder next to it.

    The manifest records absolute paths. An export made in Docker records ``/app/data/...``,
    which does not exist when ``load-snowflake`` then runs in Windows PowerShell. The table
    folders always sit next to ``_manifest.json``, so use those whenever they exist.
    """
    out_dir = Path(out_dir)
    path = out_dir / "_manifest.json"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found; run `python -m ecom_etl export-snowflake` first")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    for table, info in manifest.items():
        here = out_dir / table
        if here.is_dir():
            info["files"] = str(here.resolve())
    return manifest
