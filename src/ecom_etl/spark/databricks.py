"""Step 10: the batch pipeline on Databricks, or on any Spark session you already have.

The notebook ``cloud/databricks/ecom_pipeline.py`` installs this package and
calls :func:`run_batch` with the notebook's own ``spark``. Nothing here is
specific to Databricks: the same functions that run on a laptop and in Docker
run there, which is the point of keeping transformations as functions of a
session and a lake location. What differs on Databricks Free Edition:

* compute is serverless and refuses ``persist()``: set ``ECOM_NO_CACHE=1``
  (see ``compat.py``);
* files live in a Unity Catalog **volume**, ``/Volumes/<catalog>/<schema>/<volume>``,
  which Python and Spark both read and write as an ordinary path;
* gold is published as managed tables, ``<catalog>.<schema>.<table>``.

CI runs :func:`run_batch` on local Spark with caching off, so this code path
is tested on every push even though Databricks itself is not.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, Optional

from .gold import GOLD_TABLES
from .lake import Lake


def publish_tables(spark, lake: Lake, schema: str) -> Dict[str, int]:
    """Save every gold table as a managed table ``<schema>.<table>`` and return row counts.

    ``schema`` is ``catalog.schema`` in Unity Catalog (``workspace.ecom``), or a
    database name on plain Spark.
    """
    for table in GOLD_TABLES:
        (spark.read.parquet(lake.path("gold", table))
         .write.mode("overwrite").option("overwriteSchema", "true")
         .saveAsTable(f"{schema}.{table}"))
    return {t: spark.table(f"{schema}.{t}").count() for t in GOLD_TABLES}


def run_batch(spark, root, orders: int = 20_000, seed: int = 42, defect_rate: float = 0.01,
              publish_to: Optional[str] = None) -> dict:
    """Generate synthetic data under ``root``, run bronze → silver → gates → gold → gates,
    and optionally publish gold as tables. Returns what happened, with timings.

    Postgres is not used here (``load=False``, ``save_dq=False``): on Databricks the
    tables themselves are the warehouse.
    """
    from ..generator import GeneratorConfig, generate
    from .pipeline import run_pipeline

    root = Path(root)
    source_dir = root / "raw" / "synthetic"
    t0 = time.perf_counter()
    manifest = generate(GeneratorConfig(orders=orders, seed=seed, defect_rate=defect_rate), source_dir)
    generated = round(time.perf_counter() - t0, 2)
    lake = Lake(root / "lake" / "synthetic")
    report = run_pipeline(spark, source_dir, "synthetic", lake, manifest["extracted_at"],
                          load=False, save_dq=False)
    result = {
        "orders": orders,
        "status": report["status"],
        "run_id": report["run_id"],
        "bronze_records": sum(report["bronze_rows"].values()),
        "gold_rows": report["gold_rows"],
        "checks": report["summary"],
        "evaluation": report.get("evaluation"),
        "seconds": {"generate": generated, **report["seconds"]},
        "lake": str(lake.root),
    }
    if publish_to:
        t1 = time.perf_counter()
        result["published"] = publish_tables(spark, lake, publish_to)
        result["seconds"]["publish"] = round(time.perf_counter() - t1, 2)
    return result
