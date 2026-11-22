"""Step 2: land the CSV files in Postgres, then build a normalized core schema.

Two layers, two schemas per source (``synthetic_raw``, ``synthetic_core``...):

``<source>_raw``
    An untouched copy of each CSV file: every column TEXT, no constraints,
    plus ``_raw_id`` (load order), ``_source_file`` and ``_loaded_at``.
    Loading never fails because of bad data, so nothing is lost.

``<source>_core``
    Typed, normalized tables with primary keys, foreign keys and CHECK
    constraints, built from raw by ``sql/core.sql`` (ELT: transform inside
    the database with SQL).

Loading uses ``COPY ... FROM STDIN`` streamed in 1 MB chunks: memory use
stays flat whatever the file size, and it is far faster than row-by-row
INSERTs or ``pandas.to_sql``.
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from .contracts import TABLES, TIMESTAMP_FORMAT, Table
from .db import DbSettings, connect, read_sql, schema_name

CHUNK_BYTES = 1 << 20


def raw_table_ddl(schema: str, table: Table, source_file: str) -> str:
    columns = ",\n    ".join(f'"{c}" TEXT' for c in table.column_names)
    return (
        f"CREATE TABLE {schema}.{table.name} (\n"
        f"    _raw_id BIGSERIAL PRIMARY KEY,\n"
        f"    {columns},\n"
        f"    _source_file TEXT NOT NULL DEFAULT '{source_file}',\n"
        f"    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()\n)"
    )


def count_csv_records(path: Path) -> int:
    """Data rows in a CSV file (quoted newlines counted correctly)."""
    with path.open(newline="", encoding="utf-8-sig") as f:
        return max(sum(1 for _ in csv.reader(f)) - 1, 0)


def load_raw(source_dir: Path, source: str, settings: Optional[DbSettings] = None) -> Dict[str, int]:
    """Copy every contract CSV in ``source_dir`` into ``<source>_raw``. Returns row counts."""
    schema = schema_name(source, "raw")
    counts: Dict[str, int] = {}
    with connect(settings) as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for table in TABLES:
            path = Path(source_dir) / table.file_name
            if not path.exists():
                raise FileNotFoundError(f"{path} not found")
            conn.execute(f"DROP TABLE IF EXISTS {schema}.{table.name}")
            conn.execute(raw_table_ddl(schema, table, path.name))
            column_list = ", ".join(f'"{c}"' for c in table.column_names)
            with conn.cursor() as cur:
                with cur.copy(f"COPY {schema}.{table.name} ({column_list}) "
                              f"FROM STDIN WITH (FORMAT csv, HEADER true)") as copy:
                    with path.open("rb") as f:
                        while chunk := f.read(CHUNK_BYTES):
                            copy.write(chunk)
            counts[table.name] = conn.execute(f"SELECT count(*) FROM {schema}.{table.name}").fetchone()[0]
        conn.commit()
    return counts


def build_core(source: str, extracted_at: Optional[datetime] = None,
               settings: Optional[DbSettings] = None) -> Dict[str, Dict[str, int]]:
    """Run ``sql/core.sql`` for a source. Returns raw vs core row counts per table."""
    raw, core = schema_name(source, "raw"), schema_name(source, "core")
    cutoff = (extracted_at or datetime.now()).strftime(TIMESTAMP_FORMAT)
    sql = read_sql("core.sql", raw=raw, core=core, extracted_at=cutoff)
    result: Dict[str, Dict[str, int]] = {}
    with connect(settings) as conn:
        conn.execute(sql)
        for table in TABLES:
            raw_rows = conn.execute(f"SELECT count(*) FROM {raw}.{table.name}").fetchone()[0]
            core_rows = conn.execute(f"SELECT count(*) FROM {core}.{table.name}").fetchone()[0]
            result[table.name] = {"raw": raw_rows, "core": core_rows}
        conn.commit()
    return result
