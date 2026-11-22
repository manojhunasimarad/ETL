"""Data lake layout (Parquet files on disk, the "medallion" layers).

    data/lake/<source>/bronze/<table>/        raw text + lineage columns
    data/lake/<source>/silver/<table>/        clean, typed, deduplicated
    data/lake/<source>/quarantine/<table>/    rejected rows + reasons
    data/lake/<source>/gold/<table>/          star schema (step 4)
    data/lake/<source>/_reports/              one JSON report per run
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .. import config


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


@dataclass(frozen=True)
class Lake:
    root: Path  # data/lake/<source>

    @classmethod
    def for_source(cls, source: str) -> "Lake":
        return cls(config.DATA_DIR / "lake" / source)

    def path(self, layer: str, table: str) -> str:
        return str(self.root / layer / table)

    def write_report(self, name: str, report: dict) -> Path:
        out = self.root / "_reports" / f"{name}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        return out
