"""Project paths. Everything is relative to the repository root, so the
same commands work on Windows, WSL, Linux and inside Docker.

Set ECOM_ETL_DATA_DIR to keep large data outside the repo.
"""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = Path(os.environ.get("ECOM_ETL_DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"
REPORTS_DIR = PROJECT_ROOT / "reports"

KNOWN_SOURCES = ("synthetic", "olist")


def source_dir(source: str) -> Path:
    """Map a source name ("synthetic", "olist") or a path to a directory."""
    if source in KNOWN_SOURCES:
        return RAW_DIR / source
    return Path(source)
