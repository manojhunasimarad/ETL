"""Get the real Olist dataset from Kaggle and check it matches the contract.

Dataset: "Brazilian E-Commerce Public Dataset by Olist"
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
License: CC BY-NC-SA 4.0. The files are downloaded into data/raw/olist,
which is git-ignored: we never commit the data itself, only the code.

Two ways in:
1. Kaggle CLI (needs ~/.kaggle/kaggle.json or KAGGLE_USERNAME/KAGGLE_KEY).
2. Manual: download the zip in the browser and pass ``--zip path/to/archive.zip``.
"""

from __future__ import annotations

import shutil
import subprocess
import zipfile
from pathlib import Path
from typing import List, Optional

from .contracts import TABLES
from .profiling import read_raw

OLIST_DATASET = "olistbr/brazilian-ecommerce"
OLIST_URL = f"https://www.kaggle.com/datasets/{OLIST_DATASET}"

MANUAL_STEPS = f"""Could not download automatically. Do this instead:
  1. Open {OLIST_URL} and click Download (you need a free Kaggle account).
  2. Run:  python -m ecom_etl download-olist --zip <path to the downloaded zip>
Or set up the Kaggle CLI: pip install kaggle, then put kaggle.json from
https://www.kaggle.com/settings (API > Create New Token) in ~/.kaggle/."""


def extract_zip(zip_path: Path, dest: Path) -> List[str]:
    """Extract only the CSV files we need, flattening any folders inside the zip."""
    wanted = {t.file_name for t in TABLES}
    dest.mkdir(parents=True, exist_ok=True)
    extracted = []
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            name = Path(member).name
            if name in wanted:
                with zf.open(member) as src, (dest / name).open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                extracted.append(name)
    return sorted(extracted)


def download_olist(dest: Path, zip_path: Optional[Path] = None) -> List[str]:
    if zip_path:
        return extract_zip(Path(zip_path), dest)
    if shutil.which("kaggle") is None:
        raise RuntimeError(MANUAL_STEPS)
    dest.mkdir(parents=True, exist_ok=True)
    cmd = ["kaggle", "datasets", "download", "-d", OLIST_DATASET, "-p", str(dest), "--force"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"kaggle CLI failed:\n{result.stderr.strip()}\n\n{MANUAL_STEPS}")
    zips = sorted(dest.glob("*.zip"))
    if not zips:
        raise RuntimeError(f"kaggle CLI finished but no zip found in {dest}\n\n{MANUAL_STEPS}")
    return extract_zip(zips[-1], dest)


def validate_source(directory: Path) -> List[str]:
    """Return a list of problems (empty list = files and headers match the contract)."""
    problems = []
    for table in TABLES:
        path = Path(directory) / table.file_name
        if not path.exists():
            problems.append(f"missing file: {table.file_name}")
            continue
        header = list(read_raw(path).columns) if path.stat().st_size else []
        missing = [c for c in table.column_names if c not in header]
        if missing:
            problems.append(f"{table.file_name}: missing columns {missing}")
    return problems
