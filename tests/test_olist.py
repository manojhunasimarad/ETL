import zipfile

from ecom_etl.contracts import TABLES
from ecom_etl.olist import extract_zip, validate_source


def test_extract_zip_keeps_only_contract_files_and_flattens_folders(dirty_source, tmp_path):
    src, _ = dirty_source
    archive = tmp_path / "archive.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        for table in TABLES:
            zf.write(src / table.file_name, f"brazilian-ecommerce/{table.file_name}")
        zf.writestr("olist_geolocation_dataset.csv", "not,needed\n")

    dest = tmp_path / "olist"
    extracted = extract_zip(archive, dest)

    assert extracted == sorted(t.file_name for t in TABLES)
    assert not (dest / "olist_geolocation_dataset.csv").exists()
    assert validate_source(dest) == []
