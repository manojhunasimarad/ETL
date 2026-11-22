import csv

import pytest

from ecom_etl.contracts import TABLES
from ecom_etl.generator import DEFECTS_FILE, GeneratorConfig, generate
from ecom_etl.profiling import read_raw


def test_same_seed_gives_identical_files(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    generate(GeneratorConfig(orders=500, seed=1), a)
    generate(GeneratorConfig(orders=500, seed=1), b)
    for table in TABLES:
        assert (a / table.file_name).read_bytes() == (b / table.file_name).read_bytes(), table.name


def test_headers_match_the_contract(dirty_source):
    out, _ = dirty_source
    for table in TABLES:
        with (out / table.file_name).open(encoding="utf-8") as f:
            assert next(csv.reader(f)) == table.column_names


def test_row_counts_in_manifest_match_files(dirty_source):
    out, manifest = dirty_source
    for table in TABLES:
        assert len(read_raw(out / table.file_name)) == manifest["row_counts"][table.name]


def test_every_defect_type_is_injected(dirty_source):
    _, manifest = dirty_source
    missing = [d["defect_type"] for d in manifest["defects"] if d["count"] == 0]
    assert not missing


def test_defect_log_matches_manifest(dirty_source):
    out, manifest = dirty_source
    with (out / DEFECTS_FILE).open(encoding="utf-8") as f:
        logged = list(csv.DictReader(f))
    assert len(logged) == manifest["total_defects"]


def test_clean_rows_are_identical_whatever_the_defect_rate(dirty_source, clean_source):
    """Defects use their own random generator, so the sellers (which never get
    defects) must be byte-identical between a clean and a dirty run."""
    dirty, _ = dirty_source
    clean, _ = clean_source
    assert (dirty / "olist_sellers_dataset.csv").read_bytes() == (clean / "olist_sellers_dataset.csv").read_bytes()


def test_clean_payments_reconcile_with_items(clean_source):
    out, _ = clean_source
    items = read_raw(out / "olist_order_items_dataset.csv")
    pays = read_raw(out / "olist_order_payments_dataset.csv")
    item_total = (items["price"].astype(float) + items["freight_value"].astype(float)).groupby(items["order_id"]).sum()
    pay_total = pays["payment_value"].astype(float).groupby(pays["order_id"]).sum()
    both = item_total.index.intersection(pay_total.index)
    assert len(both) > 0
    assert ((pay_total[both] - item_total[both]).abs() < 0.011).all()


def test_no_clean_event_happens_after_the_extract(clean_source):
    out, manifest = clean_source
    orders = read_raw(out / "olist_orders_dataset.csv")
    cols = ["order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date"]
    latest = max(orders[c].dropna().max() for c in cols)
    assert latest < manifest["extracted_at"]


def test_rejects_bad_config(tmp_path):
    with pytest.raises(ValueError):
        generate(GeneratorConfig(orders=0), tmp_path)
    with pytest.raises(ValueError):
        generate(GeneratorConfig(orders=10, defect_rate=0.5), tmp_path)
