"""Step 2 integration tests: need a running Postgres 16+ (see docker-compose.yml)."""

import csv
from datetime import datetime

import pytest

from ecom_etl.contracts import TABLES, TIMESTAMP_FORMAT
from ecom_etl.db import connect, schema_name
from ecom_etl.generator import DEFECTS_FILE
from ecom_etl.landing import build_core, count_csv_records, load_raw

pytestmark = pytest.mark.postgres


def _defect_keys(directory, table, defect_type):
    with (directory / DEFECTS_FILE).open(encoding="utf-8") as f:
        return [r["key"] for r in csv.DictReader(f) if r["table"] == table and r["defect_type"] == defect_type]


def _extracted_at(manifest):
    return datetime.strptime(manifest["extracted_at"], TIMESTAMP_FORMAT)


@pytest.fixture(scope="module")
def dirty_db(dirty_source):
    out, manifest = dirty_source
    raw_counts = load_raw(out, "t_dirty")
    core_counts = build_core("t_dirty", _extracted_at(manifest))
    return out, manifest, raw_counts, core_counts


@pytest.fixture(scope="module")
def clean_db(clean_source):
    out, manifest = clean_source
    load_raw(out, "t_clean")
    return build_core("t_clean", _extracted_at(manifest))


def _present(sql, keys):
    with connect() as conn:
        return {r[0] for r in conn.execute(sql, (keys,)).fetchall()}


def test_raw_is_an_exact_copy_of_the_files(dirty_db):
    out, manifest, raw_counts, _ = dirty_db
    for table in TABLES:
        assert raw_counts[table.name] == manifest["row_counts"][table.name]
        assert raw_counts[table.name] == count_csv_records(out / table.file_name)


def test_clean_data_loses_no_rows(clean_db):
    for table, counts in clean_db.items():
        assert counts["core"] == counts["raw"], table


def test_rejected_orders(dirty_db):
    out, *_ = dirty_db
    core = schema_name("t_dirty", "core")
    for defect in ("null_customer_id", "invalid_status", "delivered_before_purchase", "future_purchase"):
        keys = _defect_keys(out, "orders", defect)
        assert keys, defect
        assert not _present(f"SELECT order_id FROM {core}.orders WHERE order_id = ANY(%s)", keys), defect


def test_fixed_order_status_is_kept_and_normalized(dirty_db):
    out, *_ = dirty_db
    core = schema_name("t_dirty", "core")
    keys = _defect_keys(out, "orders", "status_unnormalized")
    with connect() as conn:
        rows = conn.execute(f"SELECT order_status FROM {core}.orders WHERE order_id = ANY(%s)", (keys,)).fetchall()
    assert len(rows) == len(keys)
    assert all(r[0] == r[0].strip().lower() for r in rows)


def test_order_lines(dirty_db):
    out, *_ = dirty_db
    core = schema_name("t_dirty", "core")
    line_key = f"SELECT order_id || '|' || order_item_id FROM {core}.order_items WHERE order_id || '|' || order_item_id = ANY(%s)"
    for defect in ("orphan_product", "non_positive_price"):
        assert not _present(line_key, _defect_keys(out, "order_items", defect)), defect

    # comma decimals are fixed, so those lines survive unless their order was rejected
    comma = _defect_keys(out, "order_items", "price_comma_decimal")
    kept_orders = _present(f"SELECT order_id FROM {core}.orders WHERE order_id = ANY(%s)", [k.split("|")[0] for k in comma])
    expected = {k for k in comma if k.split("|")[0] in kept_orders}
    assert expected and _present(line_key, comma) == expected


def test_payments(dirty_db):
    out, *_ = dirty_db
    core = schema_name("t_dirty", "core")
    pay_key = f"SELECT order_id || '|' || payment_sequential FROM {core}.order_payments WHERE order_id || '|' || payment_sequential = ANY(%s)"
    for defect in ("invalid_payment_type", "orphan_order"):
        assert not _present(pay_key, _defect_keys(out, "order_payments", defect)), defect


def test_untranslated_categories_are_added_not_rejected(dirty_db):
    out, *_ = dirty_db
    core = schema_name("t_dirty", "core")
    keys = _defect_keys(out, "products", "category_missing_translation")
    with connect() as conn:
        rows = conn.execute(
            f"""SELECT t.is_translated FROM {core}.products p
                JOIN {core}.category_translation t ON t.product_category_name = p.category_name
                WHERE p.product_id = ANY(%s)""", (keys,)).fetchall()
    assert len(rows) == len(keys) and not any(r[0] for r in rows)


def test_duplicates_are_removed(dirty_db):
    _, manifest, _, core_counts = dirty_db
    dups = {d["table"]: d["count"] for d in manifest["defects"] if d["defect_type"] == "duplicate_row"}
    assert core_counts["customers"]["core"] == core_counts["customers"]["raw"] - dups["customers"]


def test_build_core_is_idempotent(dirty_db):
    _, manifest, _, first = dirty_db
    assert build_core("t_dirty", _extracted_at(manifest)) == first
