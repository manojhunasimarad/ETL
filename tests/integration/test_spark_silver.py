"""Step 3: bronze and silver, checked against the generator's ground truth."""

import csv
from decimal import Decimal

import pytest

from ecom_etl.contracts import TABLES
from ecom_etl.generator import DEFECTS_FILE

pytestmark = pytest.mark.spark

QUARANTINE_REASON = {
    ("orders", "null_customer_id"): "not_null:customer_id",
    ("orders", "invalid_status"): "accepted_values:order_status",
    ("orders", "delivered_before_purchase"): "sequence:delivered_before_purchase",
    ("orders", "future_purchase"): "future_event",
    ("order_items", "orphan_product"): "fk:product_id->products",
    ("order_items", "non_positive_price"): "range:price",
    ("order_payments", "invalid_payment_type"): "accepted_values:payment_type",
    ("order_payments", "orphan_order"): "fk:order_id->orders",
}
KEY = {
    "orders": lambda r: r["order_id"],
    "order_items": lambda r: f"{r['order_id']}|{r['order_item_id']}",
    "order_payments": lambda r: f"{r['order_id']}|{r['payment_sequential']}",
    "products": lambda r: r["product_id"],
}


def _defects(directory):
    with (directory / DEFECTS_FILE).open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    out = {}
    for r in rows:
        out.setdefault((r["table"], r["defect_type"]), set()).add(r["key"])
    return out


def _build(spark, source, tmp_path_factory, name):
    from ecom_etl.spark.bronze import run_bronze
    from ecom_etl.spark.lake import Lake
    from ecom_etl.spark.silver import run_silver

    out, manifest = source
    lake = Lake(tmp_path_factory.mktemp(name))
    bronze = run_bronze(spark, out, lake, "test-run")
    reports = run_silver(spark, lake, manifest["extracted_at"])
    return out, manifest, lake, bronze, reports


@pytest.fixture(scope="module")
def dirty(spark, dirty_source, tmp_path_factory):
    return _build(spark, dirty_source, tmp_path_factory, "lake_dirty")


@pytest.fixture(scope="module")
def clean(spark, clean_source, tmp_path_factory):
    return _build(spark, clean_source, tmp_path_factory, "lake_clean")


def _rows(spark, lake, layer, table):
    from ecom_etl.spark.silver import TABLES_BY_NAME, quarantine_schema, read_layer

    schema = quarantine_schema(TABLES_BY_NAME[table]) if layer == "quarantine" else None
    return read_layer(spark, lake, layer, table, schema).collect()


def test_bronze_keeps_every_row(dirty):
    _, manifest, _, bronze, _ = dirty
    assert bronze == manifest["row_counts"]


def test_every_row_is_accounted_for(dirty):
    _, _, _, _, reports = dirty
    for name, report in reports.items():
        assert report.balanced(), report


def test_exact_duplicates_match_ground_truth(dirty):
    _, manifest, _, _, reports = dirty
    injected = {d["table"]: d["count"] for d in manifest["defects"] if d["defect_type"] == "duplicate_row"}
    for table in TABLES:
        assert reports[table.name].exact_duplicates == injected.get(table.name, 0), table.name


@pytest.mark.parametrize("table", ["orders", "order_items", "order_payments"])
def test_quarantine_matches_ground_truth_exactly(spark, dirty, table):
    """Recall: every injected defect is quarantined with the right reason.
    Precision: nothing else is quarantined, apart from children of rejected parents."""
    out, _, lake, _, _ = dirty
    truth = _defects(out)
    quarantined = {KEY[table](r): set(r["_reasons"]) for r in _rows(spark, lake, "quarantine", table)}

    expected_keys = set()
    for (t, defect), reason in QUARANTINE_REASON.items():
        if t != table:
            continue
        for key in truth.get((t, defect), set()):
            assert reason in quarantined.get(key, set()), (defect, key)
        expected_keys |= truth.get((t, defect), set())

    primary = {k for k, reasons in quarantined.items()
               if any(not r.startswith("parent_quarantined:") for r in reasons)}
    assert primary == expected_keys


def test_fixed_rows_are_kept_and_tagged(spark, dirty):
    out, _, lake, _, _ = dirty
    truth = _defects(out)

    orders = {r["order_id"]: r for r in _rows(spark, lake, "silver", "orders")}
    for key in truth[("orders", "status_unnormalized")]:
        assert "normalized:order_status" in orders[key]["_fixes"]

    items = {KEY["order_items"](r): r for r in _rows(spark, lake, "silver", "order_items")}
    comma = [k for k in truth[("order_items", "price_comma_decimal")] if k.split("|")[0] in orders]
    assert comma
    for key in comma:
        assert "decimal_comma:price" in items[key]["_fixes"]
        assert isinstance(items[key]["price"], Decimal) and items[key]["price"] > 0

    products = {r["product_id"]: r for r in _rows(spark, lake, "silver", "products")}
    for key in truth[("products", "category_missing_translation")]:
        assert "added_translation:product_category_name" in products[key]["_fixes"]
    translations = {r["product_category_name"]: r["is_translated"]
                    for r in _rows(spark, lake, "silver", "category_translation")}
    assert all(translations[products[k]["product_category_name"]] is False
               for k in truth[("products", "category_missing_translation")])


def test_silver_is_typed(spark, dirty):
    from pyspark.sql import types as T

    _, _, lake, _, _ = dirty
    items = spark.read.parquet(lake.path("silver", "order_items")).schema
    orders = spark.read.parquet(lake.path("silver", "orders")).schema
    assert items["price"].dataType == T.DecimalType(12, 2)
    assert items["order_item_id"].dataType == T.IntegerType()
    assert orders["order_purchase_timestamp"].dataType == T.TimestampType()


def test_clean_data_has_no_quarantine(clean):
    _, manifest, _, bronze, reports = clean
    for name, report in reports.items():
        assert report.quarantined == 0 and report.exact_duplicates == 0, report
        assert report.silver_rows == bronze[name]
