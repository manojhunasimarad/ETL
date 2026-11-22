from datetime import datetime

import pandas as pd

from ecom_etl.olist import validate_source
from ecom_etl.profiling import (
    ERROR,
    compare_with_manifest,
    parse_typed,
    profile_source,
    render_markdown,
)


def test_profiler_detects_every_injected_defect_exactly(dirty_source):
    out, manifest = dirty_source
    rows = compare_with_manifest(profile_source(out), manifest)
    mismatches = [r for r in rows if not r["match"]]
    assert not mismatches, mismatches


def test_clean_data_has_no_errors(clean_source):
    out, _ = clean_source
    profile = profile_source(out)
    assert profile.errors() == []


def test_legit_nulls_are_not_errors(clean_source):
    """Products without a category and orders without a delivery date are
    allowed by the contract, so they must not show up as errors."""
    out, _ = clean_source
    profile = profile_source(out)
    products = {c["column"]: c for c in profile.tables["products"].columns}
    assert products["product_category_name"]["nulls"] > 0
    assert not [f for f in profile.findings if f.check == "not_null:product_category_name"]


def test_parse_typed_flags_comma_decimals_and_fractional_ints():
    s = pd.Series(["10.50", "129,90", None, "3"])
    _, failed = parse_typed(s, "float")
    assert failed.tolist() == [False, True, False, False]
    _, failed = parse_typed(pd.Series(["1", "2.5", "x"]), "int")
    assert failed.tolist() == [False, True, True]


def test_missing_file_is_reported(tmp_path):
    profile = profile_source(tmp_path, reference_time=datetime(2020, 1, 1))
    assert {f.check for f in profile.findings} == {"file_missing"}
    assert all(f.severity == ERROR for f in profile.findings)


def test_markdown_report_renders(dirty_source):
    out, manifest = dirty_source
    profile = profile_source(out)
    md = render_markdown(profile, compare_with_manifest(profile, manifest))
    assert "## Findings" in md and "## Injected vs detected (14/14" in md


def test_validate_source(dirty_source, tmp_path):
    out, _ = dirty_source
    assert validate_source(out) == []
    assert len(validate_source(tmp_path)) == 7
