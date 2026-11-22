"""Tests for the YOUR TURN exercises.

Each test skips (it does not fail) until its function is implemented, so
CI stays green. Run ``pytest -rs`` to see which ones are still waiting.
"""

import pytest

from ecom_etl.contracts import CUSTOMERS, ORDERS
from ecom_etl.generator import GeneratorConfig, generate
from ecom_etl.profiling import customers_without_orders, profile_source, read_raw


def _call_or_skip(fn, *args):
    try:
        return fn(*args)
    except NotImplementedError as exc:
        pytest.skip(str(exc))


def _customers_and_orders(directory):
    return read_raw(directory / CUSTOMERS.file_name), read_raw(directory / ORDERS.file_name)


@pytest.fixture(scope="module")
def tricky_source(tmp_path_factory):
    """With this seed, one duplicated customer row belongs to an order whose
    customer_id was blanked, so counting rows instead of distinct ids is off by one."""
    out = tmp_path_factory.mktemp("tricky")
    manifest = generate(GeneratorConfig(orders=3000, seed=11, defect_rate=0.03), out)
    expected = next(d["count"] for d in manifest["defects"] if d["defect_type"] == "null_customer_id")
    return out, expected


# --- Step 1 ------------------------------------------------------------------

def test_step1_customers_without_orders_is_zero_on_clean_data(clean_source):
    out, _ = clean_source
    assert _call_or_skip(customers_without_orders, *_customers_and_orders(out)) == 0


def test_step1_customers_without_orders_counts_distinct_ids(tricky_source):
    out, expected = tricky_source
    got = _call_or_skip(customers_without_orders, *_customers_and_orders(out))
    assert got == expected, (
        f"expected {expected}, got {got}. "
        + ("Did you count rows instead of distinct customer_id values?" if got == expected + 1 else "")
    )


def test_step1_finding_appears_in_profile(tricky_source):
    out, expected = tricky_source
    _call_or_skip(customers_without_orders, *_customers_and_orders(out))
    assert profile_source(out).count("customers", "customers_without_orders") == expected
