"""Step 5: the check registry (no Spark needed)."""

from ecom_etl.spark.quality import all_checks, row_level_checks, table_level_checks


def test_check_names_are_unique():
    names = [c.name for c in all_checks()]
    assert len(names) == len(set(names))


def test_only_table_level_checks_can_block():
    assert not any(c.blocking for c in row_level_checks())
    assert any(c.blocking for c in table_level_checks())


def test_every_check_has_a_valid_severity_and_layer():
    for c in all_checks():
        assert c.severity in ("error", "warn")
        assert c.layer in ("silver", "gold")
        assert c.kind in ("row", "table")
