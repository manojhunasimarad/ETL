"""Step 10: Snowflake SQL generation and the load, against a fake connection.

No Snowflake account is needed: the SQL is built by plain functions and the
loader takes any DB-API connection. What a real account adds (does Snowflake
accept these exact statements?) is checked when you run load-snowflake.
"""
from pathlib import Path

import pytest

from ecom_etl.snowflake import (Target, count_sql, ident, load, load_sql, put_sql, script, setup_sql,
                                snowflake_type, table_ddl)


def test_types_map_every_gold_spark_type():
    assert snowflake_type("decimal(12,2)") == "NUMBER(12,2)"
    assert snowflake_type("DECIMAL(38, 0)") == "NUMBER(38,0)"
    assert [snowflake_type(t) for t in ("int", "bigint", "string", "double", "boolean", "date", "timestamp")] == \
        ["INTEGER", "INTEGER", "VARCHAR", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP_NTZ"]
    assert snowflake_type("array<string>") == "ARRAY"
    with pytest.raises(ValueError):
        snowflake_type("interval day")


def test_identifiers_cannot_inject_sql():
    assert ident("fact_order_line") == "FACT_ORDER_LINE"
    for bad in ("x; DROP TABLE y", "a.b", "1abc", "", "name'"):
        with pytest.raises(ValueError):
            ident(bad)
    with pytest.raises(ValueError):
        Target(database="ECOM; DROP DATABASE X")


def test_ddl_put_copy_and_count():
    t = Target()
    ddl = table_ddl(t, "dim_product", [("product_sk", "bigint"), ("category", "string")])
    assert ddl == "CREATE OR REPLACE TABLE ECOM.GOLD.DIM_PRODUCT (\n    PRODUCT_SK INTEGER,\n    CATEGORY VARCHAR\n)"
    remove, put, copy = load_sql(t, "dim_product", Path("/tmp/export/dim_product"))
    assert remove == "REMOVE @ECOM.GOLD.GOLD_STAGE/dim_product/"
    assert put == ("PUT 'file:///tmp/export/dim_product/*.parquet' @ECOM.GOLD.GOLD_STAGE/dim_product/ "
                   "AUTO_COMPRESS = FALSE OVERWRITE = TRUE")
    assert copy.startswith("COPY INTO ECOM.GOLD.DIM_PRODUCT FROM @ECOM.GOLD.GOLD_STAGE/dim_product/ ")
    assert "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in copy and "PURGE = TRUE" in copy
    assert count_sql(t, "dim_product") == "SELECT COUNT(*) FROM ECOM.GOLD.DIM_PRODUCT"
    assert setup_sql(t)[0].startswith("CREATE WAREHOUSE IF NOT EXISTS ECOM_WH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60")
    with pytest.raises(ValueError):
        put_sql(t, "dim_product", "/tmp/it's here")


class FakeCursor:
    def __init__(self, counts):
        self.counts, self.statements, self._last = counts, [], None

    def execute(self, sql):
        self.statements.append(sql)
        self._last = sql
        return self

    def fetchone(self):
        table = self._last.rsplit(".", 1)[1].lower()
        return (self.counts[table],)


class FakeConnection:
    def __init__(self, counts):
        self.cur = FakeCursor(counts)

    def cursor(self):
        return self.cur


MANIFEST = {
    "dim_product": {"columns": [["product_sk", "bigint"], ["category", "string"]], "rows": 3, "files": "/x/dim_product"},
    "fact_payment": {"columns": [["order_id", "string"], ["payment_value", "decimal(12,2)"]], "rows": 5,
                     "files": "/x/fact_payment"},
}


def test_load_runs_setup_then_each_table_and_compares_counts():
    conn = FakeConnection({"dim_product": 3, "fact_payment": 4})
    results = load(conn, MANIFEST, Target())
    assert results == {"dim_product": {"expected": 3, "loaded": 3, "ok": True},
                       "fact_payment": {"expected": 5, "loaded": 4, "ok": False}}
    s = conn.cur.statements
    assert s[:len(setup_sql(Target()))] == setup_sql(Target())
    per_table = s[len(setup_sql(Target())):]
    assert [x.split()[0] for x in per_table] == ["CREATE", "REMOVE", "PUT", "COPY", "SELECT"] * 2


def test_script_is_complete_and_runnable_in_order():
    text = script(MANIFEST, Target())
    assert text.index("CREATE STAGE") < text.index("CREATE OR REPLACE TABLE ECOM.GOLD.DIM_PRODUCT")
    assert text.index("REMOVE @ECOM.GOLD.GOLD_STAGE/fact_payment/") < text.index("PUT 'file:///x/fact_payment/")
    assert text.count("COPY INTO") == 2 and text.count("SELECT COUNT(*)") == 2
    assert all(line.endswith(";") for line in text.splitlines() if line and not line.startswith("--")
               and not line.startswith("    ") and not line.endswith("("))


def test_manifest_from_docker_points_at_the_folders_next_to_it(tmp_path):
    """An export made in Docker records /app/... paths; Windows must still find the files."""
    import json

    from ecom_etl.spark.export import read_manifest

    out = tmp_path / "export" / "snowflake"
    (out / "dim_product").mkdir(parents=True)
    (out / "dim_product" / "part-0.parquet").write_bytes(b"")
    manifest = {"dim_product": {"columns": [["product_sk", "bigint"]], "rows": 3,
                                "files": "/app/data/lake/synthetic/export/snowflake/dim_product"},
                "dim_seller": {"columns": [["seller_sk", "bigint"]], "rows": 2,
                               "files": "/somewhere/else/dim_seller"}}
    (out / "_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    got = read_manifest(out)
    assert Path(got["dim_product"]["files"]) == (out / "dim_product").resolve()
    assert got["dim_seller"]["files"] == "/somewhere/else/dim_seller"   # no folder here: left as written
    assert got["dim_product"]["rows"] == 3


def test_missing_manifest_says_what_to_run(tmp_path):
    from ecom_etl.spark.export import read_manifest

    with pytest.raises(FileNotFoundError, match="export-snowflake"):
        read_manifest(tmp_path)
