"""Ask-your-data against a real Postgres, with a stand-in model (no network, no key).

What matters here is the database side of the guards: the reader role can only
read the gold schema, cannot write even when the SQL check is bypassed, and is
stopped by the timeout and the row cap.
"""
import pytest

from ecom_etl import ask as A
from ecom_etl import config
from ecom_etl.db import connect

pytestmark = pytest.mark.postgres
SOURCE, GOLD = "t_ask", "t_ask_gold"


@pytest.fixture(scope="module")
def warehouse():
    with connect(autocommit=True) as conn:
        for schema in (GOLD, "t_ask_core"):
            conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            conn.execute(f"CREATE SCHEMA {schema}")
        conn.execute(f"CREATE TABLE {GOLD}.fact_order_line (order_id text, order_status text, line_total numeric(12,2))")
        conn.execute(f"INSERT INTO {GOLD}.fact_order_line VALUES ('o1', 'delivered', 10.50), ('o2', 'canceled', 5.00),"
                     " ('o3', 'delivered', 4.25)")
        conn.execute("CREATE TABLE t_ask_core.secret (x text)")
    A.ensure_reader(SOURCE)
    A.ensure_reader(SOURCE)  # idempotent
    return GOLD


def test_reader_reads_gold(warehouse):
    rows = A.run_readonly("SELECT round(sum(line_total), 2) AS revenue FROM fact_order_line "
                          "WHERE order_status <> 'canceled'", warehouse)
    assert rows.columns == ["revenue"] and float(rows.rows[0][0]) == 14.75 and not rows.truncated


@pytest.mark.parametrize("sql", ["UPDATE fact_order_line SET line_total = 0",
                                 "CREATE TABLE fact_copy AS SELECT * FROM fact_order_line",
                                 "SELECT * FROM t_ask_core.secret"])
def test_reader_cannot_write_or_leave_gold_even_without_the_check(warehouse, sql):
    import psycopg

    with pytest.raises(psycopg.Error):
        A.run_readonly(sql, warehouse)
    with connect() as conn:
        assert conn.execute(f"SELECT sum(line_total) FROM {GOLD}.fact_order_line").fetchone()[0] == 19.75


def test_timeout_and_row_cap(warehouse):
    import psycopg

    with pytest.raises(psycopg.errors.QueryCanceled):
        A.run_readonly("SELECT count(*) FROM generate_series(1, 100000000)", warehouse, timeout_ms=200)
    capped = A.run_readonly("SELECT g FROM generate_series(1, 10) g", warehouse, max_rows=3)
    assert capped.rows == [(1,), (2,), (3,)] and capped.truncated


def test_grants_survive_a_reload_of_gold(warehouse):
    with connect(autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA {GOLD} CASCADE")
        conn.execute(f"CREATE SCHEMA {GOLD}")
        conn.execute(f"CREATE TABLE {GOLD}.fact_order_line AS SELECT 'o9'::text AS order_id, 'delivered'::text AS "
                     "order_status, 1.00::numeric(12,2) AS line_total")
    A.ensure_reader(SOURCE)
    assert A.run_readonly("SELECT count(*) FROM fact_order_line", GOLD).rows == [(1,)]


def test_whole_path_with_a_stand_in_model(warehouse, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "REPORTS_DIR", tmp_path)

    class Model:
        model = "stand-in"

        def __init__(self):
            self.prompts = []

        def generate_json(self, system, prompt):
            self.prompts.append(prompt)
            return {"sql": "SELECT order_id FROM fact_order_line ORDER BY order_id", "explanation": "all orders"}, {}

    llm = Model()
    answer = A.ask_warehouse("Which orders are there?", SOURCE, llm=llm)
    assert answer.ok and [r[0] for r in answer.rows] and answer.columns == ["order_id"]
    assert "TABLE fact_order_line (" in llm.prompts[0] and "Which orders are there?" in llm.prompts[0]
    assert (tmp_path / "ask_history.jsonl").exists()
