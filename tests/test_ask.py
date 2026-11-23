"""Ask-your-data without network or database: the guards, the Gemini request, the repair loop.

The real path (Gemini plus Postgres) is exercised by tests/integration/test_ask_db.py
with a stand-in model, and by `python -m ecom_etl ask-eval` with your key.
"""
import io
import json
import urllib.error

import pytest

from ecom_etl import ask as A

GOLD = "synthetic_gold"


@pytest.mark.parametrize("sql", [
    "SELECT 1",
    "select count(*) from fact_order_line;",
    "WITH m AS (SELECT 1 AS x) SELECT x FROM m",
    "SELECT order_id FROM fact_order_line WHERE order_status = 'canceled; drop table x'",
    'SELECT "update" FROM t',
    "SELECT f.line_total FROM synthetic_gold.fact_order_line f",
    "SELECT d.year_month FROM dim_date d -- a trailing comment; with a semicolon",
    "SELECT customer_unique_id, max(version) AS versions FROM dim_customer GROUP BY 1",
])
def test_read_only_queries_pass(sql):
    assert A.check_sql(sql, GOLD)


@pytest.mark.parametrize("sql, reason", [
    ("", "empty"),
    ("SELECT 1; SELECT 2", "more than one statement"),
    ("DELETE FROM fact_order_line", "only SELECT"),
    ("WITH x AS (DELETE FROM fact_order_line RETURNING *) SELECT * FROM x", "delete"),
    ("SELECT * INTO new_table FROM fact_order_line", "into"),
    ("SELECT pg_sleep(60)", "pg_sleep"),
    ("SELECT * FROM pg_catalog.pg_roles", "pg_catalog"),
    ("SELECT * FROM information_schema.tables", "information_schema"),
    ("SELECT * FROM synthetic_core.orders", "synthetic_core"),
    ("SELECT * FROM public.t", "public"),
    ("SELECT current_setting('server_version')", "current_setting"),
    ("SELECT 1; /* hidden */ DROP TABLE x", "more than one statement"),
    ("SELECT set_config('statement_timeout', '0', false)", "set"),
])
def test_unsafe_queries_are_refused(sql, reason):
    with pytest.raises(A.UnsafeSql, match=reason):
        A.check_sql(sql, GOLD)


def test_json_answer_with_or_without_fence():
    assert A.parse_json_answer('{"sql": "SELECT 1", "explanation": "x"}')["sql"] == "SELECT 1"
    assert A.parse_json_answer('```json\n{"sql": null, "explanation": "no"}\n```')["sql"] is None
    with pytest.raises(A.LlmError):
        A.parse_json_answer("SELECT 1")
    with pytest.raises(A.LlmError):
        A.parse_json_answer('{"query": "SELECT 1"}')


def _reply(text, tokens=42):
    return json.dumps({"candidates": [{"content": {"parts": [{"text": "thinking...", "thought": True},
                                                              {"text": text}]}}],
                       "usageMetadata": {"totalTokenCount": tokens}}).encode()


def test_gemini_request_and_response():
    seen = {}

    def transport(request, timeout):
        seen.update(url=request.full_url, key=request.get_header("X-goog-api-key"),
                    body=json.loads(request.data), timeout=timeout)
        return _reply('{"sql": "SELECT 1", "explanation": "one"}')

    llm = A.Gemini("k-123", model="gemini-test", transport=transport)
    answer, usage = llm.generate_json("system rules", "the question")
    assert answer == {"sql": "SELECT 1", "explanation": "one"} and usage["totalTokenCount"] == 42
    assert seen["url"].endswith("/models/gemini-test:generateContent")
    assert seen["key"] == "k-123" and "k-123" not in seen["url"]          # key in a header, never in the URL
    assert seen["body"]["systemInstruction"]["parts"][0]["text"] == "system rules"
    assert seen["body"]["generationConfig"]["responseMimeType"] == "application/json"


def _http_error(code):
    return urllib.error.HTTPError("https://x", code, "err", {}, io.BytesIO(b'{"error": "detail"}'))


def test_gemini_errors_are_explained(monkeypatch):
    monkeypatch.setattr(A.time, "sleep", lambda s: None)
    for code, words in ((403, "not valid"), (404, "GEMINI_MODEL"), (429, "rate limit")):
        def transport(request, timeout, code=code):
            raise _http_error(code)
        with pytest.raises(A.LlmError, match=words):
            A.Gemini("k", model="m", transport=transport).generate_json("s", "p")
    calls = []

    def flaky(request, timeout):  # one 429, then success
        calls.append(1)
        if len(calls) == 1:
            raise _http_error(429)
        return _reply('{"sql": "SELECT 2", "explanation": ""}')
    assert A.Gemini("k", model="m", transport=flaky).generate_json("s", "p")[0]["sql"] == "SELECT 2"


class FakeLlm:
    model = "fake"

    def __init__(self, answers):
        self.answers, self.prompts = list(answers), []

    def generate_json(self, system, prompt):
        self.prompts.append(prompt)
        return self.answers.pop(0), {"totalTokenCount": 10}


def test_ask_repairs_a_refused_query_then_runs():
    llm = FakeLlm([{"sql": "DELETE FROM fact_order_line", "explanation": "oops"},
                   {"sql": "SELECT count(*) AS n FROM fact_order_line", "explanation": "counts lines"}])
    ran = []
    answer = A.ask("how many lines?", "TABLE fact_order_line (order_id text)", GOLD, llm,
                   lambda q: ran.append(q) or A.Rows(["n"], [(5,)], False))
    assert answer.ok and answer.attempts == 2 and answer.rows == [(5,)] and answer.tokens == 20
    assert ran == ["SELECT count(*) AS n FROM fact_order_line"]
    assert "refused" in llm.prompts[1] and "DELETE FROM fact_order_line" in llm.prompts[1]


def test_ask_sends_a_database_error_back_once():
    llm = FakeLlm([{"sql": "SELECT nope FROM fact_order_line", "explanation": ""},
                   {"sql": "SELECT nope2 FROM fact_order_line", "explanation": ""}])

    def execute(q):
        raise RuntimeError(f'column "{q.split()[1]}" does not exist\nLINE 1: ...')
    answer = A.ask("q", "schema", GOLD, llm, execute)
    assert not answer.ok and answer.attempts == 2
    assert answer.error == 'database error: column "nope2" does not exist'
    assert 'column "nope" does not exist' in llm.prompts[1]


def test_unanswerable_question_returns_no_sql():
    answer = A.ask("what is the weather?", "schema", GOLD, FakeLlm([{"sql": None, "explanation": "No weather data."}]),
                   lambda q: pytest.fail("nothing should run"))
    assert answer.sql is None and not answer.ok and answer.explanation == "No weather data."


def test_dotenv_never_overrides_and_key_is_required(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("# comment\nexport GEMINI_API_KEY='from-file'\nOTHER=1\n", encoding="utf-8")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.setattr(A.config, "PROJECT_ROOT", tmp_path)
    assert A.api_key() == "from-file"
    monkeypatch.setenv("GEMINI_API_KEY", "from-env")
    A.load_dotenv(env)
    assert A.api_key() == "from-env"
    monkeypatch.delenv("GEMINI_API_KEY")
    env.write_text("", encoding="utf-8")
    with pytest.raises(RuntimeError, match=".env"):
        A.api_key()


def test_schema_text_carries_meaning():
    text = A.render_schema([("fact_order_line", "line_total", "numeric"), ("dim_customer", "customer_sk", "bigint")])
    assert "TABLE fact_order_line (line_total numeric)" in text
    assert "SCD Type 2" in text and "grain" in text
    with pytest.raises(RuntimeError, match="load-gold"):
        A.render_schema([])


def test_execution_match_ignores_names_order_and_extra_columns():
    expected = A.Rows(["month", "revenue"], [("2018-01", 100.04), ("2018-02", 200.0)], False)
    got = A.Rows(["total", "ym", "orders"], [(200.0, "2018-02", 3), (100.0, "2018-01", 2)], False)
    assert A.result_matches(expected, got)
    assert not A.result_matches(expected, A.Rows(["ym"], [("2018-01",), ("2018-02",)], False))
    assert not A.result_matches(expected, A.Rows(["ym", "r"], [("2018-01", 100.0)], False))


def test_history_records_outcome_not_rows(tmp_path):
    answer = A.Answer("q", "SELECT 1", "e", ["x"], [(1,)], False, 1, 0.5, "fake", 10)
    path = A.record(answer, "synthetic", tmp_path / "h.jsonl")
    entry = json.loads(path.read_text().splitlines()[0])
    assert entry["sql"] == "SELECT 1" and entry["rows_returned"] == 1 and "rows" not in entry
