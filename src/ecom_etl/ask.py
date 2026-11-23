"""Ask the warehouse in plain English: Gemini writes the SQL, Postgres answers.

A hybrid by design. The language model runs in the cloud (Google's Gemini API,
with your Google AI Studio key) and sees only two things: a description of the
gold star schema and your question. It returns one SQL query. The query runs
on your own Postgres, and the rows never leave your machine.

A model can write wrong or harmful SQL, so the query passes four guards, each
enough on its own for writes:

1. ``check_sql`` refuses anything that is not a single SELECT (or WITH) over
   the gold tables: no data changes, no DDL, no ``SELECT ... INTO``, no
   system catalogs, no other schemas, no server functions such as pg_sleep;
2. the query runs as ``ecom_reader``, a role that may only SELECT on the gold
   schema (created and re-granted by ``ensure_reader``);
3. inside a READ ONLY transaction, rolled back afterwards;
4. with a statement timeout and a cap on the rows fetched.

If Postgres rejects the query (a misspelt column, say), the error goes back to
the model once, with a request to fix it. Every question, query, outcome and
token count is appended to ``reports/ask_history.jsonl``.

``ask-eval`` measures it: the ten questions of step 6 come with hand-written
SQL, so Gemini's answers can be checked against them by running both.
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from . import config
from .db import DbSettings, connect, schema_name

# An alias Google moves to each new Flash release (with two weeks' notice), so the
# code does not break when a version is retired. Pin a version with GEMINI_MODEL.
DEFAULT_MODEL = "gemini-flash-latest"
API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
READER_ROLE = "ecom_reader"
MAX_ROWS = 500
TIMEOUT_MS = 10_000

TABLE_NOTES = {
    "fact_order_line": "One row per order line (the grain). line_total = price + freight_value. "
                       "order_status is one of delivered, shipped, canceled, unavailable, invoiced, processing, "
                       "created, approved; revenue excludes canceled and unavailable. date_key joins dim_date "
                       "(purchase date), delivered_date_key the delivery date. customer_sk is the customer version "
                       "valid when the order was placed. product_sk, seller_sk join their dimensions. -1 is the "
                       "unknown member. order_month is 'yyyy-MM'.",
    "fact_payment": "One row per payment of an order (an order can have several). Aggregate per order before "
                    "joining to fact_order_line, or totals are counted more than once.",
    "dim_customer": "SCD Type 2: one row per version of a person (customer_unique_id), valid from effective_from "
                    "to effective_to; is_current marks the latest. Count people with customer_unique_id, not "
                    "customer_sk. customer_sk -1 is the unknown member.",
    "dim_product": "One row per product. category_name is Portuguese, category_name_english the translation. "
                   "product_sk -1 is the unknown member.",
    "dim_seller": "One row per seller with city and state. seller_sk -1 is the unknown member.",
    "dim_date": "One row per calendar day. date_key is yyyymmdd as an integer; year_month is 'yyyy-MM'; "
                "day_of_week 1 = Monday.",
    "agg_daily_sales": "Daily totals pre-computed from fact_order_line.",
}

SYSTEM_PROMPT = """You translate questions about an e-commerce data warehouse into PostgreSQL 16.
Return JSON only: {"sql": string or null, "explanation": string}.
Rules for "sql":
- exactly one read-only query: SELECT, optionally starting with WITH; no semicolon, no comments;
- only the tables listed, without a schema prefix (the search path is set for you);
- never INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, GRANT, COPY or SELECT ... INTO;
- give columns readable names, round money to 2 decimals, add an ORDER BY;
- return at most 100 rows unless the question asks for everything;
- if the tables cannot answer the question, set "sql" to null.
"explanation": one or two plain sentences on how the query answers the question and any assumption made
(for example, that revenue excludes canceled orders)."""


# --------------------------------------------------------------------------- the key

def load_dotenv(path: Path) -> None:
    """Read KEY=VALUE lines from a .env file into the environment, never overriding what is set."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip().removeprefix("export ").strip(), value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def api_key() -> str:
    load_dotenv(config.PROJECT_ROOT / ".env")
    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not key:
        raise RuntimeError("No Gemini API key. Put GEMINI_API_KEY=... in the .env file at the root of the "
                           "repository (it is git-ignored), or set the environment variable.")
    return key


# --------------------------------------------------------------------------- the model

class LlmError(RuntimeError):
    pass


Transport = Callable[[urllib.request.Request, float], bytes]


def _urlopen(request: urllib.request.Request, timeout: float) -> bytes:
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 (fixed https host)
        return response.read()


@dataclass
class Gemini:
    """The Gemini API's generateContent over HTTPS, asking for JSON. No SDK needed."""

    key: str
    model: str = field(default_factory=lambda: os.environ.get("GEMINI_MODEL", DEFAULT_MODEL))
    timeout: float = 60.0
    retries: int = 2
    transport: Transport = _urlopen

    def generate_json(self, system: str, prompt: str) -> Tuple[dict, dict]:
        """Return (parsed JSON answer, token usage)."""
        body = {"systemInstruction": {"parts": [{"text": system}]},
                "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                "generationConfig": {"responseMimeType": "application/json"}}
        request = urllib.request.Request(
            API_URL.format(model=self.model), data=json.dumps(body).encode("utf-8"), method="POST",
            headers={"Content-Type": "application/json", "x-goog-api-key": self.key})
        for attempt in range(self.retries + 1):
            try:
                raw = self.transport(request, self.timeout)
                break
            except urllib.error.HTTPError as e:
                detail = e.read().decode("utf-8", "replace")[:300] if hasattr(e, "read") else ""
                if e.code == 429 and attempt < self.retries:  # rate limit: the free tier allows a few per minute
                    time.sleep(10 * (attempt + 1))
                    continue
                hints = {400: "the request was refused", 403: "the API key is not valid for the Gemini API",
                         404: f"model '{self.model}' not found; set GEMINI_MODEL to a current model",
                         429: "rate limit or quota reached; wait a minute"}
                raise LlmError(f"Gemini API {e.code}: {hints.get(e.code, 'error')}. {detail}") from e
            except urllib.error.URLError as e:
                raise LlmError(f"Gemini API not reachable: {e.reason}") from e
        reply = json.loads(raw.decode("utf-8"))
        candidates = reply.get("candidates") or []
        if not candidates:
            raise LlmError(f"Gemini returned no answer: {json.dumps(reply.get('promptFeedback', reply))[:300]}")
        parts = candidates[0].get("content", {}).get("parts", [])
        text = "".join(p.get("text", "") for p in parts if not p.get("thought"))
        return parse_json_answer(text), reply.get("usageMetadata", {})


def parse_json_answer(text: str) -> dict:
    """The model's JSON, tolerating a ```json fence around it."""
    cleaned = re.sub(r"^\s*```(?:json)?\s*|\s*```\s*$", "", text.strip())
    try:
        answer = json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise LlmError(f"the model did not return JSON: {text[:200]}") from e
    if not isinstance(answer, dict) or "sql" not in answer:
        raise LlmError(f"the model's JSON has no 'sql' field: {text[:200]}")
    return answer


# --------------------------------------------------------------------------- guard 1: the SQL itself

class UnsafeSql(ValueError):
    pass


FORBIDDEN_WORDS = {
    "insert", "update", "delete", "merge", "upsert", "drop", "alter", "create", "truncate", "grant", "revoke",
    "copy", "call", "do", "execute", "prepare", "deallocate", "listen", "notify", "unlisten", "vacuum", "analyze",
    "cluster", "reindex", "refresh", "lock", "set", "reset", "comment", "security", "into", "import", "load",
    "checkpoint", "discard",
}
FORBIDDEN_NAMES = re.compile(r"\b(pg_\w+|information_schema|dblink\w*|lo_\w+|current_setting|set_config"
                             r"|txid_\w+)\b", re.IGNORECASE)


def _strip_literals(sql: str) -> str:
    """Remove string literals, quoted identifiers and comments, so words inside them are ignored."""
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"\$(\w*)\$.*?\$\1\$", " '' ", sql, flags=re.DOTALL)  # dollar quoting
    sql = re.sub(r"'(?:[^']|'')*'", " '' ", sql)
    return re.sub(r'"(?:[^"]|"")*"', ' "x" ', sql)


def check_sql(sql: str, gold: str) -> str:
    """Return the query without a trailing semicolon, or raise UnsafeSql with the reason."""
    if not sql or not sql.strip():
        raise UnsafeSql("empty query")
    query = sql.strip().rstrip(";").strip()
    bare = _strip_literals(query)
    if ";" in bare:
        raise UnsafeSql("more than one statement")
    words = re.findall(r"[A-Za-z_][A-Za-z0-9_$]*", bare)
    if not words or words[0].lower() not in ("select", "with"):
        raise UnsafeSql("only SELECT queries are allowed")
    bad = sorted({w.lower() for w in words} & FORBIDDEN_WORDS)
    if bad:
        raise UnsafeSql("not allowed in a read-only question: " + ", ".join(bad))
    names = FORBIDDEN_NAMES.search(bare)
    if names:
        raise UnsafeSql(f"system objects are not allowed: {names.group(0)}")
    other_schema = re.search(r"\b(\w+_(?:raw|core|dq|stream|gold)|public)\s*\.", bare, re.IGNORECASE)
    if other_schema and other_schema.group(1).lower() != gold.lower():
        raise UnsafeSql(f"only the gold tables can be queried, not {other_schema.group(1)}")
    return query


# --------------------------------------------------------------------------- guards 2-4: the database

def reader_password() -> str:
    return os.environ.get("ECOM_READER_PASSWORD", "ecom_reader")


def ensure_reader(source: str, admin: Optional[DbSettings] = None) -> None:
    """Create the read-only role if needed and (re)grant SELECT on the gold schema.

    Loading gold drops and recreates the schema, which drops the grants, so
    this runs before every question. It is idempotent.
    """
    from psycopg import sql

    admin = admin or DbSettings.from_env()
    gold = schema_name(source, "gold")
    role, schema = sql.Identifier(READER_ROLE), sql.Identifier(gold)
    with connect(admin, autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (READER_ROLE,)).fetchone()
        verb = "ALTER" if exists else "CREATE"
        conn.execute(sql.SQL(verb + " ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD {}")
                     .format(role, sql.Literal(reader_password())))
        conn.execute(sql.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(role))
        conn.execute(sql.SQL("ALTER ROLE {} SET statement_timeout = {}").format(role, sql.Literal(f"{TIMEOUT_MS}ms")))
        conn.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(admin.dbname), role))
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema, role))
        conn.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(schema, role))


def reader_settings(admin: Optional[DbSettings] = None) -> DbSettings:
    admin = admin or DbSettings.from_env()
    return DbSettings(host=admin.host, port=admin.port, user=READER_ROLE, password=reader_password(),
                      dbname=admin.dbname)


@dataclass
class Rows:
    columns: List[str]
    rows: List[tuple]
    truncated: bool


def run_readonly(query: str, gold: str, settings: Optional[DbSettings] = None,
                 max_rows: int = MAX_ROWS, timeout_ms: int = TIMEOUT_MS) -> Rows:
    """Run a checked query as the reader role, in a READ ONLY transaction that is rolled back."""
    from psycopg import sql

    with connect(settings or reader_settings()) as conn:
        conn.read_only = True
        cur = conn.cursor()
        cur.execute(sql.SQL("SET LOCAL statement_timeout = {}").format(sql.Literal(int(timeout_ms))))
        cur.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(gold)))
        cur.execute(query)
        columns = [d.name for d in cur.description or []]
        fetched = cur.fetchmany(max_rows + 1)
        conn.rollback()
    return Rows(columns, [tuple(r) for r in fetched[:max_rows]], len(fetched) > max_rows)


def describe_schema(gold: str, settings: Optional[DbSettings] = None) -> str:
    """The gold tables and columns from information_schema, with what each table means."""
    with connect(settings or reader_settings()) as conn:
        cols = conn.execute(
            "SELECT table_name, column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = %s ORDER BY table_name, ordinal_position", (gold,)).fetchall()
    return render_schema(cols)


def render_schema(columns: Sequence[Tuple[str, str, str]]) -> str:
    tables: Dict[str, List[str]] = {}
    for table, column, dtype in columns:
        tables.setdefault(table, []).append(f"{column} {dtype}")
    if not tables:
        raise RuntimeError("the gold schema has no tables: run the pipeline and load-gold first")
    lines = []
    for table, cols in tables.items():
        lines.append(f"TABLE {table} ({', '.join(cols)})")
        if table in TABLE_NOTES:
            lines.append(f"  -- {TABLE_NOTES[table]}")
    return "\n".join(lines)


# --------------------------------------------------------------------------- asking

@dataclass
class Answer:
    question: str
    sql: Optional[str]
    explanation: str
    columns: List[str]
    rows: List[tuple]
    truncated: bool
    attempts: int
    seconds: float
    model: str
    tokens: int
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.sql is not None


Executor = Callable[[str], Rows]


def ask(question: str, schema_text: str, gold: str, llm, execute: Executor, max_attempts: int = 2) -> Answer:
    """Question → SQL from the model → guard → run; one repair round on a refusal or a database error."""
    t0 = time.perf_counter()
    prompt = f"Tables:\n{schema_text}\n\nQuestion: {question}"
    tokens, last_sql, explanation, error = 0, None, "", None
    for attempt in range(1, max_attempts + 1):
        answer, usage = llm.generate_json(SYSTEM_PROMPT, prompt)
        tokens += int(usage.get("totalTokenCount", 0) or 0)
        last_sql, explanation = answer.get("sql"), str(answer.get("explanation", "")).strip()
        if not last_sql:
            return Answer(question, None, explanation or "The tables cannot answer this question.", [], [], False,
                          attempt, round(time.perf_counter() - t0, 2), getattr(llm, "model", ""), tokens)
        try:
            query = check_sql(last_sql, gold)
            result = execute(query)
            return Answer(question, query, explanation, result.columns, result.rows, result.truncated, attempt,
                          round(time.perf_counter() - t0, 2), getattr(llm, "model", ""), tokens)
        except UnsafeSql as e:
            error = f"refused: {e}"
        except Exception as e:  # a database error: send it back once
            error = f"database error: {str(e).strip().splitlines()[0]}"
        prompt = (f"Tables:\n{schema_text}\n\nQuestion: {question}\n\nYour previous query:\n{last_sql}\n"
                  f"was not used because of a {error}\nReturn a corrected query.")
    return Answer(question, last_sql, explanation, [], [], False, max_attempts, round(time.perf_counter() - t0, 2),
                  getattr(llm, "model", ""), tokens, error)


def _jsonable(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def record(answer: Answer, source: str, path: Optional[Path] = None) -> Path:
    """Append the question and its outcome (not the rows) to the history file."""
    path = path or config.REPORTS_DIR / "ask_history.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    entry = {k: v for k, v in asdict(answer).items() if k not in ("rows", "columns")}
    entry.update(source=source, rows_returned=len(answer.rows), at=datetime.now().isoformat(timespec="seconds"))
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, default=_jsonable) + "\n")
    return path


def ask_warehouse(question: str, source: str, llm=None) -> Answer:
    """The whole path for one question against ``<source>_gold``."""
    gold = schema_name(source, "gold")
    ensure_reader(source)
    llm = llm or Gemini(api_key())
    answer = ask(question, describe_schema(gold), gold, llm, lambda q: run_readonly(q, gold))
    record(answer, source)
    return answer


# --------------------------------------------------------------------------- evaluation

def _normal(value):
    value = _jsonable(value)
    if isinstance(value, float):
        return round(value, 1)
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return float(value)
    return str(value)


def result_matches(expected: Rows, got: Rows) -> bool:
    """Execution match: same number of rows, and every column of the hand-written answer
    appears among the model's columns with the same values (rounded to 0.1), in any column
    order and under any name. Row order is ignored."""
    if len(expected.rows) != len(got.rows) or not expected.rows:
        return False
    got_cols = [sorted((_normal(r[i]) for r in got.rows), key=repr) for i in range(len(got.columns))]
    for i in range(len(expected.columns)):
        want = sorted((_normal(r[i]) for r in expected.rows), key=repr)
        if want not in got_cols:
            return False
    return True


def evaluate(source: str, llm=None, pause: float = 6.0) -> List[dict]:
    """Ask Gemini each step-6 question and compare its result with the hand-written SQL's."""
    from .analysis import load_analyses

    gold = schema_name(source, "gold")
    ensure_reader(source)
    llm = llm or Gemini(api_key())
    schema_text = describe_schema(gold)
    results = []
    for i, analysis in enumerate(load_analyses(source)):
        if i:
            time.sleep(pause)  # stay under the free tier's requests per minute
        question = re.sub(r"^Q\d+\.\s*", "", analysis.question)
        expected = run_readonly(analysis.sql, gold)
        answer = ask(question, schema_text, gold, llm, lambda q: run_readonly(q, gold))
        record(answer, source)
        got = Rows(answer.columns, answer.rows, answer.truncated)
        results.append({"name": analysis.name, "question": question, "ok": answer.ok,
                        "match": answer.ok and result_matches(expected, got), "attempts": answer.attempts,
                        "seconds": answer.seconds, "tokens": answer.tokens, "sql": answer.sql, "error": answer.error})
    return results
