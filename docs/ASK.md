# Ask your data: plain English to SQL with Gemini

Ask the warehouse a question in plain English. Gemini (Google's model, through
your Google AI Studio key) writes one SQL query for the gold star schema; the
query runs on your own Postgres, and you get the rows, the SQL and a one-line
explanation, in the terminal or in the dashboard's **Ask** tab.

```bash
python -m ecom_etl ask "Which 5 states had the most late deliveries in 2018?"
```

## A hybrid, on purpose

| Runs in the cloud (Gemini) | Runs on your machine |
|---|---|
| reads the table descriptions and your question | stores the data, runs the SQL |
| writes one SQL query and an explanation | checks the query, enforces read-only access |
| sees **no rows** | keeps the history of questions and outcomes |

Only the schema (table and column names, types, and what each table means)
and the question are sent. Results are never sent to the model. On the free
tier of the Gemini API, Google may use what you send to improve its products,
which is one more reason to send only the schema.

## Four guards between the model and the database

A language model can write wrong SQL, and in principle harmful SQL. Each guard
below is enough on its own to stop a write:

1. **The query is checked** (`check_sql`): one statement, starting with
   `SELECT` or `WITH`; no `INSERT`, `UPDATE`, `DELETE`, DDL, `COPY`,
   `SELECT ... INTO`, no system catalogs, no other schemas (raw, core, dq,
   stream, public), no server functions such as `pg_sleep`. Words inside
   strings and comments are ignored, so `WHERE status = 'drop'` is fine.
2. **It runs as `ecom_reader`**, a role with `SELECT` on the gold schema and
   nothing else. `ensure_reader` creates it and re-grants before every
   question, because loading gold recreates the schema.
3. **In a READ ONLY transaction**, rolled back afterwards; the role's default
   is read-only too.
4. **With a statement timeout (10 s) and a row cap (500).**

When Postgres rejects a query (a column that does not exist, say), the error
goes back to Gemini once with a request to fix it; the integration tests
check that the guards hold even when the first check is bypassed.

## Set up

1. Get a key in Google AI Studio (API keys page).
2. Copy `.env.example` to `.env` at the root of the repository and set
   `GEMINI_API_KEY=...`. `.env` is git-ignored; never put the key anywhere else.
3. Load gold into Postgres (`python -m ecom_etl run --source synthetic`).
4. Ask: `python -m ecom_etl ask "..."`, or open the dashboard's **Ask** tab.

The model is `gemini-flash-latest`, an alias Google moves to each new Flash
release. To pin a version, set `GEMINI_MODEL` in `.env`.

## Measure it

```bash
python -m ecom_etl ask-eval --source synthetic
```

The ten questions of step 6 already have hand-written SQL, so each is asked in
plain English and both queries are run. An answer **matches** when it has the
same number of rows and every column of the hand-written result appears among
Gemini's columns with the same values (rounded to 0.1), whatever the column
names or order. The report, `reports/ask_eval_synthetic.md`, gives matches,
attempts, seconds and tokens per question. It waits 6 seconds between
questions to stay within the free tier's requests per minute (`--pause`).

A mismatch is not always the model's fault: "revenue" can reasonably include
or exclude freight, and the hand-written SQL made one choice. Read the SQL of
each mismatch before deciding.

## What to say about it

- Why the model never sees data, and what it does see.
- Why one guard is not enough: the SQL check is a convenience that gives
  clear errors; the database role is the guarantee.
- How the feature is measured (execution match against hand-written SQL), and
  what the measurement cannot tell you.

## Learn more

- Google AI for Developers: Gemini API quickstart, structured output, rate limits.
- PostgreSQL manual: `CREATE ROLE`, `GRANT`, `default_transaction_read_only`, `statement_timeout`.
- The idea in the literature: "text-to-SQL" and "execution accuracy" (the Spider and BIRD benchmarks).
