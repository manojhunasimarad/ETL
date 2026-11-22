#!/usr/bin/env bash
# Start Airflow in one container for local development: api-server (UI),
# scheduler, dag-processor and triggerer, with Postgres as metadata DB.
set -euo pipefail

python - <<'PY'
import os, psycopg
conn = psycopg.connect(host=os.environ["ECOM_DB_HOST"], user=os.environ.get("ECOM_DB_USER", "ecom"),
                       password=os.environ.get("ECOM_DB_PASSWORD", "ecom"), dbname="ecom", autocommit=True)
if not conn.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'").fetchone():
    conn.execute("CREATE DATABASE airflow")
    print("created database airflow")
PY

exec airflow standalone
