"""E-commerce dashboard on the gold star schema (Streamlit).

    streamlit run dashboard/app.py          # http://localhost:8501

Reads Postgres (same ECOM_DB_* settings as the pipeline). The sales tabs
reuse the SQL files of step 6, so the dashboard and the analysis report
can never disagree. The data quality tab reads the check history that
step 5 writes to <source>_dq.
"""

from __future__ import annotations

import os
from decimal import Decimal

import pandas as pd
import streamlit as st

from ecom_etl.analysis import load_analyses
from ecom_etl.db import connect, schema_name

st.set_page_config(page_title="E-commerce ETL", layout="wide")


@st.cache_data(ttl=120, show_spinner=False)
def query(sql: str) -> pd.DataFrame:
    with connect(autocommit=True) as conn:
        cur = conn.execute(sql)
        df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
    for col in df.columns:  # Postgres NUMERIC arrives as Decimal; charts need floats
        if df[col].dtype == object and df[col].map(lambda v: isinstance(v, Decimal)).any():
            df[col] = df[col].astype(float)
    return df


def sources() -> list:
    df = query("SELECT schema_name FROM information_schema.schemata "
               "WHERE schema_name LIKE '%\\_gold' ORDER BY schema_name")
    return [s.removesuffix("_gold") for s in df["schema_name"]]


available = sources()
if not available:
    st.title("E-commerce ETL")
    st.info("No gold schema found. Run the pipeline first: `python -m ecom_etl run --source synthetic`")
    st.stop()

default = os.environ.get("ECOM_DASHBOARD_SOURCE", available[0])
source = st.sidebar.selectbox("Source", available, index=available.index(default) if default in available else 0)
gold, dq = schema_name(source, "gold"), schema_name(source, "dq")
sql = {a.name: a.sql for a in load_analyses(source)}

st.title(f"E-commerce dashboard: {source}")

kpi = query(f"""
    SELECT count(DISTINCT order_id) AS orders, sum(line_total) AS revenue,
           sum(line_total) / nullif(count(DISTINCT order_id), 0) AS aov,
           100.0 * avg(is_late::int) FILTER (WHERE is_delivered) AS late_pct
    FROM {gold}.fact_order_line WHERE order_status NOT IN ('canceled', 'unavailable')""").iloc[0]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Revenue (BRL)", f"{kpi.revenue or 0:,.0f}")
c2.metric("Orders", f"{int(kpi.orders or 0):,}")
c3.metric("Average order value", f"{kpi.aov or 0:,.2f}")
c4.metric("Late deliveries", f"{kpi.late_pct or 0:.1f}%")

sales, delivery, customers, quality = st.tabs(["Sales", "Delivery", "Customers", "Data quality"])

with sales:
    monthly = query(sql["q01_monthly_revenue_growth"])
    st.subheader("Revenue by month")
    st.line_chart(monthly, x="year_month", y="revenue")
    left, right = st.columns(2)
    with left:
        st.subheader("Top categories")
        st.bar_chart(query(sql["q02_top_categories"]), x="category", y="items_revenue", horizontal=True)
    with right:
        st.subheader("Payment mix")
        st.bar_chart(query(sql["q06_payment_mix"]), x="payment_type", y="payment_value")
    st.subheader("Seller concentration (top 20)")
    st.dataframe(query(sql["q07_seller_pareto"]), hide_index=True)

with delivery:
    perf = query(sql["q04_delivery_performance"])
    st.subheader("Late deliveries by customer state")
    st.bar_chart(perf, x="customer_state", y="late_pct")
    st.dataframe(perf, hide_index=True)
    st.subheader("Orders by weekday")
    st.bar_chart(query(sql["q08_weekday_pattern"]), x="day_name", y="orders")

with customers:
    repeat = query(sql["q05_repeat_customers"]).iloc[0]
    a, b = st.columns(2)
    a.metric("Customers", f"{int(repeat.customers):,}")
    b.metric("Repeat rate", f"{repeat.repeat_rate_pct:.2f}%")
    st.subheader("RFM segments")
    st.dataframe(query(sql["q10_rfm_segments"]), hide_index=True)
    st.subheader("Revenue by customer state")
    st.bar_chart(query(sql["q03_revenue_by_state"]), x="customer_state", y="revenue")

with quality:
    has_dq = not query(f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{dq}' "
                       f"AND table_name = 'check_results'").empty
    if not has_dq:
        st.info("No data quality history yet: run `python -m ecom_etl run` or `quality`.")
    else:
        runs = query(f"""
            SELECT run_id, min(checked_at) AS checked_at, count(*) AS checks,
                   count(*) FILTER (WHERE status = 'fail') AS flagged,
                   count(*) FILTER (WHERE status = 'fail' AND blocking) AS blocking
            FROM {dq}.check_results GROUP BY run_id ORDER BY run_id DESC""")
        latest = runs.iloc[0]
        x, y, z = st.columns(3)
        x.metric("Checks in last run", int(latest.checks))
        y.metric("Flagged", int(latest.flagged))
        z.metric("Blocking failures", int(latest.blocking))
        reasons = query(f"""
            SELECT table_name || ': ' || reason AS reason, rows FROM {dq}.quarantine_summary
            WHERE run_id = '{latest.run_id}' ORDER BY rows DESC""")
        st.subheader("Quarantined rows by reason (last run)")
        st.bar_chart(reasons, x="reason", y="rows", horizontal=True)
        st.subheader("Checks that flagged something (last run)")
        st.dataframe(query(f"""
            SELECT check_name, kind, severity, blocking, failed_rows, detail FROM {dq}.check_results
            WHERE run_id = '{latest.run_id}' AND status = 'fail' ORDER BY failed_rows DESC"""),
            hide_index=True)
        st.subheader("Run history")
        st.dataframe(runs, hide_index=True)
