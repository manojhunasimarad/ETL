# Step 6: SQL analysis and dashboard

**Goal:** use the star schema the way analysts will: answer real business
questions in SQL, and put the key numbers and the data quality history
on a dashboard.

**JD lines this answers:** "build dashboards and visualizations";
"perform ad hoc data analysis"; proficiency in SQL.

---

## 1. Concepts

### 1.1 Ten questions, ten SQL techniques

Each file in [`sql/analysis/`](../../src/ecom_etl/sql/analysis) starts
with the question and the technique it practises. Together they cover
most of what SQL interviews ask.

| # | Question | Technique |
|---|---|---|
| 1 | Revenue month over month | CTE, `LAG`, `NULLIF` |
| 2 | Top categories and their share | `SUM(SUM(x)) OVER ()` (window over an aggregate) |
| 3 | Revenue by customer state | `RANK()`; state at order time thanks to SCD2 |
| 4 | Late deliveries by state | conditional aggregation `AVG(bool::int)`, `HAVING` |
| 5 | Repeat customer rate | per-person CTE, `COUNT(*) FILTER (WHERE ...)` |
| 6 | Payment mix | querying the payment fact at its own grain |
| 7 | Seller concentration (80/20) | running total `SUM() OVER (ORDER BY ... ROWS UNBOUNDED PRECEDING)` |
| 8 | Orders by weekday | letting `dim_date` do calendar logic |
| 9 | Customers who moved | SCD2 history, `LEFT JOIN` keeps empty versions |
| 10 | RFM segments | `NTILE(4)` scoring, CTE chain, `CASE` |

### 1.2 Window functions in one minute

A window function computes over a set of rows *related* to the current
row without collapsing them like `GROUP BY` does.
`function() OVER (PARTITION BY ... ORDER BY ... frame)`:

* `PARTITION BY` = which rows belong together (per customer, per state)
* `ORDER BY` = order inside the partition (needed for `LAG`, running totals, ranking)
* frame = which rows count for running sums (`ROWS UNBOUNDED PRECEDING`)

`ROW_NUMBER` gives 1, 2, 3; `RANK` gives 1, 1, 3 on ties; `DENSE_RANK`
gives 1, 1, 2. Knowing the difference is a common interview question.

### 1.3 Metric definitions are decisions

"Revenue" here excludes canceled and unavailable orders and includes
freight (`line_total`); "items revenue" excludes freight. Repeat rate
counts **people** (`customer_unique_id`), not `customer_id` (step 1).
Write such definitions down: two dashboards that disagree by 3% almost
always differ in a definition, not in a bug.

### 1.4 Dashboard as code

The dashboard ([`dashboard/app.py`](../../dashboard/app.py)) is
Streamlit: a Python script, versioned, reviewed and tested in CI like the
pipeline. It reuses the same SQL files as `analyze`, so the report and
the dashboard cannot disagree. A BI tool like Metabase, Superset, Power
BI or Tableau is the usual choice when analysts build the charts
themselves; the star schema and `v_order_lines` serve both.

The **Data quality** tab reads the history written by step 5: checks per
run, flagged checks, quarantined rows by reason. Showing data quality next
to the business numbers is how people learn to trust them.

---

## 2. Run it

```bash
docker compose run --rm pipeline run --source synthetic
docker compose run --rm pipeline analyze --source synthetic   # reports/analysis_synthetic.md
docker compose --profile dashboard up -d dashboard           # http://localhost:8501
```

Without Docker: `pip install -e ".[postgres,dashboard]"`, then
`streamlit run dashboard/app.py`.

## 3. Exercises

1. Rewrite Q1 without a window function (self-join on the previous month). Compare readability and the `EXPLAIN` plan.
2. Q3 uses the state at order time. Write the "current state" version (join on `is_current`). When would a business want each?
3. Add Q11: average review-free "delivery promise gap" (estimated minus actual delivery days) by seller state.
4. Add a chart of monthly late-delivery rate to the Delivery tab.
5. On Olist, what is the repeat rate? What does that tell you about the business?

## 4. Check yourself

1. `ROW_NUMBER` vs `RANK` vs `DENSE_RANK`?
2. What does `SUM(SUM(price)) OVER ()` compute, and in which order does SQL evaluate it?
3. Why does Q6 not join `fact_payment` to `fact_order_line`?
4. Why count repeat customers with `customer_unique_id`?
5. How would you make sure two dashboards report the same revenue?
6. Dashboard as code vs a BI tool: pros and cons?

## 5. Learning material

**O'Reilly**
- *SQL for Data Analysis* (Cathy Tanimura): time series analysis, cohort analysis, window functions; Q1, Q5 and Q10 follow its patterns.
- *Learning SQL, 3rd Ed.*: the analytic functions chapter.

**Other**
- Streamlit documentation: charts, `st.cache_data`, and app testing (`streamlit.testing`).
- Practise window functions on LeetCode or DataLemur SQL problems; the JD lists SQL as mandatory.

## 6. Done when

- [ ] `analyze` runs on synthetic and Olist and you can explain each query
- [ ] the dashboard runs and you can walk through it in two minutes
- [ ] your Step 6 story is in [`STORIES.md`](../STORIES.md)
