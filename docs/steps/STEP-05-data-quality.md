# Step 5: Data quality framework

**Goal:** turn the profiling findings of step 1 into automated checks
that run on every pipeline run, stop bad data from being published, keep
a history, and can be *measured*.

**JD lines this answers:** "define ETL strategy & automated Data Quality
checks"; "ensure data accuracy, completeness, and reliability".

---

## 1. Concepts

### 1.1 Dimensions of data quality

| Dimension | Question | Checks here |
|---|---|---|
| Completeness | is everything there? | `not_null:*`, `fact_order_line.complete`, `silver_not_empty` |
| Validity | does it follow the rules? | `type:*`, `accepted_values:*`, `range:*` |
| Uniqueness | no duplicates? | `exact_duplicate`, `duplicate_key`, `*_key_unique`, `grain_unique` |
| Consistency / integrity | do tables agree? | `fk:*`, `no_unknown_members`, `revenue_reconciles`, `payment_reconciliation` |
| Accuracy (business rules) | is it plausible? | `sequence:*`, `future_event` |
| Timeliness | is it recent? | `orders.freshness` |
| Volume | did something break upstream? | `orders.volume_change`, `quarantine_rate` |

### 1.2 Two kinds of checks, two different reactions

* **Row-level rules** (80) run inside silver. A failing row goes to
  quarantine with its reason; the rest of the data flows on. They never
  stop the pipeline. Their result is a count: how many rows each rule caught.
* **Table-level gates** (35) look at a whole table after a layer is
  built. A failed **blocking** gate stops the pipeline before the next
  layer is published. Non-blocking gates (freshness, reconciliation,
  volume) are warnings for a human.

The row-level rules are *generated from the contract* (`contracts.py`),
so adding a column with `nullable=False` automatically adds a check.

### 1.3 Write, audit, publish

```
bronze → silver → [silver gates] → gold → [gold gates] → load Postgres
                     stop here           stop here
                     if blocking         if blocking
```

Data is written to a layer, audited, and only then published to the
next consumer. Analysts never see a half-broken star schema; the report
explaining why is still written (`run` exits with code 2).

### 1.4 Severity and thresholds

A gate that fires on every run gets ignored (alert fatigue). Choices made
here, each a trade-off you should be able to defend:

* `quarantine_rate` blocks above 10%: a few bad rows are normal, a third
  of the table quarantined means an upstream system broke.
* `freshness` only warns: on Olist it *always* fires (the data ends in
  2018), which is correct to report but must not block a historical load.
* `payment_reconciliation` warns: real Olist has installment interest, so
  payments legitimately differ from item totals for some orders.

### 1.5 Measuring the checks: precision and recall

The generator knows every defect it injected (`_defects.csv`), so the
quarantine can be scored like a classifier:

* **Recall** = injected bad rows that were caught / all injected bad rows
* **Precision** = caught rows that really were bad / all caught rows

Rows quarantined only because their parent order was rejected
(`parent_quarantined:`) are excluded from precision: they are a
consequence, not a detection. The tests require 100% / 100% on the
synthetic data, and fixes and dedupes are checked the same way.

On real Olist data there is no ground truth; there the report gives
counts, and your step 1 decisions say whether they are right.

### 1.6 Where results go

* `reports/dq_report_<source>.md`: human-readable report.
* `data/lake/<source>/_reports/quality_<run>.json`: everything, machine-readable.
* Postgres `<source>_dq.check_results` and `quarantine_summary`: one row
  per check per run (history). The dashboard in step 6 reads these.

### 1.7 Tools you will hear about

Great Expectations, Soda, dbt tests and Deequ (Spark) do what
`quality.py` does, with more features. Writing it yourself first means
you can explain what those tools automate, and why a gate and a
row-level rule need different reactions.

---

## 2. Code tour

1. [`spark/quality.py`](../../src/ecom_etl/spark/quality.py): `row_level_checks` and `table_level_checks` (the registry), `silver_gate_results`, `gold_gate_results`, `gate`, `evaluate_against_ground_truth`.
2. [`spark/pipeline.py`](../../src/ecom_etl/spark/pipeline.py): `run_pipeline`, the stages with gates in between.
3. [`tests/integration/test_quality.py`](../../tests/integration/test_quality.py).

## 3. Run it

```bash
docker compose run --rm pipeline run --source synthetic     # whole pipeline
docker compose run --rm pipeline quality --source synthetic # re-check only
```

Then open `reports/dq_report_synthetic.md`. On Olist:

```bash
docker compose run --rm pipeline run --source olist
```

The numbers for the resume ("XX checks", "flagged XX bad records per
run") come from this report; copy them into
[`docs/RESULTS.md`](../RESULTS.md) with the date and the data used.

## 4. Exercises

1. Lower `MAX_QUARANTINE_RATE` to 1% and run. Which gate blocks? Read the exit code.
2. Add a row-level rule: `freight_value <= price * 3` as a **warning**. Where do you add it so it shows up in silver, the registry and the report?
3. Run `run` twice. What does `orders.volume_change` say the second time? Then generate 1,000 orders and run again.
4. Query the history in Postgres: quarantined rows per run and reason.
5. On Olist: which table-level checks fail, and is each failure a data problem or a wrong threshold?

## 5. Check yourself

1. Why do row-level rules quarantine but never block, while gates block?
2. Name the quality dimensions and one check for each.
3. What is write-audit-publish?
4. How do you measure a data quality check? What are precision and recall here?
5. Why exclude `parent_quarantined` rows from precision?
6. How would you avoid alert fatigue?
7. What would you use in production instead of a hand-written framework, and what would you still need to decide yourself?

## 6. Learning material

**O'Reilly**
- *Data Quality Fundamentals* (Moses, Gavish, Vorwerck): ch. 1-4 (dimensions, testing, monitoring, data downtime).
- *Data Engineering Design Patterns* (Konieczny, 2025): the data quality chapter (write-audit-publish, dead-letter / quarantine patterns).

**Zoomcamp**
- [Module 04-analytics-engineering](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/04-analytics-engineering): dbt tests are the same idea, declared in YAML.
- [Module 05-data-platforms](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/05-data-platforms): ingestion, transformation and quality in one platform.

## 7. Done when

- [ ] `run` succeeds on synthetic and Olist; you have read both reports
- [ ] you can explain every blocking gate and why the warnings do not block
- [ ] the numbers are in `RESULTS.md`
- [ ] your Step 5 story is in [`STORIES.md`](../STORIES.md)
