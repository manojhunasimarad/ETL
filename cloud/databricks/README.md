# Databricks (step 10)

`ecom_pipeline.py` is a notebook in Databricks' source format: plain Python
with `# COMMAND ----------` between cells and `# MAGIC` before Markdown and
`%pip` cells. Databricks shows it as a notebook when imported.

## Run it on Free Edition

1. Open your Free Edition workspace.
2. Workspace → your folder → ⋮ → Import → File → `ecom_pipeline.py`.
3. Connect to **serverless** compute, then **Run all**.

The notebook:

1. installs this repository with `%pip` (only the base package: Databricks
   already has Spark, and installing PySpark over it would break the runtime);
2. creates `<catalog>.<schema>` and a volume `files` in it;
3. sets `ECOM_NO_CACHE=1` (serverless refuses `persist()`) and writes reports
   into the volume;
4. calls `ecom_etl.spark.databricks.run_batch`, the same bronze, silver,
   quality gates and gold code as everywhere else, and publishes the gold
   tables as `<catalog>.<schema>.<table>`;
5. queries monthly revenue growth on the published fact table.

## If something fails

| Message | Cause | Fix |
|---|---|---|
| `PERMISSION_DENIED` creating the schema or volume | the catalog belongs to someone else | set the `catalog` widget to one you own |
| `NOT_SUPPORTED_WITH_SERVERLESS ... PERSIST` | `ECOM_NO_CACHE` not set before the import | rerun from the cell that sets it, after `restartPython` |
| `ModuleNotFoundError: ecom_etl` | the `%pip` cell did not run, or Python restarted without it | run the first two cells again |
| `blocked_by_quality_gate` | a blocking check failed | read `/Volumes/<catalog>/<schema>/files/lake/synthetic/_reports/` |

## Record the result

Copy the printed `seconds` and `bronze records` into `docs/RESULTS.md`
with the date and "Databricks Free Edition, serverless".
