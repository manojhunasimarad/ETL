# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce ETL on Databricks (step 10)
# MAGIC
# MAGIC The same batch pipeline that runs on a laptop, in Docker and in CI, on Databricks
# MAGIC Free Edition (serverless compute, Unity Catalog):
# MAGIC
# MAGIC 1. install the project from GitHub (the transformations are plain Python functions);
# MAGIC 2. create a schema and a volume, and generate synthetic Olist-shaped data into the volume;
# MAGIC 3. bronze → silver (quarantine) → quality gates → gold (star schema) → gold gates;
# MAGIC 4. publish the gold tables as `<catalog>.<schema>.<table>` and query them.
# MAGIC
# MAGIC Import this file into your workspace (Workspace → Import → File), attach it to
# MAGIC serverless compute and run all cells. Record the timings in `docs/RESULTS.md`.

# COMMAND ----------

# MAGIC %pip install "git+https://github.com/manojhunasimarad/ETL.git"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "ecom")
dbutils.widgets.text("orders", "20000")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
orders = int(dbutils.widgets.get("orders"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.files")
root = f"/Volumes/{catalog}/{schema}/files"
print("Files go to", root)

# COMMAND ----------

import os

# Serverless compute refuses DataFrame.persist(); the pipeline then recomputes instead of caching.
os.environ["ECOM_NO_CACHE"] = "1"
# Reports next to the data, not inside the installed package.
os.environ["ECOM_ETL_REPORTS_DIR"] = root + "/reports"

from ecom_etl.spark.databricks import run_batch  # noqa: E402  (after the environment is set)

result = run_batch(spark, root, orders=orders, publish_to=f"{catalog}.{schema}")
print("status:", result["status"])
print("bronze records:", f"{result['bronze_records']:,}")
print("checks:", result["checks"])
print("seconds:", result["seconds"])
result["published"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the star schema
# MAGIC
# MAGIC Monthly delivered revenue and its growth, the same question as `sql/analysis/q01`.

# COMMAND ----------

display(spark.sql(f"""
    WITH m AS (
        SELECT date_trunc('MONTH', purchased_at) AS month, SUM(line_total) AS revenue
        FROM {catalog}.{schema}.fact_order_line
        WHERE order_status = 'delivered'
        GROUP BY 1
    )
    SELECT month, ROUND(revenue, 2) AS revenue,
           ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY month))
                 / NULLIF(LAG(revenue) OVER (ORDER BY month), 0), 1) AS growth_pct
    FROM m
    ORDER BY month
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to look at
# MAGIC
# MAGIC * The Spark UI of the gold step (in the query profile of each cell): how many tasks, how much shuffle.
# MAGIC * `{root}/lake/synthetic/_reports/`: the silver and quality reports, the same JSON as on your laptop.
# MAGIC * Catalog Explorer: the gold tables with their columns and history.
