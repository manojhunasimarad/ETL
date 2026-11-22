"""Step 4: the star schema."""

import pytest

pytestmark = pytest.mark.spark


def _read(spark, lake, layer, table):
    return spark.read.parquet(lake.path(layer, table))


def test_fact_grain_is_one_row_per_order_line(spark, dirty_lake):
    lake, silver, gold = dirty_lake
    fact = _read(spark, lake, "gold", "fact_order_line")
    assert gold["fact_order_line"] == silver["order_items"].silver_rows
    assert fact.select("order_id", "order_item_id").distinct().count() == gold["fact_order_line"]


def test_payment_fact_has_its_own_grain(spark, dirty_lake):
    lake, silver, gold = dirty_lake
    assert gold["fact_payment"] == silver["order_payments"].silver_rows


def test_no_fact_points_to_the_unknown_member(spark, dirty_lake):
    from pyspark.sql import functions as F

    lake, *_ = dirty_lake
    fact = _read(spark, lake, "gold", "fact_order_line")
    unknown = fact.filter((F.col("customer_sk") == -1) | (F.col("product_sk") == -1) | (F.col("seller_sk") == -1))
    assert unknown.count() == 0


def test_revenue_reconciles_with_silver(spark, dirty_lake):
    from pyspark.sql import functions as F

    lake, *_ = dirty_lake
    fact_total = _read(spark, lake, "gold", "fact_order_line").agg(F.sum("line_total")).first()[0]
    items = _read(spark, lake, "silver", "order_items")
    silver_total = items.agg(F.sum(F.col("price") + F.col("freight_value"))).first()[0]
    assert fact_total == silver_total


def test_scd2_versions_are_contiguous_with_one_current(spark, dirty_lake):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    lake, *_ = dirty_lake
    dim = _read(spark, lake, "gold", "dim_customer").filter(F.col("customer_sk") != -1)
    per_person = dim.groupBy("customer_unique_id").agg(
        F.count("*").alias("versions"), F.sum(F.col("is_current").cast("int")).alias("current"))
    assert per_person.filter(F.col("current") != 1).count() == 0
    assert per_person.filter(F.col("versions") > 1).count() >= 1, "expected some customers who moved"

    w = Window.partitionBy("customer_unique_id").orderBy("version")
    gaps = (dim.withColumn("next_from", F.lead("effective_from").over(w))
               .filter(F.col("next_from").isNotNull() & (F.col("next_from") != F.col("effective_to"))))
    assert gaps.count() == 0


def test_point_in_time_customer_join(spark, dirty_lake):
    """Every order line points at the customer version valid when it was purchased."""
    from pyspark.sql import functions as F

    lake, *_ = dirty_lake
    fact = _read(spark, lake, "gold", "fact_order_line")
    dim = _read(spark, lake, "gold", "dim_customer")
    joined = fact.join(dim, "customer_sk")
    valid = joined.filter((F.col("purchased_at") >= F.col("effective_from"))
                          & (F.col("purchased_at") < F.col("effective_to")))
    assert valid.count() == fact.count()


def test_dim_date_covers_all_fact_dates(spark, dirty_lake):
    lake, *_ = dirty_lake
    fact = _read(spark, lake, "gold", "fact_order_line")
    dates = _read(spark, lake, "gold", "dim_date").select("date_key")
    assert fact.select("date_key").join(dates, "date_key", "left_anti").count() == 0


@pytest.mark.postgres
def test_load_gold_into_postgres(spark, dirty_lake):
    from ecom_etl.db import connect, schema_name
    from ecom_etl.spark.warehouse import load_gold

    lake, _, gold = dirty_lake
    counts = load_gold(spark, lake, "t_gold")
    assert counts == gold
    schema = schema_name("t_gold", "gold")
    with connect() as conn:
        view_total = conn.execute(f"SELECT sum(line_total) FROM {schema}.v_order_lines").fetchone()[0]
        fact_total = conn.execute(f"SELECT sum(line_total) FROM {schema}.fact_order_line").fetchone()[0]
        pk = conn.execute(
            "SELECT count(*) FROM information_schema.table_constraints "
            "WHERE table_schema = %s AND constraint_type = 'PRIMARY KEY'", (schema,)).fetchone()[0]
    assert view_total == fact_total  # the view's inner joins lose no rows
    assert pk == 7
