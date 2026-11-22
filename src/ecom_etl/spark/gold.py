"""Gold: a star schema for analytics, built from silver.

    dim_date        one row per calendar day           date_key = yyyymmdd
    dim_customer    one row per customer *version*     SCD Type 2 on location
    dim_product     one row per product
    dim_seller      one row per seller
    fact_order_line one row per order line             <- the grain
    fact_payment    one row per payment                (different grain)
    agg_daily_sales one row per day                    pre-aggregated mart

Design decisions worth explaining in an interview:

* **Grain first.** ``fact_order_line`` has exactly one row per
  (order_id, order_item_id). Payments have another grain, so they get
  their own fact table; joining them to order lines would multiply revenue.
* **Surrogate keys** are ``xxhash64`` of the natural key: deterministic, so
  a re-run produces the same keys (idempotent), no sequence needed.
* **Unknown member** (key -1) in every dimension: a fact whose dimension
  row is missing points to -1 instead of disappearing from inner joins.
  Step 5 checks that no fact actually uses it.
* **SCD Type 2** for customers: when a person moves, a new version starts;
  old orders keep pointing at the address that was true when they were
  placed (point-in-time join on ``effective_from <= purchased_at < effective_to``).
"""

from __future__ import annotations

from typing import Dict

from .compat import persist, unpersist
from .lake import Lake
from .silver import read_layer

UNKNOWN_KEY = -1
FAR_FUTURE = "9999-12-31 00:00:00"
CUSTOMER_ATTRIBUTES = ("customer_zip_code_prefix", "customer_city", "customer_state")
GOLD_TABLES = ("dim_date", "dim_customer", "dim_product", "dim_seller",
               "fact_order_line", "fact_payment", "agg_daily_sales")


def _with_unknown_member(df, key: str):
    """Append the -1 'unknown' row: strings 'unknown', everything else NULL."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    values = []
    for field in df.schema.fields:
        if field.name == key:
            values.append(F.lit(UNKNOWN_KEY).cast("long").alias(field.name))
        elif isinstance(field.dataType, T.StringType):
            values.append(F.lit("unknown").alias(field.name))
        else:
            values.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.unionByName(df.sparkSession.range(1).select(*values))


def _date_key(col):
    from pyspark.sql import functions as F

    return F.date_format(col, "yyyyMMdd").cast("int")


def build_dim_date(orders):
    from pyspark.sql import functions as F

    bounds = orders.select(
        F.min(F.to_date("order_purchase_timestamp")).alias("lo"),
        F.max(F.greatest(F.to_date("order_purchase_timestamp"),
                         F.to_date("order_estimated_delivery_date"),
                         F.to_date("order_delivered_customer_date"))).alias("hi"),
    ).first()
    if bounds["lo"] is None:
        raise ValueError("silver orders is empty; nothing to build")
    dates = orders.sparkSession.sql(
        f"SELECT explode(sequence(DATE'{bounds['lo']}', DATE'{bounds['hi']}', INTERVAL 1 DAY)) AS date")
    return dates.select(
        _date_key("date").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofmonth("date").alias("day_of_month"),
        ((F.dayofweek("date") + 5) % 7 + 1).alias("day_of_week"),  # ISO: Monday = 1
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def build_dim_customer(customers, orders):
    """SCD Type 2: one version per period in which a person's location did not change."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    observations = (orders.select("customer_id", F.col("order_purchase_timestamp").alias("observed_at"))
                    .join(customers.select("customer_id", "customer_unique_id", *CUSTOMER_ATTRIBUTES),
                          "customer_id"))
    by_time = Window.partitionBy("customer_unique_id").orderBy("observed_at", "customer_id")
    location = F.xxhash64(*[F.coalesce(F.col(a), F.lit("<null>")) for a in CUSTOMER_ATTRIBUTES])
    versions = (observations
                .withColumn("_loc", location)
                .withColumn("_new", (F.lag("_loc").over(by_time).isNull()
                                     | (F.lag("_loc").over(by_time) != F.col("_loc"))).cast("int"))
                .withColumn("version", F.sum("_new").over(by_time.rowsBetween(Window.unboundedPreceding, 0)))
                .groupBy("customer_unique_id", "version")
                .agg(F.min("observed_at").alias("effective_from"),
                     *[F.first(a).alias(a) for a in CUSTOMER_ATTRIBUTES]))
    next_version = Window.partitionBy("customer_unique_id").orderBy("version")
    nxt = F.lead("effective_from").over(next_version)
    dim = versions.select(
        F.xxhash64("customer_unique_id", "version").alias("customer_sk"),
        "customer_unique_id",
        F.col("customer_zip_code_prefix").alias("zip_code_prefix"),
        F.col("customer_city").alias("city"),
        F.col("customer_state").alias("state"),
        "version",
        "effective_from",
        F.coalesce(nxt, F.to_timestamp(F.lit(FAR_FUTURE))).alias("effective_to"),
        nxt.isNull().alias("is_current"),
    )
    return _with_unknown_member(dim, "customer_sk")


def build_dim_product(products, translations):
    from pyspark.sql import functions as F

    t = translations.select("product_category_name", "product_category_name_english", "is_translated")
    dim = products.join(t, "product_category_name", "left").select(
        F.xxhash64("product_id").alias("product_sk"),
        "product_id",
        F.coalesce("product_category_name", F.lit("unknown")).alias("category_name"),
        F.coalesce("product_category_name_english", "product_category_name", F.lit("unknown"))
         .alias("category_name_english"),
        F.coalesce("is_translated", F.lit(False)).alias("is_translated"),
        F.col("product_photos_qty").alias("photos_qty"),
        F.col("product_weight_g").alias("weight_g"),
        (F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")).alias("volume_cm3"),
    )
    return _with_unknown_member(dim, "product_sk")


def build_dim_seller(sellers):
    from pyspark.sql import functions as F

    dim = sellers.select(
        F.xxhash64("seller_id").alias("seller_sk"),
        "seller_id",
        F.col("seller_zip_code_prefix").alias("zip_code_prefix"),
        F.col("seller_city").alias("city"),
        F.col("seller_state").alias("state"),
    )
    return _with_unknown_member(dim, "seller_sk")


def build_order_keys(orders, customers, dim_customer, bc):
    """Order-level attributes and keys, shared by both fact tables."""
    from pyspark.sql import functions as F

    o = orders.select(
        "order_id", "customer_id", "order_status",
        F.col("order_purchase_timestamp").alias("purchased_at"),
        F.col("order_delivered_customer_date").alias("delivered_at"),
        F.col("order_estimated_delivery_date").alias("estimated_delivery_at"),
    ).join(customers.select("customer_id", "customer_unique_id"), "customer_id")
    versions = dim_customer.filter(F.col("customer_sk") != UNKNOWN_KEY).select(
        F.col("customer_unique_id").alias("_uid"), "customer_sk", "effective_from", "effective_to")
    # point-in-time join: the customer version valid when the order was placed
    on = ((o["customer_unique_id"] == versions["_uid"])
          & (o["purchased_at"] >= versions["effective_from"])
          & (o["purchased_at"] < versions["effective_to"]))
    return (o.join(bc(versions), on, "left")
             .select("order_id", "order_status", "purchased_at", "delivered_at", "estimated_delivery_at",
                     F.coalesce("customer_sk", F.lit(UNKNOWN_KEY)).alias("customer_sk"),
                     _date_key("purchased_at").alias("date_key")))


def build_fact_order_line(items, order_keys, dim_product, dim_seller, bc):
    from pyspark.sql import functions as F

    p = dim_product.select("product_id", "product_sk")
    s = dim_seller.select("seller_id", "seller_sk")
    return (items.join(order_keys, "order_id")
            .join(bc(p), "product_id", "left")
            .join(bc(s), "seller_id", "left")
            .select(
                "order_id", "order_item_id", "date_key",
                _date_key("delivered_at").alias("delivered_date_key"),
                "customer_sk",
                F.coalesce("product_sk", F.lit(UNKNOWN_KEY)).alias("product_sk"),
                F.coalesce("seller_sk", F.lit(UNKNOWN_KEY)).alias("seller_sk"),
                "order_status",
                "price",
                "freight_value",
                (F.col("price") + F.col("freight_value")).alias("line_total"),
                "purchased_at", "delivered_at", "estimated_delivery_at",
                F.datediff("delivered_at", "purchased_at").alias("delivery_days"),
                F.col("delivered_at").isNotNull().alias("is_delivered"),
                F.coalesce(F.col("delivered_at") > F.col("estimated_delivery_at"), F.lit(False)).alias("is_late"),
                F.date_format("purchased_at", "yyyy-MM").alias("order_month"),
            ))


def build_fact_payment(payments, order_keys):
    return (payments.join(order_keys, "order_id")
            .select("order_id", "payment_sequential", "date_key", "customer_sk", "payment_type",
                    "payment_installments", "payment_value"))


def build_agg_daily_sales(fact):
    from pyspark.sql import functions as F

    sales = fact.filter(~F.col("order_status").isin("canceled", "unavailable"))
    return sales.groupBy("date_key").agg(
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("order_lines"),
        F.sum("price").alias("items_revenue"),
        F.sum("freight_value").alias("freight_revenue"),
        F.sum("line_total").alias("gross_revenue"),
        F.sum(F.col("is_late").cast("int")).alias("late_lines"),
        F.avg("delivery_days").alias("avg_delivery_days"),
    )


def run_gold(spark, lake: Lake, broadcast_dims: bool = True, partition_facts: bool = True) -> Dict[str, int]:
    """Build every gold table from silver. Returns row counts.

    ``broadcast_dims`` and ``partition_facts`` exist so step 7 can measure
    what each optimization is worth.
    """
    from pyspark.sql import functions as F

    def bc(df):
        return F.broadcast(df) if broadcast_dims else df

    silver = {t: read_layer(spark, lake, "silver", t) for t in
              ("customers", "sellers", "category_translation", "products", "orders",
               "order_items", "order_payments")}

    dims = {
        "dim_date": build_dim_date(silver["orders"]),
        "dim_customer": build_dim_customer(silver["customers"], silver["orders"]),
        "dim_product": build_dim_product(silver["products"], silver["category_translation"]),
        "dim_seller": build_dim_seller(silver["sellers"]),
    }
    for name, df in dims.items():
        df.write.mode("overwrite").parquet(lake.path("gold", name))
    dims = {name: spark.read.parquet(lake.path("gold", name)) for name in dims}

    order_keys = persist(build_order_keys(silver["orders"], silver["customers"], dims["dim_customer"], bc))
    fact = build_fact_order_line(silver["order_items"], order_keys, dims["dim_product"], dims["dim_seller"], bc)
    writer = fact.write.mode("overwrite")
    (writer.partitionBy("order_month") if partition_facts else writer).parquet(lake.path("gold", "fact_order_line"))
    build_fact_payment(silver["order_payments"], order_keys).write.mode("overwrite").parquet(
        lake.path("gold", "fact_payment"))
    unpersist(order_keys)

    fact = spark.read.parquet(lake.path("gold", "fact_order_line"))
    build_agg_daily_sales(fact).write.mode("overwrite").parquet(lake.path("gold", "agg_daily_sales"))
    return {t: spark.read.parquet(lake.path("gold", t)).count() for t in GOLD_TABLES}
