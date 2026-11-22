"""Step 9: Spark Structured Streaming from Kafka.

Four queries read the ``orders`` topic, each with its own checkpoint:

    bronze      every Kafka record as-is (value + topic/partition/offset)   -> Parquet
    quarantine  records that are not valid JSON events, with a reason       -> Parquet
    silver      valid events, deduplicated by event_id within 7 days        -> Parquet
    daily       revenue per day (event time), 3-day watermark, upserted     -> Postgres

Key ideas (see docs/steps/STEP-09-streaming.md):
* **event time vs processing time**: windows use when the order happened,
  not when Spark saw the message.
* **watermark**: how late an event may arrive and still be counted; it also
  bounds the state Spark keeps for deduplication and windows.
* **exactly-once results from at-least-once input**: Kafka offsets are in
  the checkpoint, duplicates are dropped by event_id, the Parquet sink is
  transactional and the Postgres upsert is idempotent per window.
"""

from __future__ import annotations

import json
import time
from typing import Dict, Optional

from ..db import connect, schema_name
from .lake import Lake

DEDUPE_HORIZON = "7 days"   # how long an event_id is remembered to drop its duplicates
WINDOW_WATERMARK = "3 days"  # how late an event may be and still count in its day
WINDOW = "1 day"


def kafka_package() -> str:
    import pyspark

    return f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"


def event_schema():
    from pyspark.sql import types as T

    item = T.StructType([
        T.StructField("product_id", T.StringType()),
        T.StructField("seller_id", T.StringType()),
        T.StructField("price", T.DecimalType(12, 2)),
        T.StructField("freight_value", T.DecimalType(12, 2)),
    ])
    return T.StructType([
        T.StructField("event_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("order_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("order_status", T.StringType()),
        T.StructField("event_time", T.TimestampType()),
        T.StructField("order_total", T.DecimalType(12, 2)),
        T.StructField("items", T.ArrayType(item)),
    ])


def read_kafka(spark, bootstrap: str, topic: str, max_offsets: Optional[int]):
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", bootstrap)
              .option("subscribe", topic)
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "false"))
    if max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets)
    return reader.load()


def parse(raw):
    from pyspark.sql import functions as F

    value = F.col("value").cast("string")
    parsed = raw.select(
        value.alias("value"),
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(value, event_schema(), {"timestampFormat": "yyyy-MM-dd HH:mm:ss"}).alias("e"),
    )
    reason = (F.when(F.col("e").isNull() | F.col("e.event_id").isNull() | F.col("e.order_id").isNull()
                     | F.col("e.event_time").isNull(), F.lit("malformed_or_missing_fields"))
              .when(F.col("e.order_total") < 0, F.lit("negative_total")))
    return parsed.withColumn("reason", reason)


def _upsert_daily(source: str):
    """foreachBatch sink: idempotent upsert of the updated windows into Postgres."""
    table = f"{schema_name(source, 'stream')}.daily_revenue"

    def write(batch_df, batch_id: int) -> None:
        from pyspark.sql import functions as F

        # timestamps as UTC text: collecting them as Python datetimes would
        # convert them to the local time zone of the Python process
        rows = batch_df.select(
            F.date_format("window_start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
            F.date_format("window_end", "yyyy-MM-dd HH:mm:ss").alias("window_end"),
            "orders", "revenue").collect()
        if not rows:
            return
        with connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    f"""INSERT INTO {table} (window_start, window_end, orders, revenue, batch_id, updated_at)
                        VALUES (%s, %s, %s, %s, %s, now())
                        ON CONFLICT (window_start) DO UPDATE
                        SET orders = EXCLUDED.orders, revenue = EXCLUDED.revenue,
                            batch_id = EXCLUDED.batch_id, updated_at = now()""",
                    [(r["window_start"], r["window_end"], r["orders"], r["revenue"], batch_id) for r in rows])
            conn.commit()

    return write


def prepare_postgres(source: str, reset: bool = False) -> None:
    schema = schema_name(source, "stream")
    with connect() as conn:
        if reset:
            conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.daily_revenue (
            window_start TIMESTAMP PRIMARY KEY, window_end TIMESTAMP NOT NULL,
            orders BIGINT NOT NULL, revenue NUMERIC(14, 2) NOT NULL,
            batch_id BIGINT, updated_at TIMESTAMPTZ)""")
        conn.commit()


def start_queries(spark, lake: Lake, source: str, bootstrap: str, topic: str,
                  available_now: bool = True, max_offsets: Optional[int] = None,
                  processing_time: str = "10 seconds") -> Dict[str, object]:
    from pyspark.sql import functions as F

    def sink(df, name, fmt="parquet", mode="append"):
        writer = (df.writeStream.queryName(f"{source}_{name}").outputMode(mode)
                  .option("checkpointLocation", str(lake.root / "_checkpoints" / "stream" / name)))
        writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime=processing_time)
        if fmt == "parquet":
            return writer.format("parquet").start(str(lake.root / "stream" / name))
        return writer.foreachBatch(fmt).start()

    def events():
        return parse(read_kafka(spark, bootstrap, topic, max_offsets))

    queries = {}
    queries["bronze"] = sink(read_kafka(spark, bootstrap, topic, max_offsets).select(
        F.col("key").cast("string").alias("key"), F.col("value").cast("string").alias("value"),
        "topic", "partition", "offset", F.col("timestamp").alias("kafka_timestamp")), "bronze_order_events")

    queries["quarantine"] = sink(events().filter(F.col("reason").isNotNull())
                                 .select("value", "reason", "topic", "partition", "offset", "kafka_timestamp"),
                                 "quarantine_order_events")

    valid = events().filter(F.col("reason").isNull()).select("e.*", "topic", "partition", "offset", "kafka_timestamp")
    queries["silver"] = sink(valid.withWatermark("event_time", DEDUPE_HORIZON)
                             .dropDuplicatesWithinWatermark(["event_id"])
                             .withColumn("item_count", F.size("items")),
                             "silver_order_events")

    daily = (valid.withWatermark("event_time", WINDOW_WATERMARK)
             .dropDuplicatesWithinWatermark(["event_id"])
             .groupBy(F.window("event_time", WINDOW))
             .agg(F.count("*").alias("orders"), F.sum("order_total").alias("revenue"))
             .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                     "orders", "revenue"))
    prepare_postgres(source)
    queries["daily"] = sink(daily, "daily_revenue", fmt=_upsert_daily(source), mode="update")
    return queries


def _as_dict(progress) -> dict:
    """PySpark 4 returns progress objects; older versions returned dicts."""
    if isinstance(progress, dict):
        return progress
    return json.loads(progress.json)


def run_streaming(spark, lake: Lake, source: str, bootstrap: str, topic: str,
                  max_offsets: Optional[int] = None, reset: bool = False) -> dict:
    """Process everything currently in the topic (trigger availableNow) and report."""
    import shutil

    if reset:
        shutil.rmtree(lake.root / "_checkpoints" / "stream", ignore_errors=True)
        shutil.rmtree(lake.root / "stream", ignore_errors=True)
        prepare_postgres(source, reset=True)
    t0 = time.perf_counter()
    queries = start_queries(spark, lake, source, bootstrap, topic, available_now=True, max_offsets=max_offsets)
    for q in queries.values():
        q.awaitTermination()
    seconds = round(time.perf_counter() - t0, 2)

    def progress(q):
        batches = [_as_dict(p) for p in q.recentProgress]
        dropped = sum(op.get("numRowsDroppedByWatermark", 0)
                      for p in batches for op in p.get("stateOperators", []))
        return {"batches": len(batches), "input_rows": sum(p.get("numInputRows", 0) for p in batches),
                "dropped_by_watermark": dropped}

    def rows(name):
        path = lake.root / "stream" / name
        return spark.read.parquet(str(path)).count() if any(path.rglob("*.parquet")) else 0

    with connect() as conn:
        windows, revenue = conn.execute(
            f"SELECT count(*), coalesce(sum(revenue), 0) FROM {schema_name(source, 'stream')}.daily_revenue").fetchone()
    return {
        "seconds": seconds,
        "queries": {name: progress(q) for name, q in queries.items()},
        "bronze_records": rows("bronze_order_events"),
        "quarantined": rows("quarantine_order_events"),
        "silver_events": rows("silver_order_events"),
        "daily_windows": windows,
        "daily_revenue": float(revenue),
    }


def run_continuous(spark, lake: Lake, source: str, bootstrap: str, topic: str,
                   processing_time: str = "10 seconds") -> None:
    """Keep processing new messages until interrupted (Ctrl+C)."""
    queries = start_queries(spark, lake, source, bootstrap, topic, available_now=False,
                            max_offsets=None, processing_time=processing_time)
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        for q in queries.values():
            q.stop()
