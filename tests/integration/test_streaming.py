"""Step 9: Kafka -> Spark Structured Streaming, checked against what was sent."""

import uuid

import pytest

pytestmark = [pytest.mark.spark, pytest.mark.kafka, pytest.mark.postgres]


@pytest.fixture(scope="module")
def streamed(spark, dirty_source, tmp_path_factory):
    from ecom_etl import config
    from ecom_etl.events import build_order_events, produce
    from ecom_etl.spark.lake import Lake
    from ecom_etl.spark.streaming import run_streaming

    out, _ = dirty_source
    topic = f"t_orders_{uuid.uuid4().hex[:8]}"
    stats = produce(build_order_events(out, limit=800), config.KAFKA_BOOTSTRAP, topic,
                    duplicate_rate=0.05, malformed_rate=0.02)
    lake = Lake(tmp_path_factory.mktemp("lake_stream"))
    report = run_streaming(spark, lake, "t_stream", config.KAFKA_BOOTSTRAP, topic, reset=True)
    return stats, report, lake, topic


def test_bronze_keeps_every_message(streamed):
    stats, report, *_ = streamed
    assert report["bronze_records"] == stats.messages_sent


def test_malformed_messages_are_quarantined(streamed):
    stats, report, *_ = streamed
    assert stats.malformed > 0
    assert report["quarantined"] == stats.malformed


def test_duplicates_are_dropped(streamed):
    stats, report, *_ = streamed
    assert stats.duplicates > 0
    assert report["silver_events"] == stats.valid_unique_events


def test_daily_revenue_counts_each_order_once(streamed):
    from ecom_etl.db import connect, schema_name

    stats, report, *_ = streamed
    assert report["daily_revenue"] == pytest.approx(stats.valid_total_value, abs=0.01)
    with connect() as conn:
        orders = conn.execute(f"SELECT sum(orders) FROM {schema_name('t_stream', 'stream')}.daily_revenue").fetchone()[0]
    assert orders == stats.valid_unique_events


def test_rerun_resumes_from_the_checkpoint(spark, streamed):
    """Nothing new in the topic: a second run reads nothing and changes nothing."""
    from ecom_etl import config
    from ecom_etl.spark.streaming import run_streaming

    stats, first, lake, topic = streamed
    second = run_streaming(spark, lake, "t_stream", config.KAFKA_BOOTSTRAP, topic)
    assert all(q["input_rows"] == 0 for q in second["queries"].values())
    for key in ("bronze_records", "quarantined", "silver_events", "daily_windows", "daily_revenue"):
        assert second[key] == first[key], key
