# Step 9: Streaming with Kafka and Spark Structured Streaming

**Goal:** process orders as a stream of events, as they happen, and get
correct numbers even though the stream delivers duplicates, late events
and broken messages.

**JD lines this answers:** "understanding of streaming data concepts"
(preferred); build pipelines to ingest data.

---

## 1. Concepts

### 1.1 Batch vs streaming

Batch (steps 3-8) processes a **snapshot**: "all orders as of the
extract". Streaming processes **events**: one message per thing that
happened, as it happens. The same business logic applies; what changes
is that the data never ends, arrives in an imperfect order, and the
system must remember things (state) between micro-batches.

### 1.2 Kafka in six terms

| Term | Meaning here |
|---|---|
| **Topic** | `orders`: an append-only log of order events |
| **Partition** | the topic is split in 3; order is guaranteed only *within* a partition |
| **Key** | `order_id`: all events of one order go to the same partition |
| **Offset** | position of a message in its partition; consumers track offsets |
| **Producer** | `python -m ecom_etl produce` (confluent-kafka) |
| **Consumer** | Spark Structured Streaming, which stores its offsets in a checkpoint |

This project runs Kafka 4 in **KRaft** mode: Kafka manages its own
metadata, with no ZooKeeper (removed in Kafka 4.0).

### 1.3 Delivery guarantees

* **At most once**: may lose messages. **At least once**: never loses,
  may duplicate. **Exactly once**: neither; expensive end to end.
* Real producers retry, so assume **at least once** and make the consumer
  handle duplicates. The producer here injects duplicates on purpose.
* Exactly-once *results* come from three things together: offsets stored
  in the checkpoint (restart where you stopped), deduplication by a
  business key (`event_id`), and **idempotent sinks** (the Parquet sink is
  transactional; the Postgres upsert writes the same row for the same day).

### 1.4 Event time, processing time and watermarks

* **Event time**: when the order happened (`event_time` in the message).
* **Processing time**: when Spark saw it.
* Daily revenue must use event time, or a network delay would move an
  order to the wrong day.
* A **watermark** says how late an event may arrive and still be counted:
  `withWatermark("event_time", "3 days")` means "once I have seen events
  up to time T, I will no longer accept events older than T minus 3 days".
  It also lets Spark **drop old state**: without a watermark, deduplication
  would remember every `event_id` forever.
* Too short a watermark drops valid late events; too long keeps more
  state and finalizes results later. Here the dedupe horizon is 7 days
  and the daily window watermark 3 days. Replayed history is spread over
  3 partitions that are read at slightly different speeds, which makes
  partitions differ by days of event time; real-time streams need far less.

### 1.5 The four streaming queries

| Query | Output | Mode |
|---|---|---|
| bronze | every Kafka record, value and offset (replay source) | append, Parquet |
| quarantine | messages that are not valid JSON events, with a reason | append, Parquet |
| silver | valid events, deduplicated by `event_id` within 7 days | append, Parquet |
| daily | revenue and orders per day (event time) | update, `foreachBatch` upsert into Postgres |

Each query has its own **checkpoint** (`data/lake/<source>/_checkpoints/stream/`).
Run `stream` again and each query continues from its stored offsets; nothing
is processed twice (there is a test for exactly that).

**Triggers**: `availableNow` processes everything in the topic and stops,
which suits CI and scheduled runs (it is batch-like, but with streaming
semantics and state). `--continuous` runs 10-second micro-batches until
you stop it.

### 1.6 What the producer injects

| Problem | How | Where it ends up |
|---|---|---|
| duplicates | same message sent twice (at-least-once) | dropped by `dropDuplicatesWithinWatermark` |
| malformed | truncated JSON | streaming quarantine, reason `malformed_or_missing_fields` |
| late events | held back and sent ~200 messages later | counted if within the watermark, otherwise dropped (reported as `dropped by watermark`) |

---

## 2. Code tour

1. [`events.py`](../../src/ecom_etl/events.py): `build_order_events` (snapshot → events), `plan_messages` (pure, tested without Kafka), `produce`.
2. [`spark/streaming.py`](../../src/ecom_etl/spark/streaming.py): `parse`, `start_queries` (the four queries), `_upsert_daily` (idempotent sink), `run_streaming`.
3. [`tests/integration/test_streaming.py`](../../tests/integration/test_streaming.py): every message in bronze, malformed messages in quarantine, duplicates gone, revenue counted once, rerun resumes from the checkpoint.

## 3. Run it

```bash
docker compose up -d postgres
docker compose --profile streaming up -d --wait kafka
docker compose run --rm pipeline produce --source synthetic --limit 5000
docker compose run --rm pipeline stream --source synthetic --reset
```

Live mode: in one terminal `docker compose run --rm pipeline stream --continuous`,
in another `docker compose run --rm pipeline produce --rate 20`, and watch
the dashboard's **Streaming** tab fill up.

## 4. Exercises

1. Produce with `--late-rate 0.05`, stream, and read `dropped by watermark`. Change `WINDOW_WATERMARK` to `"1 day"` and `"10 days"`. What changes, and what does each choice cost?
2. Stop `stream --continuous` with Ctrl+C in the middle of a run and start it again. Do the counts stay right? Why?
3. Send the same 1,000 events twice (`produce` twice). What does silver contain afterwards, and why?
4. Change the message key from `order_id` to a random value. What ordering guarantee do you lose?
5. Add a payments topic and a stream-stream join between orders and payments with watermarks on both sides.

## 5. Check yourself

1. Event time vs processing time: which do the daily windows use, and why?
2. What does a watermark do, and what two things does it trade off?
3. At-least-once input, exactly-once results: which three mechanisms make that work here?
4. Why is the Postgres sink written as an upsert keyed by the window?
5. What is a checkpoint, and what happens if you delete it?
6. Why are events keyed by `order_id`?
7. `availableNow` vs a processing-time trigger: when would you use each?

## 6. Learning material

**O'Reilly**
- *Kafka: The Definitive Guide, 2nd Ed.* (Shapira et al.): producers, consumers, partitions and delivery semantics.
- *Learning Spark, 2nd Ed.*, ch. 8 "Structured Streaming": triggers, checkpoints, watermarks, stateful operations.
- *Streaming Systems* (Akidau, Chernyak, Lax): the reference on event time, windows and watermarks.

**LinkedIn Learning**
- *Apache Kafka Essential Training: Getting Started*.

**Zoomcamp**
- [Module 07-streaming](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/07-streaming): Kafka/Redpanda, producers, consumers and stream processing.

## 7. Done when

- [ ] `produce` and `stream` run in Docker; the Streaming tab shows daily revenue
- [ ] you can explain watermarks and exactly-once results with this pipeline
- [ ] you have seen late events dropped and know how to change that
- [ ] your Step 9 story is in [`STORIES.md`](../STORIES.md)
