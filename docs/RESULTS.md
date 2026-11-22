# Results

Measured numbers only. Each row says which command produced it, on what
data, on which machine and when. These are the numbers that go into the
resume bullet; if a row is "pending", the bullet keeps its `XX`.

| Metric | Value | Data | Command | Machine | Date |
|---|---|---|---|---|---|
| Olist source records (orders / customers / payments / order lines) | pending | Olist (Kaggle) | `python -m ecom_etl profile --source olist` | | |
| Synthetic records ingested (full pipeline) | 91,521 bronze records (20,000 orders) | synthetic, 1% defects | `run --source synthetic` | GitHub runner, 4 cores | 2026-11-22 |
| Synthetic records ingested (benchmark) | 915,177 bronze records (201,874 orders) | synthetic, 1% defects | `benchmark --repeat 2` | GitHub runner, 4 cores | 2026-11-22 |
| Number of automated data quality checks | 115 (80 row-level rules generated from the contract + 35 table-level gates) | any | `run` / `quality` | any | 2026-11-22 |
| Bad records flagged per run, Olist | pending (step 5) | Olist | | | |
| Bad records flagged per run, synthetic | 3,497 quarantined (1,762 injected defects + their dependent rows), 622 duplicates removed, 471 fixed; precision and recall 100% | 20,000 orders, 1% defects | `run --source synthetic` | GitHub runner | 2026-11-22 |
| Batch runtime (silver + gold), baseline | 176.9 s | 201,874 orders | `benchmark --repeat 2` | GitHub runner, 4 cores, Spark 4.1.4 | 2026-11-22 |
| Batch runtime (silver + gold), tuned | 33.0 s (5.4x faster): sized shuffle partitions + broadcast joins | 201,874 orders | `benchmark --repeat 2` | GitHub runner, 4 cores, Spark 4.1.4 | 2026-11-22 |
| Postgres point lookup with FK index | 12.5 ms → 0.013 ms | 201,874 orders | `benchmark` | GitHub runner | 2026-11-22 |
| Streaming: messages in, events out | 3,063 messages → 3,063 bronze, 32 quarantined, 2,968 silver; daily revenue equals the producer's total (414,831.77) | 3,000 synthetic order events with 63 duplicates, 34 late, 32 malformed | `produce --limit 3000` then `stream --reset` | GitHub runner, Kafka 4.1.2 | 2026-11-22 |

Numbers above were measured on GitHub Actions runners by this repository's
workflows. Re-run them on your own machine before an interview and add a
row: you should be able to say where each number came from.
