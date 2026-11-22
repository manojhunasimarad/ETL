"""Step 9: order events, as the shop's order service would publish them.

The batch pipeline receives *snapshots* (CSV extracts). A streaming
pipeline receives *events*: one message per thing that happened, as it
happens. ``build_order_events`` turns the source files into
``order_created`` events in event-time order; ``produce`` sends them to
Kafka and injects the problems real streams have:

* duplicates  - at-least-once delivery: the same event arrives twice
* late events - an event arrives after newer ones (network, retries)
* malformed   - a truncated message that is not valid JSON
"""

from __future__ import annotations

import csv
import json
import random
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .contracts import ORDER_ITEMS, ORDERS, TIMESTAMP_FORMAT

DEFAULT_TOPIC = "orders"


def _num(text: str) -> Optional[float]:
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _ts(text: str) -> Optional[str]:
    try:
        return datetime.strptime(text.strip(), TIMESTAMP_FORMAT).strftime(TIMESTAMP_FORMAT)
    except (AttributeError, ValueError):
        return None


def build_order_events(source_dir: Path, limit: Optional[int] = None) -> List[dict]:
    """One ``order_created`` event per order that has valid lines, sorted by event time.

    Rows that the batch pipeline would quarantine (bad prices, missing ids)
    are left out: this stream is about *stream* problems, injected by ``produce``.
    """
    lines: Dict[str, List[dict]] = defaultdict(list)
    with (Path(source_dir) / ORDER_ITEMS.file_name).open(newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            price, freight = _num(r["price"]), _num(r["freight_value"])
            if price is None or freight is None or price <= 0 or freight < 0:
                lines[r["order_id"]].append({"invalid": True})
                continue
            lines[r["order_id"]].append({"product_id": r["product_id"], "seller_id": r["seller_id"],
                                         "price": round(price, 2), "freight_value": round(freight, 2)})
    events, seen = [], set()
    with (Path(source_dir) / ORDERS.file_name).open(newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            order_id, when = r["order_id"], _ts(r["order_purchase_timestamp"])
            items = lines.get(order_id)
            if not order_id or order_id in seen or not when or not items or not r["customer_id"]:
                continue
            if any(i.get("invalid") for i in items):
                continue
            seen.add(order_id)
            events.append({
                "event_id": f"{order_id}:created",
                "event_type": "order_created",
                "order_id": order_id,
                "customer_id": r["customer_id"],
                "order_status": r["order_status"].strip().lower(),
                "event_time": when,
                "order_total": round(sum(i["price"] + i["freight_value"] for i in items), 2),
                "items": items,
            })
    events.sort(key=lambda e: (e["event_time"], e["event_id"]))
    return events[:limit] if limit else events


@dataclass
class ProduceStats:
    topic: str
    events: int = 0
    messages_sent: int = 0
    duplicates: int = 0
    late: int = 0
    malformed: int = 0
    valid_unique_events: int = 0
    valid_total_value: float = 0.0


def plan_messages(events: List[dict], duplicate_rate: float = 0.0, late_rate: float = 0.0,
                  malformed_rate: float = 0.0, late_shift: int = 200, seed: int = 7):
    """Decide the exact sequence of messages (pure function, so it can be tested without Kafka)."""
    rng = random.Random(seed)
    ordered, held = [], []
    stats = {"late": 0}
    for i, e in enumerate(events):
        if late_rate and rng.random() < late_rate:
            held.append((i + late_shift, e))
            stats["late"] += 1
        else:
            ordered.append(e)
        while held and held[0][0] <= i:
            ordered.append(held.pop(0)[1])
    ordered += [e for _, e in held]

    messages = []  # (key, payload, is_valid)
    for e in ordered:
        payload = json.dumps(e, separators=(",", ":"))
        if malformed_rate and rng.random() < malformed_rate:
            messages.append((e["order_id"], payload[: len(payload) // 2], False))
            continue
        messages.append((e["order_id"], payload, True))
        if duplicate_rate and rng.random() < duplicate_rate:
            messages.append((e["order_id"], payload, True))
    return messages, stats["late"]


def produce(events: List[dict], bootstrap: str, topic: str = DEFAULT_TOPIC, rate: Optional[float] = None,
            duplicate_rate: float = 0.0, late_rate: float = 0.0, malformed_rate: float = 0.0,
            partitions: int = 3, seed: int = 7) -> ProduceStats:
    """Send the events to Kafka. ``rate``: messages per second (None = as fast as possible)."""
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic not in admin.list_topics(timeout=10).topics:
        for future in admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)]).values():
            future.result()

    messages, late = plan_messages(events, duplicate_rate, late_rate, malformed_rate, seed=seed)
    stats = ProduceStats(topic=topic, events=len(events), late=late)
    valid_ids, total = set(), 0.0
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20, "enable.idempotence": True})
    for key, payload, ok in messages:
        producer.produce(topic, key=key.encode(), value=payload.encode())
        producer.poll(0)
        stats.messages_sent += 1
        if not ok:
            stats.malformed += 1
        else:
            event = json.loads(payload)
            if event["event_id"] in valid_ids:
                stats.duplicates += 1
            else:
                valid_ids.add(event["event_id"])
                total += event["order_total"]
        if rate:
            time.sleep(1.0 / rate)
    producer.flush(30)
    stats.valid_unique_events = len(valid_ids)
    stats.valid_total_value = round(total, 2)
    return stats


def stats_dict(stats: ProduceStats) -> dict:
    return asdict(stats)
