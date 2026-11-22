"""Step 9: building order events and planning the messages (no Kafka needed)."""

import json

from ecom_etl.events import build_order_events, plan_messages


def test_events_are_valid_and_in_event_time_order(dirty_source):
    out, _ = dirty_source
    events = build_order_events(out)
    assert events
    times = [e["event_time"] for e in events]
    assert times == sorted(times)
    assert len({e["event_id"] for e in events}) == len(events)
    for e in events[:200]:
        assert e["order_total"] == round(sum(i["price"] + i["freight_value"] for i in e["items"]), 2)
        assert all(i["price"] > 0 for i in e["items"])


def test_plan_without_noise_sends_each_event_once(dirty_source):
    out, _ = dirty_source
    events = build_order_events(out, limit=300)
    messages, late = plan_messages(events)
    assert late == 0
    assert [json.loads(p)["event_id"] for _, p, ok in messages] == [e["event_id"] for e in events]


def test_plan_injects_duplicates_late_and_malformed_messages(dirty_source):
    out, _ = dirty_source
    events = build_order_events(out, limit=2000)
    messages, late = plan_messages(events, duplicate_rate=0.05, late_rate=0.02, malformed_rate=0.02)
    valid = [json.loads(p) for _, p, ok in messages if ok]
    malformed = [p for _, p, ok in messages if not ok]
    assert late > 0 and malformed and len(valid) > len({v["event_id"] for v in valid})  # duplicates
    for p in malformed:
        try:
            json.loads(p)
            raise AssertionError("malformed message parsed as JSON")
        except json.JSONDecodeError:
            pass
    # every event is either sent validly at least once or was the malformed copy
    assert len({v["event_id"] for v in valid}) + len(malformed) == len(events)
    # late events arrive after newer ones (a late event can also be the
    # malformed one, so not every late event shows up here)
    newest, behind = "", 0
    for v in valid:
        behind += v["event_time"] < newest
        newest = max(newest, v["event_time"])
    assert 0 < behind <= late + len(valid) - len({v["event_id"] for v in valid})
