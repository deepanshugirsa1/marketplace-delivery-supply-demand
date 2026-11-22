"""
Replays marketplace delivery lifecycle events into a real Kafka topic.

Deliberately emits a configurable share of late events so the Flink job's
watermark and late-routing path is exercised, not just the happy path.

    python produce_events.py --orders 200000 --cities 12 --late-pct 2
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

CITIES = [
    "san_francisco", "oakland", "san_jose", "los_angeles", "san_diego",
    "phoenix", "denver", "austin", "chicago", "atlanta", "boston", "seattle",
]


def iso(ts: datetime) -> str:
    return ts.replace(tzinfo=None).isoformat(timespec="milliseconds")


def order_events(now: datetime, late: bool):
    """One order's full lifecycle: placed -> assigned -> picked_up -> delivered."""
    order_id = str(uuid.uuid4())
    courier_id = f"c_{random.randint(1, 4000)}"
    city = random.choice(CITIES)
    placed = now - timedelta(seconds=random.randint(0, 120))
    promised = placed + timedelta(minutes=random.randint(25, 45))
    delivered = placed + timedelta(minutes=random.randint(18, 60))

    stamps = [
        ("placed", placed),
        ("assigned", placed + timedelta(seconds=random.randint(10, 90))),
        ("picked_up", placed + timedelta(minutes=random.randint(5, 20))),
        ("delivered", delivered),
    ]
    for event_type, ts in stamps:
        if late:
            # Simulates a courier device reconnecting after a dead zone.
            ts = ts - timedelta(seconds=random.randint(60, 600))
        yield {
            "event_id": str(uuid.uuid4()),
            "order_id": order_id,
            "courier_id": courier_id,
            "city": city,
            "event_type": event_type,
            "promised_at": iso(promised),
            "event_time": iso(ts),
        }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="marketplace.delivery_events")
    p.add_argument("--orders", type=int, default=100_000)
    p.add_argument("--late-pct", type=float, default=2.0)
    p.add_argument("--rate", type=int, default=0, help="events/sec cap; 0 = as fast as possible")
    args = p.parse_args()

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 20,
        "batch.size": 262_144,
        "compression.type": "lz4",
        "acks": "1",
    })

    now = datetime.now(timezone.utc)
    sent = 0
    started = time.perf_counter()

    for i in range(args.orders):
        late = random.random() * 100 < args.late_pct
        for ev in order_events(now, late):
            while True:
                try:
                    producer.produce(args.topic, key=ev["order_id"].encode(),
                                     value=json.dumps(ev).encode())
                    break
                except BufferError:
                    producer.poll(0.2)
            sent += 1
        if args.rate:
            expected = sent / args.rate
            drift = expected - (time.perf_counter() - started)
            if drift > 0:
                time.sleep(drift)
        if i % 5_000 == 0:
            producer.poll(0)

    producer.flush()
    elapsed = time.perf_counter() - started
    print(json.dumps({
        "events_produced": sent,
        "orders": args.orders,
        "elapsed_sec": round(elapsed, 2),
        "produce_rate_eps": round(sent / elapsed, 1),
    }, indent=2))


if __name__ == "__main__":
    main()
