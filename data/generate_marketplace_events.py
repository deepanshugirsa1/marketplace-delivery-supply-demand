"""Generate synthetic marketplace order, courier-shift, and merchant events.

Default SCALE=1 ≈ 50k orders / 8k shifts over 14 days (representative city×hour load).
Set SCALE=env int to multiply volumes for heavier local load tests.
"""
from __future__ import annotations

import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

OUT = Path(__file__).parent / "raw"
OUT.mkdir(parents=True, exist_ok=True)
random.seed(42)

CITIES = ["sf", "oak", "sj", "la", "sea"]
SCALE = max(1, int(os.getenv("SCALE", "1")))
N_ORDERS = 50_000 * SCALE
N_SHIFTS = 8_000 * SCALE
N_READINESS = 12_000 * SCALE
START = datetime(2025, 9, 1, 8, 0, 0)


def gen_orders() -> pd.DataFrame:
    rows = []
    for i in range(N_ORDERS):
        city = random.choice(CITIES)
        created = START + timedelta(minutes=random.randint(0, 14 * 24 * 60))
        promise_min = random.choice([25, 30, 35, 40, 45])
        courier_wait = random.randint(1, 25)
        trip_min = random.randint(8, 40)
        delivered = created + timedelta(minutes=courier_wait + trip_min)
        on_time = (delivered - created).total_seconds() / 60 <= promise_min
        rows.append(
            {
                "order_id": f"o{i}",
                "city": city,
                "merchant_id": f"m{random.randint(1, 200)}",
                "courier_id": f"c{random.randint(1, 3000)}",
                "created_at": created.isoformat(),
                "delivered_at": delivered.isoformat(),
                "promise_minutes": promise_min,
                "on_time": int(on_time),
                "order_hour": created.replace(minute=0, second=0).isoformat(),
            }
        )
    return pd.DataFrame(rows)


def gen_shifts() -> pd.DataFrame:
    rows = []
    for i in range(N_SHIFTS):
        city = random.choice(CITIES)
        start = START + timedelta(hours=random.randint(0, 14 * 24))
        hours = random.choice([2, 3, 4, 5, 6])
        active_hours = round(hours * random.uniform(0.45, 0.95), 2)
        rows.append(
            {
                "shift_id": f"s{i}",
                "courier_id": f"c{random.randint(1, 3000)}",
                "city": city,
                "shift_start": start.isoformat(),
                "shift_hours": hours,
                "active_delivery_hours": active_hours,
                "shift_hour": start.replace(minute=0, second=0).isoformat(),
            }
        )
    return pd.DataFrame(rows)


def gen_merchant_readiness() -> pd.DataFrame:
    rows = []
    for i in range(N_READINESS):
        city = random.choice(CITIES)
        ts = START + timedelta(hours=random.randint(0, 14 * 24))
        rows.append(
            {
                "readiness_id": f"r{i}",
                "merchant_id": f"m{random.randint(1, 200)}",
                "city": city,
                "observed_at": ts.isoformat(),
                "prep_ready": int(random.random() > 0.12),
                "hour_bucket": ts.replace(minute=0, second=0).isoformat(),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    orders = gen_orders()
    shifts = gen_shifts()
    readiness = gen_merchant_readiness()
    orders.to_csv(OUT / "orders.csv", index=False)
    shifts.to_csv(OUT / "courier_shifts.csv", index=False)
    readiness.to_csv(OUT / "merchant_readiness.csv", index=False)
    total = len(orders) + len(shifts) + len(readiness)
    print(
        f"Wrote orders={len(orders)}, shifts={len(shifts)}, readiness={len(readiness)} "
        f"(total_events={total}, SCALE={SCALE}) -> {OUT}"
    )


if __name__ == "__main__":
    main()
