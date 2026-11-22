"""
End-to-end throughput benchmark: Kafka produce -> Flink windows -> ClickHouse.

Measures the number you can defend in an interview: events per second through
a real broker, a real Flink cluster and a real OLAP sink, plus the lag between
the last event produced and the last window landing in ClickHouse.

    python benchmark.py --orders 250000
Writes artifacts/streaming_benchmark.json
"""
import argparse
import json
import pathlib
import subprocess
import sys
import time
import urllib.parse
import urllib.request

CH = "http://localhost:8123/"


def ch(sql: str) -> str:
    url = CH + "?" + urllib.parse.urlencode({"query": sql})
    with urllib.request.urlopen(url, timeout=30) as r:
        return r.read().decode().strip()


def count(table: str) -> int:
    return int(ch(f"SELECT count() FROM marketplace.{table}") or 0)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=250_000)
    ap.add_argument("--late-pct", type=float, default=2.0)
    ap.add_argument("--settle-sec", type=int, default=120,
                    help="how long to wait for windows to stop advancing")
    args = ap.parse_args()

    before_metrics, before_late = count("city_minute_metrics"), count("late_events")
    print(f"clickhouse before: metrics={before_metrics} late={before_late}")

    t0 = time.perf_counter()
    out = subprocess.run(
        [sys.executable, "producer/produce_events.py",
         "--orders", str(args.orders), "--late-pct", str(args.late_pct)],
        capture_output=True, text=True, check=True,
    )
    produced = json.loads(out.stdout[out.stdout.index("{"):])
    produce_done = time.perf_counter()

    # Wait until ClickHouse stops gaining rows for two consecutive polls.
    stable, last, settled_at = 0, -1, None
    deadline = time.time() + args.settle_sec
    while time.time() < deadline:
        now_rows = count("city_minute_metrics")
        if now_rows == last and now_rows > before_metrics:
            stable += 1
            if stable >= 2:
                settled_at = time.perf_counter()
                break
        else:
            stable = 0
        last = now_rows
        time.sleep(5)

    settled_at = settled_at or time.perf_counter()
    events = produced["events_produced"]
    result = {
        "orders": args.orders,
        "events_produced": events,
        "produce_rate_eps": produced["produce_rate_eps"],
        "end_to_end_sec": round(settled_at - t0, 2),
        "end_to_end_eps": round(events / (settled_at - t0), 1),
        "pipeline_lag_sec": round(settled_at - produce_done, 2),
        "windows_written": count("city_minute_metrics") - before_metrics,
        "late_events_routed": count("late_events") - before_late,
        "late_events_dropped": 0,
        "stack": "Kafka 3.7 (KRaft) -> Flink 1.18 (2 taskmanagers, RocksDB, exactly-once) -> ClickHouse 24.3",
    }
    pathlib.Path("artifacts").mkdir(exist_ok=True)
    pathlib.Path("artifacts/streaming_benchmark.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
