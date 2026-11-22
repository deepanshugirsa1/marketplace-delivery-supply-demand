# Streaming layer — real Kafka, real Flink, real ClickHouse

This is the streaming half of the marketplace platform. Nothing here is simulated:
Kafka runs as a KRaft broker, Flink runs as a jobmanager plus two taskmanagers with
RocksDB state and exactly-once checkpointing, and ClickHouse ingests through its
Kafka table engine into a ReplacingMergeTree.

## What it computes

Delivery lifecycle events (`placed → assigned → picked_up → delivered`) are keyed by
city and aggregated in **one-minute event-time tumbling windows** into:

| Signal | Definition |
|---|---|
| `courier_utilization` | deliveries completed per active courier in the window |
| `undersupply_rate` | shortfall of courier capacity against offered load, at a target of 3.0 orders per active courier |
| `on_time_rate` | deliveries completed at or before the promised time |

These are the same definitions as the batch marts, so a streaming window and the
nightly mart for the same minute must agree. That equivalence is the test worth
running: if they disagree, one of the two definitions is wrong.

## Why event time, not processing time

A courier's phone loses signal in a parking garage and flushes its events four minutes
late. Under processing time those events land in the wrong minute and quietly corrupt
the undersupply signal for the city — the exact failure that makes an ops team stop
trusting a dashboard. The job declares a watermark with 30 seconds of allowed lateness;
anything later is **routed to a `late_events` topic and landed in ClickHouse**, not
dropped, and the backfill replays it into the batch marts.

## Run it

```bash
pip install -r requirements-streaming.txt
make up          # Kafka + Flink cluster + ClickHouse
make submit      # submit the PyFlink job
make produce     # replay 100k orders (~400k events)
make verify      # show the latest windows that landed
make bench       # end-to-end throughput -> artifacts/streaming_benchmark.json
```

Flink UI at http://localhost:8081, ClickHouse at http://localhost:8123/play.

## Failure behaviour

`make kill-taskmanager` kills a taskmanager mid-stream. The job restarts from the last
checkpoint, Kafka offsets and window state recover together, and `make verify` shows the
same window counts as before the kill — no double counting, no gap. Checkpoint interval
is 30s with `EXACTLY_ONCE` mode.

## Results

Measured by `make bench` on 250,000 orders. Raw output: `artifacts/streaming_benchmark.json`.

| Measure | Value |
|---|---|
| Events through the pipeline | 1,000,012 |
| End-to-end throughput (events/sec) | 18,859.9 |
| Lag from last event produced to last window landed | 20.07 sec |
| Late events routed (not dropped) | 20,456 routed, 0 dropped |

Stack for that run: Kafka 3.7 (KRaft) → Flink 1.18 (2 taskmanagers, RocksDB, exactly-once) → ClickHouse 24.3. Produce rate was 31,406.3 events/sec. The pipeline wrote 60 city-minute windows.

## Scope

Single-node brokers and a single ClickHouse node, sized for a laptop. Multi-broker
replication, a ClickHouse cluster, and Airflow orchestration of the batch marts are
documented as future scope, not incomplete core.
