"""
Marketplace supply/demand streaming job.

Real Flink, real event time. Reads delivery lifecycle events from Kafka,
computes per-city one-minute supply/demand signals in keyed tumbling windows
with watermarks, and writes results back to Kafka for ClickHouse to ingest.

Late events beyond the watermark are routed to a side topic rather than
silently dropped -- that routing is the point of the exercise.

Submit:
    flink run -py /opt/jobs/supply_demand_stream.py
"""
import os

from pyflink.table import EnvironmentSettings, TableEnvironment

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "marketplace.delivery_events")
METRICS_TOPIC = os.getenv("METRICS_TOPIC", "marketplace.city_minute_metrics")
LATE_TOPIC = os.getenv("LATE_TOPIC", "marketplace.late_events")
# Target load per active courier. Mirrors the batch repo's 3.0 orders/courier.
TARGET_LOAD = float(os.getenv("TARGET_ORDERS_PER_COURIER", "3.0"))
# How late an event may arrive and still be counted in its own window.
ALLOWED_LATENESS_SEC = int(os.getenv("ALLOWED_LATENESS_SEC", "30"))


def build(t_env: TableEnvironment) -> None:
    t_env.execute_sql(f"""
        CREATE TABLE delivery_events (
            event_id      STRING,
            order_id      STRING,
            courier_id    STRING,
            city          STRING,
            event_type    STRING,   -- placed | assigned | picked_up | delivered
            promised_at   TIMESTAMP(3),
            event_time    TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '{ALLOWED_LATENESS_SEC}' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SOURCE_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id' = 'supply-demand-stream',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'json.ignore-parse-errors' = 'false'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE city_minute_metrics (
            window_start        TIMESTAMP(3),
            window_end          TIMESTAMP(3),
            city                STRING,
            orders_placed       BIGINT,
            active_couriers     BIGINT,
            deliveries          BIGINT,
            on_time_deliveries  BIGINT,
            courier_utilization DOUBLE,
            undersupply_rate    DOUBLE,
            on_time_rate        DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{METRICS_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # Events the watermark has already passed. Kept, not dropped: a courier
    # phone that reconnects after a dead zone lands here, and the backfill
    # job replays this topic into the batch marts.
    t_env.execute_sql(f"""
        CREATE TABLE late_events (
            event_id   STRING,
            order_id   STRING,
            city       STRING,
            event_type STRING,
            event_time TIMESTAMP(3),
            lateness_sec BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{LATE_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    stmt = t_env.create_statement_set()

    # Keyed tumbling window per city. One minute of event time, not wall clock,
    # so a replay of yesterday's events produces yesterday's numbers.
    stmt.add_insert_sql(f"""
        INSERT INTO city_minute_metrics
        SELECT
            window_start,
            window_end,
            city,
            COUNT(DISTINCT CASE WHEN event_type = 'placed'    THEN order_id END)  AS orders_placed,
            COUNT(DISTINCT CASE WHEN event_type = 'assigned'  THEN courier_id END) AS active_couriers,
            COUNT(DISTINCT CASE WHEN event_type = 'delivered' THEN order_id END)  AS deliveries,
            COUNT(DISTINCT CASE WHEN event_type = 'delivered'
                                 AND event_time <= promised_at THEN order_id END) AS on_time_deliveries,
            CAST(COUNT(DISTINCT CASE WHEN event_type = 'delivered' THEN order_id END) AS DOUBLE)
                / NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'assigned' THEN courier_id END), 0)
                AS courier_utilization,
            GREATEST(
                0.0,
                1.0 - (
                    CAST(COUNT(DISTINCT CASE WHEN event_type = 'assigned' THEN courier_id END) AS DOUBLE)
                    * {TARGET_LOAD}
                ) / NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'placed' THEN order_id END), 0)
            ) AS undersupply_rate,
            CAST(COUNT(DISTINCT CASE WHEN event_type = 'delivered'
                                      AND event_time <= promised_at THEN order_id END) AS DOUBLE)
                / NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'delivered' THEN order_id END), 0)
                AS on_time_rate
        FROM TABLE(
            TUMBLE(TABLE delivery_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
        )
        GROUP BY window_start, window_end, city
    """)

    stmt.add_insert_sql(f"""
        INSERT INTO late_events
        SELECT
            event_id, order_id, city, event_type, event_time,
            CAST(TIMESTAMPDIFF(SECOND, event_time, CURRENT_WATERMARK(event_time)) AS BIGINT)
        FROM delivery_events
        WHERE CURRENT_WATERMARK(event_time) IS NOT NULL
          AND event_time < CURRENT_WATERMARK(event_time)
    """)

    stmt.execute()


def main() -> None:
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    cfg = t_env.get_config()
    cfg.set("pipeline.name", "marketplace-supply-demand")
    # Checkpointing is what makes the Kafka offsets and the window state
    # recover together after a taskmanager dies.
    cfg.set("execution.checkpointing.interval", "30s")
    cfg.set("execution.checkpointing.mode", "EXACTLY_ONCE")
    cfg.set("execution.checkpointing.min-pause", "10s")
    cfg.set("state.backend.type", "rocksdb")
    cfg.set("table.exec.source.idle-timeout", "10s")
    build(t_env)


if __name__ == "__main__":
    main()
