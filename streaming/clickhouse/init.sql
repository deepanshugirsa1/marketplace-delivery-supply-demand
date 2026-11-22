-- Real ClickHouse: Kafka engine consumes the Flink output topic, a materialized
-- view lands it in MergeTree. ReplacingMergeTree keyed on the window so a
-- replay of the same window overwrites rather than double-counts.
CREATE DATABASE IF NOT EXISTS marketplace;

CREATE TABLE IF NOT EXISTS marketplace.city_minute_metrics_queue
(
    window_start        DateTime64(3),
    window_end          DateTime64(3),
    city                String,
    orders_placed       UInt64,
    active_couriers     UInt64,
    deliveries          UInt64,
    on_time_deliveries  UInt64,
    courier_utilization Nullable(Float64),
    undersupply_rate    Nullable(Float64),
    on_time_rate        Nullable(Float64)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'marketplace.city_minute_metrics',
         kafka_group_name = 'clickhouse-city-metrics',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 2,
         date_time_input_format = 'best_effort';

CREATE TABLE IF NOT EXISTS marketplace.city_minute_metrics
(
    window_start        DateTime64(3),
    window_end          DateTime64(3),
    city                LowCardinality(String),
    orders_placed       UInt64,
    active_couriers     UInt64,
    deliveries          UInt64,
    on_time_deliveries  UInt64,
    courier_utilization Nullable(Float64),
    undersupply_rate    Nullable(Float64),
    on_time_rate        Nullable(Float64),
    ingested_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (city, window_start);

CREATE MATERIALIZED VIEW IF NOT EXISTS marketplace.city_minute_metrics_mv
TO marketplace.city_minute_metrics AS
SELECT window_start, window_end, city, orders_placed, active_couriers,
       deliveries, on_time_deliveries, courier_utilization,
       undersupply_rate, on_time_rate
FROM marketplace.city_minute_metrics_queue;

-- Late events are kept for the backfill path, never dropped.
CREATE TABLE IF NOT EXISTS marketplace.late_events_queue
(
    event_id String, order_id String, city String,
    event_type String, event_time DateTime64(3), lateness_sec Int64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'marketplace.late_events',
         kafka_group_name = 'clickhouse-late-events',
         kafka_format = 'JSONEachRow',
         date_time_input_format = 'best_effort';

CREATE TABLE IF NOT EXISTS marketplace.late_events
(
    event_id String, order_id String, city LowCardinality(String),
    event_type LowCardinality(String), event_time DateTime64(3),
    lateness_sec Int64, ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (city, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS marketplace.late_events_mv
TO marketplace.late_events AS
SELECT event_id, order_id, city, event_type, event_time, lateness_sec
FROM marketplace.late_events_queue;
