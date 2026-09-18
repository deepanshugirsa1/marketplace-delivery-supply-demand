SELECT
    order_id,
    city,
    merchant_id,
    courier_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(delivered_at AS TIMESTAMP) AS delivered_at,
    promise_minutes,
    on_time,
    CAST(order_hour AS TIMESTAMP) AS order_hour
FROM read_parquet('data/lake/orders/**/*.parquet', hive_partitioning=true)
