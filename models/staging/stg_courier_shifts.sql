SELECT
    shift_id,
    courier_id,
    city,
    CAST(shift_start AS TIMESTAMP) AS shift_start,
    shift_hours,
    active_delivery_hours,
    CAST(shift_hour AS TIMESTAMP) AS shift_hour
FROM read_parquet('../data/lake/courier_shifts/**/*.parquet', hive_partitioning=true)
