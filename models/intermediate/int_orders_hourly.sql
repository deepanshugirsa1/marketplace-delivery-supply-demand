SELECT
    o.city,
    o.order_hour AS hour_bucket,
    COUNT(*) AS orders,
    SUM(o.on_time) AS on_time_orders,
    COUNT(DISTINCT o.courier_id) AS active_couriers
FROM {{ ref('stg_orders') }} o
GROUP BY 1, 2
