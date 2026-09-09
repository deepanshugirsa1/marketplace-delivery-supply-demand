WITH joined AS (
    SELECT
        COALESCE(o.city, c.city) AS city,
        COALESCE(o.hour_bucket, c.hour_bucket) AS hour_bucket,
        COALESCE(o.orders, 0) AS orders,
        COALESCE(o.on_time_orders, 0) AS on_time_orders,
        COALESCE(o.active_couriers, 0) AS active_couriers,
        COALESCE(c.offered_hours, 0) AS offered_hours,
        COALESCE(c.active_hours, 0) AS active_hours,
        COALESCE(c.couriers_on_shift, 0) AS couriers_on_shift
    FROM {{ ref('int_orders_hourly') }} o
    FULL OUTER JOIN {{ ref('int_courier_capacity_hourly') }} c
        ON o.city = c.city AND o.hour_bucket = c.hour_bucket
)
SELECT
    city,
    hour_bucket,
    orders,
    on_time_orders,
    CASE WHEN orders > 0 THEN ROUND(on_time_orders * 1.0 / orders, 4) ELSE NULL END AS on_time_delivery_rate,
    offered_hours,
    active_hours,
    CASE WHEN offered_hours > 0 THEN ROUND(active_hours / offered_hours, 4) ELSE NULL END AS dasher_utilization,
    -- Undersupply: orders per active courier above target of 3.0
    CASE
        WHEN active_couriers = 0 AND orders > 0 THEN 1.0
        WHEN active_couriers > 0 AND (orders * 1.0 / active_couriers) > 3.0
            THEN ROUND(LEAST(1.0, ((orders * 1.0 / active_couriers) - 3.0) / 3.0), 4)
        ELSE 0.0
    END AS undersupply_rate,
    active_couriers,
    couriers_on_shift
FROM joined
