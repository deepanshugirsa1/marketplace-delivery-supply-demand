SELECT
    city,
    shift_hour AS hour_bucket,
    SUM(shift_hours) AS offered_hours,
    SUM(active_delivery_hours) AS active_hours,
    COUNT(DISTINCT courier_id) AS couriers_on_shift
FROM {{ ref('stg_courier_shifts') }}
GROUP BY 1, 2
