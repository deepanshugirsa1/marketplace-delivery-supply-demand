# Metric definitions — marketplace supply & demand

Grain for all signature metrics: **city × hour_bucket**.

## dasher_utilization

- **Formula:** `active_hours / offered_hours`
- **Sources:** courier shift events (`active_delivery_hours`, `shift_hours`)
- **Bounds:** expected in `[0, 1]`; soft upper bound `1.05` allowed for clock-skew / overlapping shift edges
- **Use:** ops staffing — low utilization → oversupply of Dashers; high utilization with rising late rate → need more supply

## undersupply_rate

- **Formula:** if `active_couriers = 0` and `orders > 0` → `1.0`; else if `orders / active_couriers > 3.0` → `min(1, (load - 3) / 3)`; else `0`
- **Target load:** 3.0 orders per active courier per hour (configurable constant in `mart_supply_demand_hourly.sql`)
- **Use:** city × hour undersupply signal for rebalancing pilots

## on_time_delivery_rate (OTD)

- **Formula:** `on_time_orders / orders` where on-time = delivered within `promise_minutes` of order create
- **Use:** primary customer experience guardrail when rebalancing supply

## Edge cases

- Hours with zero orders: OTD is `NULL` (not zero)
- Hours with zero offered hours: utilization is `NULL`
- Full outer join of order demand and courier capacity so one-sided hours still appear

## Export

`data/export/kpi_supply_demand_hourly.csv` is Tableau/Sigma-ready at city × hour grain.
