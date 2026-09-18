# Roadmap — future production scope

The **local portfolio is complete**. Items below are optional production hardening, not missing demo work.

## Done (portfolio complete)

- [x] Synthetic marketplace events (orders, courier shifts, merchant readiness)
- [x] Partitioned Parquet lake layout (`data/lake/…`)
- [x] dbt models: staging → intermediate → marts
- [x] Signature metrics: `dasher_utilization`, `undersupply_rate`, `on_time_delivery_rate`
- [x] Quality gates (null, unique, freshness, metric bounds)
- [x] KPI CSV export for BI tools
- [x] Streamlit ops dashboard
- [x] GitHub Actions CI
- [x] Metric dictionary

## Optional production hardening

- [ ] Wire production Spark jobs (EMR / Databricks) replacing the local ingest stub
- [ ] Deploy serving layer on Trino over S3 (replace DuckDB profiles)
- [ ] Publish live Tableau + Sigma workbooks bound to `mart_supply_demand_hourly`
- [ ] Add Airflow DAG with freshness SLAs and failure routing
- [ ] Near-real-time undersupply alerts (Kafka / Flink → ops Slack)
- [ ] City × hour experimentation guardrails for rebalancing pilots
