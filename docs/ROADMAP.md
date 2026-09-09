# Roadmap — remaining ~30%

## Done (~70%)

- [x] Synthetic marketplace events (orders, courier shifts, merchant readiness)
- [x] Partitioned Parquet lake layout (`data/lake/…`)
- [x] dbt models: staging → intermediate → marts
- [x] Signature metrics: `dasher_utilization`, `undersupply_rate`, `on_time_delivery_rate`
- [x] Quality gates (null, unique, freshness, metric bounds)
- [x] KPI CSV/Parquet export for BI tools
- [x] Local `run_demo.py` walkthrough

## Phase next (70% → 85%)

- [ ] Wire production **Spark** jobs (EMR / Databricks) replacing the local ingest stub
- [ ] Deploy serving layer on **Trino** over **S3** (replace DuckDB profiles)
- [ ] Publish live **Tableau** + **Sigma** workbooks bound to `mart_supply_demand_hourly`
- [ ] Add Airflow DAG with freshness SLAs and failure routing

## Phase final (85% → 100%)

- [ ] Near-real-time undersupply alerts (Kafka / Flink → ops Slack / Pager)
- [ ] City × hour experimentation guardrails for rebalancing pilots
- [ ] GitHub Actions CI: `dbt build` + quality gates on every PR
- [ ] Lineage + metric dictionary auto-published to docs site
