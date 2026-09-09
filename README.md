# Marketplace Delivery & Supply-Demand Analytics Platform

Canonical analytics layer for a three-sided marketplace (consumers, couriers/Dashers, merchants): order → assign → pick → deliver events into governed metrics for **on-time delivery**, **Dasher utilization**, and **undersupply rate**.

> **Status: ~70% complete.** Local lakehouse (Parquet on S3-style layout), Spark-style ingest, Trino/DuckDB serving, dbt marts, Great Expectations–style quality gates, and Tableau/Sigma-ready KPI tables all run end-to-end. Production Trino/Spark cluster wiring, live Sigma workbooks, and near-real-time undersupply alerts are the remaining ~30%.

## Why this exists

Marketplace ops need a single source of truth for supply/demand balance. This project models fulfillment events into **canonical datasets** and surfaces signature metrics so ops can rebalance local courier supply before late-order share spikes.

## Architecture (current)

```
raw CSV events  →  Spark-style ingest (Parquet)  →  dbt staging → marts
                                                      │
                                                      ├─ fct_orders / fct_courier_shifts
                                                      ├─ mart_supply_demand_hourly
                                                      └─ quality gates + KPI export
```

## Quickstart

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
pip install -r requirements.txt
python data/generate_marketplace_events.py
python spark/ingest_to_lake.py
dbt build --profiles-dir .
python quality/run_checks.py
python run_demo.py
```

## What's done (~70%)

| Component | Status |
|-----------|--------|
| Synthetic order / courier / merchant event generator | Done |
| Lake ingest to partitioned Parquet | Done |
| dbt staging → intermediate → marts | Done |
| Signature metrics: Dasher utilization, undersupply rate, OTD | Done |
| Data quality checks (nulls, uniqueness, freshness, ranges) | Done |
| KPI export for Tableau / Sigma | Done |
| Local demo + docs | Done |

## Remaining ~30% (future scope)

See [docs/ROADMAP.md](docs/ROADMAP.md).

## Stack

Python, SQL, dbt, DuckDB (local stand-in for Trino), Parquet / S3-layout lake, Spark-style batch ingest, Great Expectations–style checks.
