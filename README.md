# Marketplace Delivery & Supply-Demand Analytics Platform

Canonical analytics layer for a three-sided marketplace (consumers, couriers/Dashers, merchants): order → assign → pick → deliver events into governed metrics for **on-time delivery**, **Dasher utilization**, and **undersupply rate**.

> **Status: Complete (local portfolio).** End-to-end lakehouse demo: synthetic events → Spark-style Parquet ingest → dbt staging/intermediate/marts → Great Expectations–style quality gates → KPI exports + Streamlit ops dashboard. Optional production wiring (EMR/Trino/live Tableau) is documented as future scope, not incomplete core.

## Why this exists

Marketplace ops need a single source of truth for supply/demand balance. This project models fulfillment events into **canonical datasets** and surfaces signature metrics so ops can rebalance local courier supply before late-order share spikes.

## Architecture

```
raw CSV events  →  Spark-style ingest (Parquet lake)  →  dbt staging → intermediate → marts
                                                              │
                                                              ├─ mart_supply_demand_hourly
                                                              ├─ quality gates (GE-style)
                                                              └─ Streamlit + CSV/Parquet KPI export
```

## Signature metrics

| Metric | Grain | Definition |
|--------|-------|------------|
| `dasher_utilization` | city × hour | active delivery hours / offered shift hours |
| `undersupply_rate` | city × hour | excess order load vs target 3.0 orders per active courier |
| `on_time_delivery_rate` | city × hour | share of orders delivered within promise window |

Full dictionary: [docs/01_metric_definitions.md](docs/01_metric_definitions.md)

## Quickstart

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
pip install -r requirements.txt
python run_demo.py
# optional dashboard
streamlit run app/dashboard.py
```

`run_demo.py` runs generate → ingest → `dbt build` → quality checks → KPI export.

Scale the generator with `SCALE` (default `1`):

```bash
# ~50k orders at SCALE=1; use SCALE=10 for heavier local load tests
set SCALE=10
python data/generate_marketplace_events.py
```

## What's included

| Component | Status |
|-----------|--------|
| Synthetic order / courier / merchant event generator (scalable) | Done |
| Lake ingest to partitioned Parquet | Done |
| dbt staging → intermediate → marts | Done |
| Signature metrics: Dasher utilization, undersupply, OTD | Done |
| Great Expectations–style quality gates | Done |
| KPI export for Tableau / Sigma | Done |
| Streamlit ops dashboard | Done |
| GitHub Actions CI (`dbt build` + quality) | Done |
| Metric dictionary | Done |

## Future production scope

See [docs/ROADMAP.md](docs/ROADMAP.md) for EMR/Trino/Airflow/live BI — optional hardening beyond this reproducible local portfolio.

## Stack

Python, SQL, dbt, DuckDB (local stand-in for Trino), Parquet / S3-layout lake, Spark-style batch ingest, Great Expectations–style checks, Streamlit.
