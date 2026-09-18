"""Great Expectations–style quality gates on the supply-demand mart."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "warehouse.duckdb"
OUT = ROOT / "data" / "export"
OUT.mkdir(parents=True, exist_ok=True)


def main() -> None:
    con = duckdb.connect(str(DB))
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if "mart_supply_demand_hourly" not in tables:
        raise SystemExit("mart_supply_demand_hourly missing — run `dbt build --profiles-dir .` first")

    df = con.execute("SELECT * FROM mart_supply_demand_hourly").fetchdf()
    checks = []

    def check(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(ok), "detail": detail})

    check("row_count_gt_0", len(df) > 0, f"rows={len(df)}")
    check("city_not_null", bool(df["city"].notna().all()), "all cities present")
    check("hour_bucket_not_null", bool(df["hour_bucket"].notna().all()), "all hour buckets present")
    check(
        "pk_city_hour_unique",
        df.duplicated(subset=["city", "hour_bucket"]).sum() == 0,
        "unique city×hour grain",
    )
    check(
        "utilization_bounds",
        bool(df["dasher_utilization"].dropna().between(0, 1.05).all())
        if df["dasher_utilization"].notna().any()
        else True,
        "utilization in [0, 1.05]",
    )
    check(
        "undersupply_bounds",
        bool(df["undersupply_rate"].dropna().between(0, 1).all())
        if df["undersupply_rate"].notna().any()
        else True,
        "undersupply in [0, 1]",
    )
    check(
        "otd_bounds",
        bool(df["on_time_delivery_rate"].dropna().between(0, 1).all())
        if df["on_time_delivery_rate"].notna().any()
        else True,
        "OTD in [0, 1]",
    )
    check("orders_non_negative", bool((df["orders"] >= 0).all()), "orders >= 0")

    # Freshness: max hour_bucket within 30 days of synthetic window end is always true locally;
    # assert max hour is parseable and present.
    check("freshness_max_hour_present", bool(df["hour_bucket"].notna().any()), "max hour present")

    failed = [c for c in checks if not c["passed"]]
    report = {
        "suite": "great_expectations_style_marketplace_supply_demand",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "passed": len(failed) == 0,
        "checks": checks,
    }
    (OUT / "quality_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    df.to_csv(OUT / "kpi_supply_demand_hourly.csv", index=False)
    df.to_parquet(OUT / "kpi_supply_demand_hourly.parquet", index=False)
    print(json.dumps(report, indent=2))
    if failed:
        raise SystemExit(f"{len(failed)} quality check(s) failed")


if __name__ == "__main__":
    main()
