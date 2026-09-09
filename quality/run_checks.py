"""Great Expectations–style quality gates on the supply-demand mart."""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "warehouse.duckdb"
OUT = ROOT / "data" / "export"
OUT.mkdir(parents=True, exist_ok=True)


def main() -> None:
    con = duckdb.connect(str(DB))
    # Ensure mart exists (dbt build should have created it)
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if "mart_supply_demand_hourly" not in tables:
        raise SystemExit("mart_supply_demand_hourly missing — run `dbt build --profiles-dir .` first")

    df = con.execute("SELECT * FROM mart_supply_demand_hourly").fetchdf()
    checks = []

    def check(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(ok), "detail": detail})

    check("row_count_gt_0", len(df) > 0, f"rows={len(df)}")
    check("city_not_null", df["city"].notna().all(), "all cities present")
    check(
        "utilization_bounds",
        df["dasher_utilization"].dropna().between(0, 1.05).all() if df["dasher_utilization"].notna().any() else True,
        "utilization in [0, 1.05]",
    )
    check(
        "undersupply_bounds",
        df["undersupply_rate"].dropna().between(0, 1).all() if df["undersupply_rate"].notna().any() else True,
        "undersupply in [0, 1]",
    )
    check(
        "otd_bounds",
        df["on_time_delivery_rate"].dropna().between(0, 1).all() if df["on_time_delivery_rate"].notna().any() else True,
        "OTD in [0, 1]",
    )

    failed = [c for c in checks if not c["passed"]]
    report = {"passed": len(failed) == 0, "checks": checks}
    (OUT / "quality_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    df.to_csv(OUT / "kpi_supply_demand_hourly.csv", index=False)
    print(json.dumps(report, indent=2))
    if failed:
        raise SystemExit(f"{len(failed)} quality check(s) failed")


if __name__ == "__main__":
    main()
