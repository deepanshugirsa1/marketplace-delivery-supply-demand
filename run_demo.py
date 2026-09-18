"""End-to-end local demo: generate → ingest → dbt → quality → KPI export."""
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent


def run(cmd: list[str]) -> None:
    print(">", " ".join(cmd))
    subprocess.check_call(cmd, cwd=ROOT)


def dbt_bin() -> str:
    scripts = Path(sys.executable).resolve().parent
    for name in ("dbt.exe", "dbt"):
        candidate = scripts / name
        if candidate.exists():
            return str(candidate)
    return "dbt"


def main() -> None:
    py = sys.executable
    run([py, "data/generate_marketplace_events.py"])
    run([py, "spark/ingest_to_lake.py"])
    run([dbt_bin(), "build", "--profiles-dir", "."])
    run([py, "quality/run_checks.py"])
    print("\nDemo complete.")
    print("  KPIs: data/export/kpi_supply_demand_hourly.csv")
    print("  Quality: data/export/quality_report.json")
    print("  Dashboard: streamlit run app/dashboard.py")
    print("  Metrics: docs/01_metric_definitions.md")


if __name__ == "__main__":
    main()
