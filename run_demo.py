"""End-to-end local demo: generate → ingest → summarize signature metrics."""
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent


def run(cmd: list[str]) -> None:
    print(">", " ".join(cmd))
    subprocess.check_call(cmd, cwd=ROOT)


def main() -> None:
    run([sys.executable, "data/generate_marketplace_events.py"])
    run([sys.executable, "spark/ingest_to_lake.py"])
    run(["dbt", "build", "--profiles-dir", "."])
    run([sys.executable, "quality/run_checks.py"])
    print("\nDemo complete. Signature metrics in data/export/kpi_supply_demand_hourly.csv")
    print("Future scope (~30%): docs/ROADMAP.md")


if __name__ == "__main__":
    main()
