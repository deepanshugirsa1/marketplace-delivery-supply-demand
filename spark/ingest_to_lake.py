"""Spark-style batch ingest: CSV raw → partitioned Parquet lake (local stand-in)."""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
LAKE = ROOT / "data" / "lake"
LAKE.mkdir(parents=True, exist_ok=True)


def ingest(name: str, partition_col: str | None = None) -> None:
    src = RAW / f"{name}.csv"
    df = pd.read_csv(src)
    out = LAKE / name
    out.mkdir(parents=True, exist_ok=True)
    if partition_col and partition_col in df.columns:
        for key, part in df.groupby(partition_col):
            part_dir = out / f"{partition_col}={key}"
            part_dir.mkdir(parents=True, exist_ok=True)
            part.to_parquet(part_dir / "part-000.parquet", index=False)
    else:
        df.to_parquet(out / "part-000.parquet", index=False)
    print(f"Ingested {name}: {len(df)} rows → {out}")


def main() -> None:
    ingest("orders", partition_col="city")
    ingest("courier_shifts", partition_col="city")
    ingest("merchant_readiness", partition_col="city")


if __name__ == "__main__":
    main()
