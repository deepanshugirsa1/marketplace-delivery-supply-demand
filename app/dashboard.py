"""Streamlit ops dashboard over mart_supply_demand_hourly KPIs."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "warehouse.duckdb"
CSV = ROOT / "data" / "export" / "kpi_supply_demand_hourly.csv"


@st.cache_data
def load_kpi() -> pd.DataFrame:
    if DB.exists():
        con = duckdb.connect(str(DB), read_only=True)
        try:
            return con.execute("SELECT * FROM mart_supply_demand_hourly").fetchdf()
        finally:
            con.close()
    if CSV.exists():
        return pd.read_csv(CSV)
    raise FileNotFoundError("Run `python run_demo.py` first to build the mart / KPI export.")


def main() -> None:
    st.set_page_config(page_title="Marketplace Supply-Demand", layout="wide")
    st.title("Marketplace supply & demand")
    st.caption("Dasher utilization · undersupply rate · on-time delivery — city × hour")

    df = load_kpi()
    cities = sorted(df["city"].dropna().unique().tolist())
    city = st.sidebar.multiselect("City", cities, default=cities)
    filtered = df[df["city"].isin(city)].copy()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("City×hour rows", f"{len(filtered):,}")
    c2.metric("Avg Dasher utilization", f"{filtered['dasher_utilization'].mean():.1%}")
    c3.metric("Avg undersupply rate", f"{filtered['undersupply_rate'].mean():.1%}")
    c4.metric("Avg OTD", f"{filtered['on_time_delivery_rate'].mean():.1%}")

    st.subheader("Undersupply by city")
    by_city = (
        filtered.groupby("city", as_index=False)
        .agg(
            orders=("orders", "sum"),
            undersupply_rate=("undersupply_rate", "mean"),
            dasher_utilization=("dasher_utilization", "mean"),
            on_time_delivery_rate=("on_time_delivery_rate", "mean"),
        )
        .sort_values("undersupply_rate", ascending=False)
    )
    st.dataframe(by_city, use_container_width=True)
    st.bar_chart(by_city.set_index("city")[["undersupply_rate", "dasher_utilization"]])

    st.subheader("Hourly detail")
    st.dataframe(
        filtered.sort_values(["city", "hour_bucket"]).head(500),
        use_container_width=True,
    )


if __name__ == "__main__":
    main()
