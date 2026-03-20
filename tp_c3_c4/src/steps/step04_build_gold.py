import os
import pandas as pd
from pathlib import Path

SILVER_BASE = Path("lake/silver/sales_events")
GOLD_BASE   = Path("lake/gold")


def read_silver():
    frames = [pd.read_parquet(f) for f in SILVER_BASE.rglob("*.parquet")]
    df = pd.concat(frames, ignore_index=True)
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    df["amount"]     = pd.to_numeric(df["amount"],      errors="coerce")
    return df


def build_gold():
    df = read_silver()
    GOLD_BASE.mkdir(parents=True, exist_ok=True)
    df["sale_date"] = df["event_time"].dt.date

    # KPI 1 : Ventes journalières par store
    daily = (
        df.groupby(["sale_date", "store_id"])
        .agg(total_revenue=("amount", "sum"),
             order_count=("order_id", "nunique"))
        .reset_index()
    )
    daily.to_parquet(GOLD_BASE / "daily_sales.parquet", index=False)
    print(f"[Gold] daily_sales     : {len(daily)} lignes")

    # KPI 2 : Ranking produits
    prod = (
        df.groupby("product_id")
        .agg(total_revenue=("amount", "sum"),
             order_count=("order_id", "nunique"))
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )
    prod["rank"] = range(1, len(prod) + 1)
    prod.to_parquet(GOLD_BASE / "product_ranking.parquet", index=False)
    print(f"[Gold] product_ranking : {len(prod)} produits")

    # KPI 3 : Résumé par store
    stores = (
        df.groupby("store_id")
        .agg(total_revenue=("amount", "sum"),
             order_count=("order_id", "nunique"),
             avg_basket=("amount", "mean"))
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )
    stores.to_parquet(GOLD_BASE / "store_summary.parquet", index=False)
    print(f"[Gold] store_summary   : {len(stores)} stores")

    return {"rows_written": len(daily) + len(prod) + len(stores)}


if __name__ == "__main__":
    os.chdir(Path(__file__).parent.parent.parent)
    build_gold()