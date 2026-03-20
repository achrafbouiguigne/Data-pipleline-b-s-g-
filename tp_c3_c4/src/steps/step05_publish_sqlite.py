import os
import sqlite3
import pandas as pd
from pathlib import Path

GOLD_BASE = Path("lake/gold")
DB_PATH   = Path("lake/gold/sales_mart.db")


def publish():
    tables = {
        "daily_sales":     GOLD_BASE / "daily_sales.parquet",
        "product_ranking": GOLD_BASE / "product_ranking.parquet",
        "store_summary":   GOLD_BASE / "store_summary.parquet",
    }
    conn = sqlite3.connect(DB_PATH)
    for name, path in tables.items():
        df = pd.read_parquet(path)
        df.to_sql(name, conn, if_exists="replace", index=False)
        print(f"[Publish] {name} : {len(df)} lignes → SQLite")
    conn.close()
    print(f"[Publish] DB : {DB_PATH}")
    return {"db_path": str(DB_PATH)}


if __name__ == "__main__":
    os.chdir(Path(__file__).parent.parent.parent)
    publish()