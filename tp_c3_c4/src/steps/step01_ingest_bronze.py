import shutil, os
from datetime import date
from pathlib import Path

SOURCE      = Path("data/source/sales_events.csv")
BRONZE_BASE = Path("lake/bronze/sales_events")

def ingest_bronze(run_date=None):
    run_date  = run_date or str(date.today())
    partition = BRONZE_BASE / f"ingest_date={run_date}"
    partition.mkdir(parents=True, exist_ok=True)
    dest = partition / "sales_events.csv"
    shutil.copy2(SOURCE, dest)
    rows = sum(1 for _ in open(dest)) - 1
    print(f"[Bronze] {rows} lignes → {dest}")
    return {"rows_written": rows}

if __name__ == "__main__":
    os.chdir(Path(__file__).parent.parent.parent)
    ingest_bronze()