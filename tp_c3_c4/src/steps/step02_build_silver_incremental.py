import json, pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

BRONZE_BASE  = Path("lake/bronze/sales_events")
SILVER_BASE  = Path("lake/silver/sales_events")
QUARANTINE   = Path("lake/quarantine")
WATERMARK_F  = Path("state/watermark.json")

def load_watermark():
    if WATERMARK_F.exists():
        wm = json.loads(WATERMARK_F.read_text())["last_updated_at"]
        return datetime.fromisoformat(wm)
    return datetime(2000, 1, 1)

def save_watermark(ts):
    WATERMARK_F.parent.mkdir(parents=True, exist_ok=True)
    WATERMARK_F.write_text(json.dumps({"last_updated_at": ts.isoformat()}))
    print(f"[Silver] Watermark → {ts.isoformat()}")

def read_bronze():
    frames = [pd.read_csv(f, dtype=str) for f in BRONZE_BASE.rglob("*.csv")]
    return pd.concat(frames, ignore_index=True)

def apply_types_and_quarantine(df):
    df = df.copy()
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
    df["amount"]     = pd.to_numeric(df["amount"],      errors="coerce")
    df["product_id"] = pd.to_numeric(df["product_id"],  errors="coerce")
    mask = (df["order_id"].notna() & df["event_time"].notna() &
            df["updated_at"].notna() & df["amount"].notna() &
            (df["amount"] > 0) & df["product_id"].notna() &
            df["store_id"].notna())
    return df[mask].copy(), df[~mask].copy()

def write_quarantine(invalid):
    if invalid.empty: return
    QUARANTINE.mkdir(parents=True, exist_ok=True)
    dest = QUARANTINE / "bad_rows.parquet"
    for col in invalid.select_dtypes("datetime64[ns]").columns:
        invalid[col] = invalid[col].astype(str)
    invalid.to_parquet(dest, index=False)
    print(f"[Quarantine] {len(invalid)} lignes → {dest}")

def write_silver(df):
    df["_y"] = df["event_time"].dt.year
    df["_m"] = df["event_time"].dt.month
    df["_d"] = df["event_time"].dt.day
    for (y,m,d), grp in df.groupby(["_y","_m","_d"]):
        part = SILVER_BASE / f"year={y}/month={m:02d}/day={d:02d}"
        part.mkdir(parents=True, exist_ok=True)
        grp.drop(columns=["_y","_m","_d"]).to_parquet(part/"data.parquet", index=False)
    print(f"[Silver] {len(df)} lignes écrites")

def build_silver(run_date=None, lookback_days=2):
    watermark = load_watermark()
    start     = watermark - timedelta(days=lookback_days)
    print(f"[Silver] Watermark={watermark} | Start={start}")
    raw = read_bronze()
    raw["updated_at_dt"] = pd.to_datetime(raw["updated_at"], errors="coerce")
    filtered = raw[raw["updated_at_dt"] > start].copy()
    print(f"[Silver] Après filtre : {len(filtered)} / {len(raw)}")
    valid, invalid = apply_types_and_quarantine(filtered)
    write_quarantine(invalid)
    valid = valid.sort_values("updated_at", ascending=False)
    valid = valid.drop_duplicates(subset=["order_id"], keep="first")
    valid = valid[valid["op"] != "D"]
    print(f"[Silver] Après dédup : {len(valid)} lignes")
    if not valid.empty: write_silver(valid)
    new_wm = filtered["updated_at_dt"].max()
    if pd.notna(new_wm) and new_wm > watermark: save_watermark(new_wm)
    return {"rows_read":len(raw),"rows_valid":len(valid),"rows_quarantined":len(invalid)}

if __name__ == "__main__":
    import os
    os.chdir(Path(__file__).parent.parent.parent)
    build_silver()