import os
import pandas as pd
from pathlib import Path
from datetime import datetime

SILVER_BASE      = Path("lake/silver/sales_events")
PREV_VOLUME_FILE = Path("state/prev_volume.txt")


def read_silver():
    frames = [pd.read_parquet(f) for f in SILVER_BASE.rglob("*.parquet")]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def quality_gate():
    df = read_silver()
    errors, warnings, results = [], [], {}

    # Gate 1 : Non vide
    if df.empty:
        raise ValueError("Silver est VIDE")
    results["row_count"] = len(df)

    # Gate 2 : Null check
    for col in ["order_id", "event_time", "amount", "product_id", "store_id"]:
        n = int(df[col].isna().sum()) if col in df.columns else 0
        if n > 0:
            errors.append(f"NULL dans {col}: {n} lignes")
    results["null_check"] = "OK"

    # Gate 3 : Range check amount > 0
    bad = int((pd.to_numeric(df["amount"], errors="coerce") <= 0).sum())
    if bad > 0:
        errors.append(f"{bad} lignes avec amount <= 0")
    results["range_check"] = bad

    # Gate 4 : Unicité order_id
    dupes = int(df["order_id"].duplicated().sum())
    if dupes > 0:
        errors.append(f"{dupes} order_id dupliqués")
    results["uniqueness"] = dupes

    # Gate 5 : Volume anomaly ±20%
    vol = len(df)
    if PREV_VOLUME_FILE.exists():
        prev = int(PREV_VOLUME_FILE.read_text().strip())
        if prev > 0:
            delta = abs(vol - prev) / prev
            results["volume_delta_pct"] = round(delta * 100, 2)
            if delta > 0.20:
                warnings.append(f"Volume anomaly: {vol} vs {prev} (delta={delta*100:.1f}%)")
    PREV_VOLUME_FILE.parent.mkdir(parents=True, exist_ok=True)
    PREV_VOLUME_FILE.write_text(str(vol))

    # Gate 6 : Freshness SLA 8h
    max_upd = pd.to_datetime(df["updated_at"], errors="coerce").max()
    if pd.notna(max_upd):
        age_h = (datetime.utcnow() - max_upd).total_seconds() / 3600
        results["freshness_age_h"] = round(age_h, 1)
        if age_h > 8:
            warnings.append(f"Freshness: {age_h:.1f}h > SLA 8h")

    # Résumé
    status = "FAIL" if errors else ("WARN" if warnings else "OK")
    for e in errors:
        print(f"[Gate] FAIL: {e}")
    for w in warnings:
        print(f"[Gate] WARN: {w}")
    if status == "OK":
        print("[Gate] Tous les checks OK")
    print(f"[Gate] Résultats: {results}")

    if errors:
        raise ValueError(f"Quality gate FAILED: {errors}")
    return {"status": status, "checks": results}


if __name__ == "__main__":
    os.chdir(Path(__file__).parent.parent.parent)
    quality_gate()