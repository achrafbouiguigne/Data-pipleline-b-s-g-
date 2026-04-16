import argparse, json, os, sys, time, uuid, traceback
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, sp(Path(__file__).parent / "src" / "steps"))

from steps.step01_ingest_bronze            import ingest_bronze
from steps.step02_build_silver_incremental import build_silver
from steps.step03_quality_gates            import quality_gate
from steps.step04_build_gold               import build_gold
from steps.step05_publish_sqlite           import publish

LOGS_FILE = Path("logs/runs.jsonl")


def log_run(run_id, step, status, metrics, duration):
    LOGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "run_id":           run_id,
        "timestamp":        datetime.utcnow().isoformat(),
        "step":             step,
        "status":           status,
        "duration_sec":     round(duration, 3),
        "rows_read":        metrics.get("rows_read", 0),
        "rows_written":     metrics.get("rows_written", 0),
        "rows_quarantined": metrics.get("rows_quarantined", 0),
    }
    with open(LOGS_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")
    icon = "OK" if status == "SUCCESS" else "FAIL"
    print(f"  [{icon}] {step} | {round(duration, 2)}s | {metrics}")


def with_retry(fn, retries=3, backoff=1):
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == retries:
                raise
            wait = backoff * (2 ** (attempt - 1))
            print(f"  Retry {attempt}/{retries} apres: {e} — wait {wait}s")
            time.sleep(wait)


def run_single(run_date, run_id, lookback=2):
    print(f"\n{'='*55}")
    print(f"Pipeline run_date={run_date}  run_id={run_id}")
    print(f"{'='*55}")
    steps = [
        ("ingest_bronze",  lambda: ingest_bronze(run_date)),
        ("build_silver",   lambda: build_silver(run_date, lookback)),
        ("quality_gate",   lambda: quality_gate()),
        ("build_gold",     lambda: build_gold()),
        ("publish_sqlite", lambda: publish()),
    ]
    for name, fn in steps:
        t0 = time.time()
        try:
            result = with_retry(fn)
            log_run(run_id, name, "SUCCESS", result or {}, time.time() - t0)
        except Exception:
            log_run(run_id, name, "FAIL", {}, time.time() - t0)
            traceback.print_exc()
            return False
    print("\nPipeline termine avec succes!")
    return True


def run_backfill(start, end, lookback=2):
    cur = datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end, "%Y-%m-%d").date()
    while cur <= end_dt:
        run_single(str(cur), str(uuid.uuid4())[:8], lookback)
        cur += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=str(date.today()))
    parser.add_argument("--mode", default="incremental",
                        choices=["incremental", "backfill"])
    parser.add_argument("--backfill-start")
    parser.add_argument("--backfill-end")
    parser.add_argument("--lookback-days", type=int, default=2)
    args = parser.parse_args()
    os.chdir(Path(__file__).parent)
    if args.mode == "backfill":
        run_backfill(args.backfill_start, args.backfill_end, args.lookback_days)
    else:
        run_single(args.run_date, str(uuid.uuid4())[:8], args.lookback_days)


if __name__ == "__main__":
    main()