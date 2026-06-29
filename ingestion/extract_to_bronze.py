"""
BRONZE layer (Airflow owns this). Zero business transformation:
  8 source CSVs -> bronze/<name> as Parquet, plus two lineage columns
  (_ingested_at_utc, _source_file) for auditability.

The source is already in star shape, so bronze is a faithful raw landing of each
file. Idempotent: each run clears its own partition dir, so re-running never
duplicates data.

Run:  python extract_to_bronze.py
"""
import os
import shutil
from datetime import datetime, timezone
import pandas as pd

RAW = os.environ.get("RAW_DIR", "/opt/data/raw")
LAKE = os.environ.get("LAKE_DIR", "/opt/data/lake")
BRONZE = f"{LAKE}/bronze"

# All 8 dimensional CSVs (the full star, already keyed).
SOURCES = [
    "dim_date", "dim_time", "dim_location", "dim_account",
    "dim_merchant", "dim_transaction_type", "dim_decline_reason",
    "fact_transactions",
]


def _fresh_dir(path):
    """Idempotency: clear the partition dir so a re-run never mixes new part
    files with stale ones from a previous extract."""
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)


def extract_csvs():
    os.makedirs(BRONZE, exist_ok=True)
    # Lineage: ONE timestamp for the whole run (not per chunk / per table).
    ingested_at_utc = datetime.now(timezone.utc).isoformat()
    for name in SOURCES:
        out = f"{BRONZE}/{name}"
        _fresh_dir(out)
        src = f"{RAW}/{name}.csv"
        source_file = f"{name}.csv"          # original CSV filename
        rows = 0
        # Read as string (dtype=str) to preserve raw values + keep NULLs as NULL
        # (empty CSV field -> NaN -> parquet null). Typing happens in silver.
        for i, chunk in enumerate(pd.read_csv(src, dtype=str, chunksize=500_000)):
            # Append lineage columns only — raw business columns and row counts unchanged.
            chunk["_ingested_at_utc"] = ingested_at_utc
            chunk["_source_file"] = source_file
            chunk.to_parquet(f"{out}/part-{i:04d}.parquet", index=False)
            rows += len(chunk)
        print(f"bronze/{name}: {rows:,} rows", flush=True)


if __name__ == "__main__":
    extract_csvs()
