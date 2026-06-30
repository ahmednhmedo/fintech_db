"""
load_silver_incremental_to_sql.py
SQL INCREMENTAL LOADING layer — Silver Parquet -> SQL staging -> MERGE -> fact.

    data/incremental/silver/silver_batch_id=<b>/   (PySpark Silver output)
            │   read ONE new batch at a time
            ▼
    stg.fact_transactions_stage        (truncate + bulk load via fast_executemany)
            │   EXEC stg.usp_merge_incremental_fact @silver_batch_id
            ▼
    validate -> stg.fact_transactions_reject   (bad rows quarantined)
    MERGE    -> dbo.fact_transactions           (idempotent upsert on transaction_id)
    audit    -> stg.incremental_load_log        (SUCCESS / FAILED)

WHY A STAGING TABLE (not loading straight into the fact):
  * It lets us VALIDATE (dedupe, null keys, FK existence, null measures) and
    quarantine bad rows BEFORE they can touch the warehouse or abort a MERGE on
    a foreign-key violation.
  * The MERGE then runs set-based, server-side, in ONE transaction — far faster
    and safer than row-by-row inserts straight from Python.
  * Staging is disposable: truncated per run, so a crashed load leaves no
    partial warehouse state.

IDEMPOTENCY (two independent layers):
  1. Batch level — a batch already marked SUCCESS in stg.incremental_load_log is
     skipped (this loader never reprocesses a completed Silver batch).
  2. Row level — the MERGE keys on transaction_id and only updates on a real
     diff, so even a forced reprocess inserts/updates nothing extra.

Connection mirrors the existing batch loader: ODBC Driver 18 + Windows (Trusted)
auth to the WH_* target. autocommit=True here — the staging load is transient and
the warehouse atomicity is owned entirely by the stored procedure's transaction.

Run:  python incremental/load_silver_incremental_to_sql.py
Env:  WH_SERVER (ahmed\\SQLEXPRESS) | WH_DATABASE (fintech_db) | WH_DRIVER
      WH_USER / WH_PASSWORD -> SQL auth if both set, else Trusted_Connection
      SILVER_DIR -> default <repo>/data/incremental/silver
"""
import logging
import os
import time
from pathlib import Path

import pyarrow.dataset as ds
import pyodbc

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
REPO = Path(__file__).resolve().parents[1]
SILVER_DIR = Path(os.environ.get("SILVER_DIR", str(REPO / "data" / "incremental" / "silver")))
SQL_DIR = Path(__file__).resolve().parent / "sql"
READ_BATCH = 50_000   # parquet -> executemany chunk size

# The 18 fact business columns (order matches the staging table + MERGE).
FACT_COLS = [
    "date_key", "time_key", "account_key", "peer_account_key", "transaction_type_key",
    "decline_reason_key", "merchant_key", "transaction_id", "mc_transaction_id",
    "ach_transfer_id", "amount_minor", "amount_egp", "abs_amount_egp",
    "fx_amount_minor", "exchange_rate_e6", "is_outbound", "is_declined", "is_fx",
]
# Columns inserted into staging = business cols + two lineage columns.
STAGE_COLS = FACT_COLS + ["silver_batch_id", "_silver_processed_at_utc"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | sql-incr | %(message)s",
)
log = logging.getLogger("sql-incr")


# -----------------------------------------------------------------------------
# SQL Server connection (same style as ingestion/load_silver_to_sqlserver.py)
# -----------------------------------------------------------------------------
def connect():
    driver = os.environ.get("WH_DRIVER", "ODBC Driver 18 for SQL Server")
    server = os.environ.get("WH_SERVER", r"ahmed\SQLEXPRESS")
    database = os.environ.get("WH_DATABASE", "fintech_db")
    parts = [f"DRIVER={{{driver}}}", f"SERVER={server}", f"DATABASE={database}",
             "Encrypt=yes", "TrustServerCertificate=yes"]
    user, pwd = os.environ.get("WH_USER"), os.environ.get("WH_PASSWORD")
    parts += [f"UID={user}", f"PWD={pwd}"] if (user and pwd) else ["Trusted_Connection=yes"]
    # autocommit=True: staging load is transient; the proc owns warehouse atomicity.
    return pyodbc.connect(";".join(parts) + ";", autocommit=True)


def apply_sql_file(cur, path: Path) -> None:
    """Execute a .sql file, splitting on `GO` batch separators.

    `GO` is a client directive, not T-SQL, so pyodbc can't run it. We split on
    lines that are exactly `GO` and execute each batch in order. This is required
    because merge_incremental_fact.sql sets QUOTED_IDENTIFIER/ANSI_NULLS ON in
    their own batch (CREATE PROCEDURE must be the first statement in a batch).
    """
    import re
    text = path.read_text(encoding="utf-8")
    batches = re.split(r"(?im)^[ \t]*GO[ \t]*;?[ \t]*$", text)
    for batch in batches:
        if batch.strip():
            cur.execute(batch)


# -----------------------------------------------------------------------------
# Batch discovery
# -----------------------------------------------------------------------------
def discover_silver_batches() -> list[str]:
    """All silver_batch_id values present on disk (Hive partition folders)."""
    if not SILVER_DIR.exists():
        return []
    ids = [p.name.split("=", 1)[1]
           for p in SILVER_DIR.iterdir()
           if p.is_dir() and p.name.startswith("silver_batch_id=")]
    return sorted(ids)


def already_loaded(cur) -> set[str]:
    """silver_batch_ids that already have a SUCCESS row -> never reprocessed."""
    cur.execute("SELECT silver_batch_id FROM stg.incremental_load_log WHERE status='SUCCESS';")
    return {r[0] for r in cur.fetchall()}


# -----------------------------------------------------------------------------
# Per-batch load: truncate staging -> bulk load -> MERGE proc
# -----------------------------------------------------------------------------
def load_batch(cur, batch_id: str) -> dict:
    partition_dir = SILVER_DIR / f"silver_batch_id={batch_id}"

    # 1) Fresh staging (transient; never accumulates across runs).
    cur.execute("TRUNCATE TABLE stg.fact_transactions_stage;")

    # 2) Bulk load this batch's Parquet into staging. silver_batch_id is the
    #    partition value (not in the files), so we re-attach it per row.
    insert_cols = ",".join(f"[{c}]" for c in STAGE_COLS)
    placeholders = ",".join("?" * len(STAGE_COLS))
    insert_sql = f"INSERT INTO stg.fact_transactions_stage ({insert_cols}) VALUES ({placeholders})"
    cur.fast_executemany = True

    read_cols = FACT_COLS + ["_silver_processed_at_utc"]
    rows_read = 0
    dataset = ds.dataset(str(partition_dir), format="parquet")
    for rb in dataset.to_batches(columns=read_cols, batch_size=READ_BATCH):
        recs = rb.to_pylist()
        rows = [[r[c] for c in FACT_COLS] + [batch_id, r["_silver_processed_at_utc"]]
                for r in recs]
        if rows:
            cur.executemany(insert_sql, rows)
            rows_read += len(rows)
    log.info("  staged %s rows for batch %s", f"{rows_read:,}", batch_id)

    # 3) Validate + MERGE + audit, all inside the proc's single transaction.
    cur.execute("EXEC stg.usp_merge_incremental_fact @silver_batch_id = ?;", batch_id)
    summary = cur.fetchone()
    return {
        "silver_batch_id": summary.silver_batch_id,
        "rows_staged": summary.rows_staged,
        "rows_valid": summary.rows_valid,
        "rows_inserted": summary.rows_inserted,
        "rows_updated": summary.rows_updated,
        "rows_rejected": summary.rows_rejected,
    }


def main() -> int:
    t0 = time.time()
    conn = connect()
    cur = conn.cursor()

    # Idempotent setup (schema, staging/reject/log tables, MERGE proc).
    apply_sql_file(cur, SQL_DIR / "create_incremental_staging.sql")
    apply_sql_file(cur, SQL_DIR / "merge_incremental_fact.sql")

    on_disk = discover_silver_batches()
    done = already_loaded(cur)
    todo = [b for b in on_disk if b not in done]

    log.info("Silver batches: %d on disk | %d already loaded | %d to process",
             len(on_disk), len(done), len(todo))
    if not todo:
        log.info("Nothing to do — warehouse is up to date.")
        cur.close(); conn.close()
        return 0

    tot_ins = tot_upd = tot_rej = 0
    failed = []
    for batch_id in todo:
        log.info("Processing batch %s ...", batch_id)
        try:
            r = load_batch(cur, batch_id)
            tot_ins += r["rows_inserted"]; tot_upd += r["rows_updated"]; tot_rej += r["rows_rejected"]
            log.info("  batch %s -> staged=%d valid=%d inserted=%d updated=%d rejected=%d",
                     batch_id, r["rows_staged"], r["rows_valid"],
                     r["rows_inserted"], r["rows_updated"], r["rows_rejected"])
        except pyodbc.Error as exc:
            # The proc already logged FAILED and rolled back; stop here so the
            # batch is retried (in order) on the next run. Restart-safe.
            log.error("  batch %s FAILED: %s", batch_id, exc)
            failed.append(batch_id)
            break

    cur.close(); conn.close()
    log.info("Done in %.1fs | inserted=%d updated=%d rejected=%d | failed_batches=%d",
             time.time() - t0, tot_ins, tot_upd, tot_rej, len(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
