"""
SILVER INCREMENTAL layer (PySpark, micro-batch).

    Bronze Incremental JSONL  ->  [ this job ]  ->  Silver Incremental Parquet
                                                    +  Quarantine Parquet (bad rows)

This is the streaming counterpart of the batch `spark/bronze_to_silver.py`. It
applies the SAME conforming transformations to the streamed `fact_transactions`
events, but with three additions the batch job does not need:

  1. INCREMENTAL: it processes only Bronze rows newer than the last run, using a
     Kafka-offset checkpoint (see "Incremental strategy" below).
  2. DATA QUALITY + QUARANTINE: invalid rows are isolated (never dropped).
  3. LINEAGE: Bronze lineage (_kafka_*) is preserved and Silver lineage
     (_silver_processed_at_utc, _silver_batch_id) is added.

Run on the host (PySpark installed in the venv):
    python silver_incremental.py
or with spark-submit / inside a Spark container (see README_SILVER_INCREMENTAL.md).

-------------------------------------------------------------------------------
Incremental strategy — processed Kafka OFFSET tracking (checkpoint file)
-------------------------------------------------------------------------------
Every Bronze record already carries its Kafka coordinates (_kafka_topic,
_kafka_partition, _kafka_offset) written by consumer.py. Kafka offsets are
*monotonic and unique per partition*, which makes them a perfect natural
watermark — strictly better than a row-count or a wall-clock timestamp (those
have ties and break if the file is ever rewritten).

We persist, per (topic, partition), the highest offset already processed:

    data/incremental/_checkpoints/silver_offsets.json
    { "transactions_raw": { "0": 17 } }

Each run keeps only rows whose offset is GREATER than the stored high-water mark,
then advances the mark. First run (no file) processes everything. This is the
simplest production-quality option for a Kafka-fed Bronze: no extra store, exact,
and replay-safe.

-------------------------------------------------------------------------------
Idempotency — running twice never duplicates rows
-------------------------------------------------------------------------------
Two layers guarantee it:
  * Normal re-run (no new events): the checkpoint reports 0 new offsets, so the
    job writes nothing and exits.
  * Crash between the Parquet write and the checkpoint update: the batch id is
    DERIVED DETERMINISTICALLY from the offset range processed
    (b_<minOffset>_<maxOffset>). A re-run sees the same un-advanced checkpoint,
    reprocesses the identical offset range, computes the identical batch id, and
    writes to the SAME partition with dynamic partition overwrite — replacing it
    in place instead of appending a duplicate.
Any cross-run duplicate transaction_id that survives at-least-once delivery is
finally collapsed downstream by the SQL MERGE / dbt unique_key (see README).
"""
import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# Paths — resolve to PROJECT ROOT so the job runs from anywhere; env-overridable
# (override these when running inside a Spark container with mounted /data).
# -----------------------------------------------------------------------------
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, ".."))
_INCR = os.path.join(_PROJECT_ROOT, "data", "incremental")

BRONZE_JSONL = os.environ.get("BRONZE_JSONL", os.path.join(_INCR, "bronze", "transactions_raw.jsonl"))
SILVER_DIR = os.environ.get("SILVER_DIR", os.path.join(_INCR, "silver"))
QUARANTINE_DIR = os.environ.get("QUARANTINE_DIR", os.path.join(_INCR, "quarantine"))
CHECKPOINT_FILE = os.environ.get("CHECKPOINT_FILE", os.path.join(_INCR, "_checkpoints", "silver_offsets.json"))

# -----------------------------------------------------------------------------
# Schema contract — IDENTICAL to batch spark/bronze_to_silver.py fact_transactions
# (transaction_sk is a CSV surrogate, intentionally excluded from the warehouse).
# -----------------------------------------------------------------------------
INT, LONG, STR, DEC = "int", "long", "string", "decimal(18,2)"
FACT_SCHEMA = {
    "date_key": INT, "time_key": INT, "account_key": INT,
    "peer_account_key": INT, "transaction_type_key": INT,
    "decline_reason_key": INT, "merchant_key": INT,
    "transaction_id": STR, "mc_transaction_id": STR, "ach_transfer_id": STR,
    "amount_minor": LONG, "amount_egp": DEC, "abs_amount_egp": DEC,
    "fx_amount_minor": LONG, "exchange_rate_e6": LONG,
    "is_outbound": INT, "is_declined": INT, "is_fx": INT,
}
LINEAGE_COLS = ["_kafka_topic", "_kafka_partition", "_kafka_offset", "_consumed_at_utc"]
DEDUPE_KEY = "transaction_id"


# -----------------------------------------------------------------------------
# Checkpoint helpers (tiny local JSON; written atomically)
# -----------------------------------------------------------------------------
def load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def save_checkpoint(checkpoint: dict) -> None:
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(checkpoint, fh, indent=2, sort_keys=True)
    os.replace(tmp, CHECKPOINT_FILE)   # atomic on the same filesystem


# -----------------------------------------------------------------------------
# Transformations
# -----------------------------------------------------------------------------
def conform(df):
    """Trim strings, '' -> NULL, cast every column to its warehouse type.

    Identical rules to the batch Silver job; lineage columns are carried through
    untouched (the batch job drops them — the incremental layer keeps them).
    """
    business = []
    for col, typ in FACT_SCHEMA.items():
        c = F.col(col)
        if typ == STR:
            c = F.trim(c)
            c = F.when(c == "", None).otherwise(c)              # '' -> NULL
        else:
            c = F.when(F.trim(c) == "", None).otherwise(c)      # empty -> NULL pre-cast
            c = c.cast(typ)
        business.append(c.alias(col))
    return df.select(*business, *LINEAGE_COLS)


def read_new_bronze(spark, checkpoint):
    """Read Bronze JSONL, flatten the payload, and keep only un-processed offsets.

    Returns None if the Bronze file exists but is empty (no records yet) — Spark
    infers a column-less DataFrame in that case, and selecting `payload.*` would
    otherwise raise. The caller treats None as "nothing to do".
    """
    raw = spark.read.json(BRONZE_JSONL)
    # Empty / whitespace-only file -> no inferred columns (no `payload` struct).
    if "payload" not in raw.columns:
        return None
    # Bronze shape: {payload:{...transaction...}, _kafka_topic, _kafka_partition, _kafka_offset, _consumed_at_utc}
    flat = raw.select(F.col("payload.*"), *[F.col(c) for c in LINEAGE_COLS])

    # Incremental filter via a left-join against the checkpoint high-water marks.
    ckpt_rows = [
        (topic, int(part), int(off))
        for topic, parts in checkpoint.items()
        for part, off in parts.items()
    ]
    if ckpt_rows:
        ckdf = spark.createDataFrame(ckpt_rows, ["_kafka_topic", "_kafka_partition", "_ckpt_off"])
        flat = (
            flat.join(ckdf, ["_kafka_topic", "_kafka_partition"], "left")
                .filter(F.col("_kafka_offset") > F.coalesce(F.col("_ckpt_off"), F.lit(-1)))
                .drop("_ckpt_off")
        )
    return flat


def main(spark):
    if not os.path.exists(BRONZE_JSONL):
        print(f"[silver] Bronze file not found yet: {BRONZE_JSONL} — nothing to do.")
        return

    checkpoint = load_checkpoint()
    raw_new = read_new_bronze(spark, checkpoint)
    if raw_new is None:
        print("[silver] Bronze file is empty (no records yet) — nothing to do.")
        return

    new_rows = conform(raw_new).cache()
    new_count = new_rows.count()
    if new_count == 0:
        print("[silver] 0 new Bronze records — already up to date. Nothing written.")
        return

    # Offset range drives a DETERMINISTIC batch id (replay-safe, see module docstring).
    bounds = new_rows.agg(
        F.min("_kafka_offset").alias("lo"), F.max("_kafka_offset").alias("hi")
    ).first()
    batch_id = f"b_{int(bounds['lo']):09d}_{int(bounds['hi']):09d}"
    processed_at = datetime.now(timezone.utc).isoformat()

    enriched = (
        new_rows
        .withColumn("_silver_processed_at_utc", F.lit(processed_at))
        # Lineage metadata column (underscore-prefixed, mirrors _kafka_* / batch Bronze).
        .withColumn("_silver_batch_id", F.lit(batch_id))
        # PARTITION column — same value, but NO leading underscore. Hive folders that
        # start with "_" are treated as hidden by pandas/pyarrow (and Spark's own file
        # listing), so partitioning by "_silver_batch_id" made directory reads return 0
        # rows. Partitioning by "silver_batch_id" keeps the output readable downstream.
        .withColumn("silver_batch_id", F.lit(batch_id))
    )

    # ---- Data quality: flag, don't drop -------------------------------------
    # Required fields for a usable fact row: a key, a date, and a numeric amount.
    dq_reason = (
        F.when(F.col("transaction_id").isNull(), F.lit("missing_transaction_id"))
         .when(F.col("date_key").isNull(), F.lit("invalid_or_missing_date_key"))
         .when(F.col("amount_egp").isNull(), F.lit("invalid_or_missing_amount"))
         .otherwise(F.lit(None))
    )
    flagged = enriched.withColumn("_dq_reason", dq_reason)
    invalid = flagged.filter(F.col("_dq_reason").isNotNull())
    valid = flagged.filter(F.col("_dq_reason").isNull())

    # ---- Dedupe on transaction_id (keep earliest offset; quarantine the rest)
    w = Window.partitionBy(DEDUPE_KEY).orderBy(F.col("_kafka_offset").asc())
    ranked = valid.withColumn("_rn", F.row_number().over(w))
    clean = ranked.filter(F.col("_rn") == 1).drop("_rn", "_dq_reason")
    dup_rows = (
        ranked.filter(F.col("_rn") > 1)
              .drop("_rn")
              .withColumn("_dq_reason", F.lit("duplicate_transaction_id"))
    )
    quarantine = invalid.unionByName(dup_rows)

    # ---- Write (dynamic partition overwrite => idempotent re-runs) -----------
    clean_n = clean.count()
    (clean.write
          .mode("overwrite")
          .option("partitionOverwriteMode", "dynamic")
          .partitionBy("silver_batch_id")
          .parquet(SILVER_DIR))

    quar_n = quarantine.count()
    if quar_n > 0:
        (quarantine.write
                   .mode("overwrite")
                   .option("partitionOverwriteMode", "dynamic")
                   .partitionBy("silver_batch_id")
                   .parquet(QUARANTINE_DIR))

    # ---- Advance the checkpoint LAST (after a successful write) --------------
    for row in new_rows.groupBy("_kafka_topic", "_kafka_partition").agg(
        F.max("_kafka_offset").alias("mx")
    ).collect():
        topic, part, mx = row["_kafka_topic"], str(row["_kafka_partition"]), int(row["mx"])
        checkpoint.setdefault(topic, {})
        checkpoint[topic][part] = max(checkpoint[topic].get(part, -1), mx)
    save_checkpoint(checkpoint)

    print(
        f"[silver] batch={batch_id} | new={new_count} | "
        f"silver={clean_n} | quarantined={quar_n} | checkpoint advanced.",
        flush=True,
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("silver_incremental")
        # dynamic overwrite => only the current batch partition is replaced.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    try:
        main(spark)
    finally:
        spark.stop()
