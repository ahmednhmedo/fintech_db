# Silver Incremental Layer — `fintech-lakehouse`

Extends the incremental pipeline one stage further, **without touching** the
batch pipeline or the Bronze incremental code (`producer.py`, `consumer.py`,
`docker-compose.yml`, FastAPI, Kafka config).

```
Bronze Incremental JSONL                          (data/incremental/bronze/transactions_raw.jsonl)
        │   micro-batch, offset-incremental
        ▼
silver_incremental.py  (PySpark)
        │
        ├──►  Silver Incremental Parquet           (data/incremental/silver/)
        └──►  Quarantine Parquet (bad rows)         (data/incremental/quarantine/)
                                                    checkpoint: data/incremental/_checkpoints/silver_offsets.json
```

It applies the **same conforming transformations as the batch
`spark/bronze_to_silver.py`** (trim, `''→NULL`, type casts, dedupe on
`transaction_id`) and adds the things a streaming layer needs: incremental offset
tracking, data-quality quarantine, and lineage metadata.

---

## Why these design choices

### Incremental strategy — processed Kafka offset checkpoint
Every Bronze row carries its `_kafka_partition` / `_kafka_offset`. Offsets are
**monotonic and unique per partition**, so they are the ideal watermark. The job
stores the highest processed offset per `(topic, partition)` in
`_checkpoints/silver_offsets.json` and each run keeps only rows **above** that
mark. Chosen over alternatives because it is the *simplest production-quality*
option for a Kafka-fed Bronze: no extra database, exact (no ties like a
timestamp watermark), and survives the Bronze file being rotated/rewritten
(unlike a line-count). First run (no checkpoint) processes everything.

### Idempotency — running twice never duplicates
* **Normal re-run** (no new events): the checkpoint reports 0 new offsets → the
  job writes nothing and exits.
* **Crash between Parquet write and checkpoint update**: `_silver_batch_id` is
  **derived deterministically** from the processed offset range
  (`b_<minOffset>_<maxOffset>`). A re-run sees the un-advanced checkpoint,
  reprocesses the identical offsets, computes the identical batch id, and writes
  to the **same partition with dynamic partition overwrite** — replacing it in
  place instead of appending duplicates.
* Any duplicate `transaction_id` from at-least-once delivery that spans runs is
  finally collapsed downstream by **SQL MERGE / dbt `unique_key`**.

### Quarantine instead of dropping
Dropped rows are gone forever — you can't audit, alert, or backfill them, and in
a regulated financial system silently losing a transaction is unacceptable.
Invalid rows are written to `data/incremental/quarantine/` with a `_dq_reason`
so they can be inspected, fixed at source, and replayed. Bad data becomes
*visible* rather than *lost*.

### Partitioning — by `_silver_batch_id`
Each micro-batch is written as its own partition. Why: (1) each run is
self-contained → a single failed batch can be reloaded into SQL staging without
touching others; (2) it enables the dynamic-overwrite idempotency trick above;
(3) it avoids the **small-file explosion** you'd get partitioning a tiny
per-second micro-batch by event date (which would scatter ~10 rows across many
date folders every run). Event-date partitioning belongs in the Gold layer,
where data volumes per partition are large.

### Metadata — `_silver_processed_at_utc`, `_silver_batch_id`
Bronze lineage (`_kafka_topic/partition/offset`, `_consumed_at_utc`) is preserved
so every Silver row is traceable back to the exact Kafka message. The Silver
columns add **when** the row was conformed and **which batch** produced it —
essential for debugging late/duplicate data, reconciling counts, and driving the
downstream SQL incremental load by batch.

---

## Deliverables added
```
incremental/silver_incremental.py          # the PySpark job
incremental/requirements_incremental.txt   # new dep: pyspark
incremental/README_SILVER_INCREMENTAL.md   # this file
```
Outputs are created on first run:
```
data/incremental/silver/silver_batch_id=b_.../ *.parquet
data/incremental/quarantine/silver_batch_id=b_.../ *.parquet      (only if bad rows exist)
data/incremental/_checkpoints/silver_offsets.json
```
> Note: the partition folder is `silver_batch_id=...` (no leading underscore).
> A `_`-prefixed folder is treated as hidden by pandas/pyarrow and Spark, which
> would make directory reads return 0 rows. The underscore-prefixed
> `_silver_batch_id` is still kept inside the Parquet as a metadata column.

---

## How to run

Prereqs: Bronze JSONL already has data (run the Bronze stack from
`README_INCREMENTAL.md`). PySpark needs a JDK on PATH.

> `pandas` and `pyarrow` are **not** required to run the Silver job — they are
> only used for the local inspection/validation snippet below. Install them
> (`pip install pandas pyarrow`) only if you want to read the Parquet output by
> hand; the pipeline itself does not depend on them.

### Option A — host venv (Python 3.11)
```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\incremental"
.\.venv-incremental\Scripts\Activate.ps1
pip install -r requirements_incremental.txt
python silver_incremental.py
```
> Windows note: writing Parquet locally needs Hadoop `winutils.exe` with
> `HADOOP_HOME` set. If you hit a `winutils`/`NativeIO` error, use Option B.

### Option B — Dockerized Spark (zero local Spark setup)
Runs the exact same script in an `apache/spark` container, with the project
`data/` and `incremental/` folders mounted and paths pointed at `/data`:
```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse"
docker run --rm `
  -v "${PWD}\data:/data" `
  -v "${PWD}\incremental:/app" `
  -e BRONZE_JSONL=/data/incremental/bronze/transactions_raw.jsonl `
  -e SILVER_DIR=/data/incremental/silver `
  -e QUARANTINE_DIR=/data/incremental/quarantine `
  -e CHECKPOINT_FILE=/data/incremental/_checkpoints/silver_offsets.json `
  apache/spark:3.5.1 /opt/spark/bin/spark-submit /app/silver_incremental.py
```

---

## Validation checklist

- [ ] **Bronze detected**: first run logs `new=<N>` with N = Bronze line count.
- [ ] **Only new processed**: add more events, re-run → `new=` only the delta.
- [ ] **Re-run is a no-op**: run again with no new events →
      `0 new Bronze records — already up to date. Nothing written.`
- [ ] **Duplicates removed**: a repeated `transaction_id` appears once in
      `silver/`; the extra copy lands in `quarantine/` with
      `_dq_reason=duplicate_transaction_id`.
- [ ] **Invalid quarantined**: a row missing `transaction_id` / amount / date_key
      goes to `quarantine/` (with its `_dq_reason`), not `silver/`.
- [ ] **No data loss**: `silver` row count + `quarantine` row count == new rows.
- [ ] **Parquet created**: `data/incremental/silver/silver_batch_id=.../` exists.
- [ ] **Types conformed**: `amount_egp` is decimal, keys are int/long, `''`→null.
- [ ] **Metadata present**: every Silver row has `_kafka_*`, `_consumed_at_utc`,
      `_silver_processed_at_utc`, `_silver_batch_id`.
- [ ] **Checkpoint advanced**: `_checkpoints/silver_offsets.json` shows the new
      high-water offset per partition.

Inspect the output quickly (reads the partitioned directory directly — works
now that the partition folder is `silver_batch_id=...`). Needs `pandas` +
`pyarrow`, used here only for inspection: `pip install pandas pyarrow`.
```powershell
.\.venv-incremental\Scripts\python.exe -c "import pandas as pd; df=pd.read_parquet(r'..\data\incremental\silver'); print(df.shape); print(df.dtypes); print(df.head())"
```
The `silver_batch_id` partition value is restored as a column on read, and the
`_silver_batch_id` metadata column is present inside the files.

---

## Future integration (explanation only — NOT implemented here)
```
Silver Incremental Parquet
   │   load one micro-batch (by _silver_batch_id) into a staging table
   ▼
SQL Incremental Staging   (stg_fact_transactions)
   ▼
SQL MERGE                 MERGE stg INTO fact ON transaction_id
   ▼                        (insert new, update/ignore existing -> idempotent)
dbt Incremental Models    materialized='incremental', unique_key='transaction_id'
   ▼
Power BI Desktop          incremental refresh of the Gold star schema
```
The Silver layer only *conforms and lands* data; final de-duplication across
runs (from at-least-once delivery) is enforced by the **SQL MERGE** key and
**dbt's `unique_key`**, which is exactly why offset-based at-least-once
processing here is safe.
