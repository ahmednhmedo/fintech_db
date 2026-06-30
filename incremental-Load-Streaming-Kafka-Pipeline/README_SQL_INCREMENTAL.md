# SQL Incremental Loading Layer — `fintech-lakehouse`

Extends the incremental pipeline from Silver Parquet into the existing SQL Server
star schema, **without modifying** any earlier stage or the batch SQL loader.

```
data/incremental/silver/silver_batch_id=<b>/        (PySpark Silver output)
        │   one NEW batch at a time
        ▼
stg.fact_transactions_stage      truncate + bulk load (pyodbc fast_executemany)
        │   EXEC stg.usp_merge_incremental_fact @silver_batch_id
        ▼
   ┌────────────── inside ONE server-side transaction ──────────────┐
   │  validate ──► stg.fact_transactions_reject   (bad rows quarantined)
   │  MERGE    ──► dbo.fact_transactions          (idempotent upsert)
   │  audit    ──► stg.incremental_load_log       (SUCCESS / FAILED)
   └────────────────────────────────────────────────────────────────┘
```

Deliverables (all new; nothing existing changed):
```
incremental/load_silver_incremental_to_sql.py   # orchestrator (host, Windows auth)
incremental/sql/create_incremental_staging.sql  # schema + staging/reject/log tables
incremental/sql/merge_incremental_fact.sql       # stg.usp_merge_incremental_fact proc
incremental/README_SQL_INCREMENTAL.md            # this file
```
No extra helper files were needed.

---

## Why a staging table (not loading straight into the fact)

Loading directly into `dbo.fact_transactions` would be wrong here for four reasons:

1. **Validate before you touch the warehouse.** A streamed fact can reference a
   dimension key that doesn't exist, carry a null business key, or contain an
   intra-batch duplicate. Staging lets us catch and **quarantine** those rows
   *before* the MERGE, so one bad row can't abort the whole load with a
   foreign-key violation.
2. **MERGE needs a set, server-side.** Bulk-loading the batch into a table and
   then running one set-based `MERGE` is dramatically faster and safer than
   row-by-row upserts issued from Python.
3. **Crash isolation.** Staging is truncated every run, so a half-finished load
   leaves **no** partial state in the warehouse.
4. **A clean seam for retries/audit.** Staging + the load log give an auditable,
   replayable boundary between "data has arrived" and "data is in the warehouse."

---

## Reading Silver — loading strategy

The loader reads only `data/incremental/silver/`. Silver is Hive-partitioned by
`silver_batch_id`, so each partition folder *is* one micro-batch. The strategy:

1. **Discover** all `silver_batch_id=…` folders on disk.
2. **Subtract** the batches already marked `SUCCESS` in `stg.incremental_load_log`.
3. **Process the remaining batches one at a time, in sorted order.** Each batch
   is read from its own partition folder (so we never scan the whole Silver tree),
   bulk-loaded into staging, and MERGE-d independently.

Processing one batch per transaction is what makes a backlog **restart-safe**: if
the loader dies on batch 3, batches 1–2 are already committed and audited, and
the next run resumes cleanly at batch 3.

---

## Validation before MERGE (quarantine, never silently drop)

Inside the proc, each staged row gets a `reject_reason` (first failing rule wins):

| Reason | Rule |
|---|---|
| `null_transaction_id` | `transaction_id` null/blank |
| `duplicate_transaction_id_in_staging` | >1 row per `transaction_id` (keep latest by `_silver_processed_at_utc`) |
| `missing_required_key` | `date_key` / `time_key` / `account_key` / `transaction_type_key` null |
| `null_measure` | `amount_minor` / `amount_egp` / `abs_amount_egp` null |
| `fk_*_not_found` | a (non-null) dimension key has no match in the `dbo` dimension |

Rejected rows are **inserted into `stg.fact_transactions_reject`** with their
reason and excluded from the MERGE; valid rows proceed. Nothing is dropped — bad
data is auditable and replayable, mirroring the Silver quarantine philosophy.
De-duplication is not optional here: `MERGE` errors if its source has two rows
for the same target key, so the dedupe rule is what keeps the MERGE legal.

---

## MERGE strategy — clause by clause

```sql
MERGE dbo.fact_transactions WITH (HOLDLOCK) AS tgt     -- (1)
USING (SELECT … FROM #batch WHERE reject_reason IS NULL) AS src   -- (2)
   ON tgt.transaction_id = src.transaction_id          -- (3)
WHEN MATCHED AND (<any column differs, NULL-safe>)      -- (4)
     THEN UPDATE SET tgt.<all 17 non-key cols> = src.<…>
WHEN NOT MATCHED BY TARGET                              -- (5)
     THEN INSERT (<18 business cols>) VALUES (src.<…>)
OUTPUT $action INTO @actions;                           -- (6)
```

1. **`WITH (HOLDLOCK)`** — takes a serializable range lock so the “does this
   transaction_id exist?” check and the INSERT are atomic; closes the classic
   MERGE race under concurrency.
2. **`USING (… WHERE reject_reason IS NULL)`** — the source is only the *valid,
   de-duplicated* rows of this batch. Source key is therefore unique → MERGE legal.
3. **`ON transaction_id`** — the business/natural key (backed by the warehouse’s
   `UNIQUE` index `UX_fact_transactions_natural_key`), **not** the surrogate
   `transaction_sk`.
4. **`WHEN MATCHED AND (<diff>)`** — an existing transaction is updated **only if
   a value actually changed** (NULL-safe comparison across all 17 non-key
   columns). Transactions are largely immutable, so a pure at-least-once replay
   matches with identical values and updates **nothing** (no churn / no
   unnecessary index maintenance). The update path remains for genuine late
   corrections.
5. **`WHEN NOT MATCHED BY TARGET`** — a new `transaction_id` is inserted;
   `transaction_sk` is the warehouse IDENTITY, so it is omitted and generated.
6. **`OUTPUT $action`** — captures per-row INSERT/UPDATE so we can log exact
   counts.

There is **deliberately no `WHEN NOT MATCHED BY SOURCE`** clause: this is an
incremental load, so we must never delete warehouse rows just because they are
absent from the current batch.

---

## Idempotency — guaranteed two ways

* **Batch level:** a `silver_batch_id` with a `SUCCESS` row in
  `stg.incremental_load_log` is skipped. A filtered `UNIQUE` index
  (`… WHERE status='SUCCESS'`) makes “loaded twice” physically impossible.
* **Row level:** the MERGE keys on `transaction_id` and only updates on a real
  diff. Even if a batch is *forced* to reprocess (e.g. after a failure), it
  inserts/updates nothing it already applied.

Re-running the loader with no new batches logs *“Nothing to do”* and writes zero
rows.

---

## Transactions & XACT_ABORT

The validate → MERGE → audit steps run in **one** `BEGIN TRANSACTION … COMMIT`
inside `BEGIN TRY/CATCH`, with **`SET XACT_ABORT ON`**:

* **`BEGIN TRANSACTION`** opens the unit of work.
* **`COMMIT`** persists the MERGE *and* the `SUCCESS` audit row together — so a
  batch is marked done only if its data actually landed (atomic).
* **`ROLLBACK`** (in `CATCH`) undoes any partial MERGE on error.
* **`XACT_ABORT ON`** is enabled because the batch issues several statements in
  one transaction: with it on, *any* runtime error aborts the entire batch and
  rolls back, instead of risking a half-applied, still-open transaction. For a
  multi-statement data-loading transaction this should be **ON** — it is the
  difference between all-or-nothing and partial corruption.

The `FAILED` audit row is written **after** the rollback (its own auto-committed
statement) so the failure is always recorded, then `THROW` re-raises to Python.

---

## Logging

The loader logs per batch: batch id, **rows staged**, **rows inserted**, **rows
updated**, **rows rejected**, and overall **execution time**; the proc persists
the same counts plus `started/finished_at_utc`, `duration_ms`, and (on failure)
`error_message` into `stg.incremental_load_log` — a durable, queryable audit
trail.

---

## Metadata — what stays where

| Column | In staging? | In the fact table? | Why |
|---|---|---|---|
| `silver_batch_id` | **Yes** | No | Drives batch selection + audit; not an analytical attribute. |
| `_silver_batch_id` | No | No | Exact duplicate of `silver_batch_id`; carrying both adds nothing. |
| `_silver_processed_at_utc` | **Yes** | No | Lineage/ordering for dedupe + audit; not a business measure. |

The warehouse fact keeps its **existing, fixed schema** — pure business columns at
the transaction grain. Ingestion lineage lives in **staging + the load log**,
where it is auditable without polluting the star schema. (If lineage in the fact
were ever required, it would be added as explicit audit columns, not as a grain
change — out of scope here.)

---

## How to run

Prereqs: the star schema + dimensions already exist in the target DB (they do in
`fintech_db`), and at least one Silver batch exists under
`data/incremental/silver/`. Uses the same Python 3.11 venv + ODBC Driver 18 +
Windows auth as the batch loader (`pyodbc`, `pyarrow` — both already installed
for the batch pipeline).

```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse"
# point at your warehouse (defaults shown)
$env:WH_SERVER   = "ahmed\SQLEXPRESS"
$env:WH_DATABASE = "fintech_db"
python incremental\load_silver_incremental_to_sql.py
```
The loader auto-applies `create_incremental_staging.sql` and
`merge_incremental_fact.sql` (both idempotent) before processing batches.

---

## Validation checklist

- [ ] **Setup is idempotent** — running the loader twice creates the `stg` objects
      once; second run logs *“Nothing to do.”*
- [ ] **Staging row count** == the batch’s Silver row count.
- [ ] **Inserted rows** — first load of a batch inserts all valid rows;
      `dbo.fact_transactions` grows by exactly that count.
- [ ] **Rejected rows** — invalid rows (e.g. an unknown `account_key`) land in
      `stg.fact_transactions_reject` with a `reject_reason`, not in the fact.
- [ ] **No data loss** — `rows_valid + rows_rejected == rows_staged` (per the load
      log): every staged row is either valid or quarantined, none vanish. Note
      that `rows_inserted + rows_updated <= rows_valid` — an identical replay
      produces *valid* rows that MERGE matches with no change, so they emit no
      INSERT/UPDATE action and are (correctly) not counted. (So
      `inserted + updated + rejected == staged` holds only on a first load, and
      is **not** the right no-data-loss check.)
- [ ] **Duplicate rerun** — re-running the loader skips the SUCCESS batch and
      inserts/updates **0** rows.
- [ ] **Update path** — change one fact row + replay the batch → exactly **1
      update**, 0 inserts, and the other rows are untouched (no unnecessary updates).
- [ ] **Failed rerun / rollback** — force an error mid-MERGE → the transaction
      rolls back, the fact is unchanged, the log shows `FAILED`, and the loader
      exits non-zero; a later clean run loads the batch successfully.
- [ ] **Referential integrity** — every loaded fact row resolves to existing
      dimensions; no orphan FKs.
- [ ] **No duplicate transaction_id** — `COUNT(*) == COUNT(DISTINCT transaction_id)`
      in `dbo.fact_transactions`.

Handy queries:
```sql
SELECT * FROM stg.incremental_load_log ORDER BY log_id DESC;
SELECT reject_reason, COUNT(*) FROM stg.fact_transactions_reject GROUP BY reject_reason;
SELECT COUNT(*) AS fact_rows, COUNT(DISTINCT transaction_id) AS distinct_ids FROM dbo.fact_transactions;
```

---

## Next phase (explanation only — NOT implemented here)

```
dbo.fact_transactions (now incrementally loaded)
        │
        ▼
dbt Incremental Models   materialized='incremental', unique_key='transaction_id'
        │
        ▼
Power BI Desktop         incremental / scheduled refresh of the Gold star schema
```
dbt would read the warehouse fact and build Gold marts incrementally on the same
`transaction_id` key — the same idempotency guarantee, one layer up. Out of scope
for this stage.
