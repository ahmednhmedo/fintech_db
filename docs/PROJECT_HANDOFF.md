# FinTech Data Warehouse — Engineering Handoff

Permanent handoff for the FinTech lakehouse pipeline. Reflects the **frozen, approved**
state of the project. Read **§ START HERE** (bottom) first if you're continuing the work.

> Workspace root: `f:\NTI FOLDER\NTI FINAL PROJECTssssssssssssssss`
> Pipeline project: `fintech-lakehouse/`
> Warehouse: SQL Server `ahmed\SQLEXPRESS` (instance `Ahmed-PC\SQLEXPRESS`, v17.0 / 2025 Express), database `fintech_db`.

---

## 1. Project Overview

**Purpose.** Build an end-to-end **hybrid local lakehouse** that takes synthetic Egyptian
FinTech transaction data (8 CSVs ≈ 1.04M rows) from raw files through a medallion pipeline
(Bronze → Silver → Gold star) into SQL Server, validated by dbt, and ready for a Power BI
dashboard. It's a final-project deliverable that demonstrates the modern data stack
(Docker, Airflow, PySpark, dbt, SQL Server, Power BI) running locally.

**Architecture (one line).** Docker (Airflow + PySpark) builds Bronze/Silver as Parquet; the
Windows host (sqlcmd + a pyodbc loader + dbt-sqlserver, all Windows auth) loads the Gold star
into `fintech_db` and runs the dbt quality gate; Power BI Desktop reads the Gold star.

**Technology stack.**
- Docker Desktop (WSL2), Docker Compose v2.
- Apache Airflow 2.9.3 (LocalExecutor; Postgres 15 metadata DB).
- PySpark 3.5.1 on OpenJDK 17 (Spark **local mode** inside the Airflow image).
- pandas 2.2.2 (Bronze extract), pyarrow + pyodbc 5.1.0 (Silver→SQL loader).
- SQL Server 2025 Express (`ahmed\SQLEXPRESS`), **Windows Authentication only**.
- dbt-core 1.8.9 / dbt-sqlserver 1.8.4 (dbt-fabric **1.8.7**, ODBC Driver 18), Python 3.11 host venv.
- Power BI Desktop (Import mode, SQL Server connector).

**Why this architecture.** The host SQL Server permits **Windows Authentication only** (no SQL
logins, no mixed mode). A Linux container cannot cleanly do Windows/integrated auth, so the
pipeline is **deliberately split**: Docker (Linux) owns Bronze/Silver (file compute), the
**Windows host** owns Gold + dbt (anything touching SQL Server). This is the core design
constraint that shaped everything.

**Current implementation status.** ✅ **Complete and frozen through Phase 8** (full end-to-end
pipeline runs from one command, `\.run_all.ps1`, EXIT=0). Power BI is **planned & documented**
(not yet built). Production reliability hardening done. Overall readiness ~8.5/10 (local hybrid).

---

## 2. Current Pipeline Status (completed phases)

> Data generation (pre-pipeline) produced the 8 CSVs in `dw_data/` and the frozen contract.

### Phase 0 — Data modeling + generation
- **Objective:** analyze the star (`fintech_dw_create_tables.sql`), research real Egyptian
  geography/merchants, generate non-uniform (Cairo/Giza/Alex-weighted) dims + a 1M-row fact with
  normally-distributed amounts (~100–1000 EGP), and validate.
- **Status:** ✅ done. **Validation:** `validate_datasets.py` — all tables PASS schema + FK checks.
- **Decisions:** ~40,000 accounts → ~25 txns/acct; amounts Normal(550,225) core; lineage/SCD deferred.
- **Files:** `generate_dimensions.py`, `generate_fact_transactions.py`, `validate_datasets.py` (root).

### Phase 1 — Environment verification
- **Objective:** verify Docker, SQL Server, ODBC18, Python 3.11, Java, dbt, dbt-sqlserver, dbt profile.
- **Status:** ✅ 10/10 green (Java not on host = OK; Spark uses the image's JDK).
- **Important fix:** the dbt profile `fintech` pointed at the **old** `fintech_wh` DB → created a
  **dedicated `fintech_db` profile** (home `~/.dbt/profiles.yml`), pointed `dbt_project.yml` at it,
  left the old `fintech` profile untouched.
- **Files:** `dbt/fintech/dbt_project.yml`, `dbt/fintech/profiles.yml.example`, `~/.dbt/profiles.yml`.

### Phase 2A/2B — Configuration review + cleanup ("Phase 2.5")
- **Objective:** audit the 8 config files; apply schema-neutral hygiene.
- **Status:** ✅ no real errors; three cleanups applied.
- **Fixes:** added `.gitignore`; `run_all.ps1` switched from a hardcoded container name to
  `docker compose exec airflow-scheduler`; **wired `.env`** so `WH_SERVER/WH_DATABASE/WH_DRIVER`
  are actually read.
- **Files:** `.gitignore` (new), `run_all.ps1`, `run_gold.ps1`, `.env.example`.

### Phase 3A/3B — Docker validation
- **Objective:** build image, bring up the stack, verify Airflow/Spark/mounts.
- **Status:** ✅ 10/10 (image built; Postgres healthy; UI 8081; DAG parses; Java 17; PySpark 3.5.1; mounts OK).
- **Critical fix:** the old project folder is **also** named `fintech-lakehouse` → same Compose
  project name → shared containers/volumes. Fixed by adding **`name: fintech_lakehouse_new`** to
  `docker-compose.yml` (isolated containers/network/volume); old project untouched.
- **Also added** (reliability, schema-neutral): a **named volume** `fintech_lakehouse_new_pgdata`,
  `depends_on: airflow-init service_completed_successfully` (deterministic startup), and an
  **idempotent `airflow-init`** (guarded `users create`).
- **Files:** `docker-compose.yml`.

### Phase 4A/4B — Bronze + lineage
- **Objective:** 8 CSV → Bronze Parquet.
- **Status:** ✅ all 8, counts match raw, nulls preserved, idempotent (`_fresh_dir`).
- **Enhancement:** added Bronze **lineage columns** `_ingested_at_utc` (one UTC timestamp per run)
  and `_source_file`. Verified once-per-run semantics + counts unchanged.
- **Files:** `ingestion/extract_to_bronze.py` (+ a clarifying comment in `spark/bronze_to_silver.py`).

### Phase 5A/5B — Silver
- **Objective:** Bronze Parquet → conformed/typed/deduped Silver Parquet (PySpark).
- **Status:** ✅ all 8, types match the warehouse contract, `''→NULL`, dedupe on key (0 dups, keys
  unique), **Bronze metadata dropped** via explicit projection, idempotent (overwrite).
- **Decisions:** BIT flags stored as int32 0/1 in Silver (Gold casts to BIT); dedupe tie-breaker deferred.
- **Files:** none (Silver job unchanged; one operational note — a stray `silver` *file* created by a
  bad `echo ->` redirect was removed; the job code was correct).

### Phase 6A/6B — SQL Server load (Silver → `[silver]` → `dbo.*`)
- **Objective:** load Silver into `[silver]`, then into the **existing** `dbo.*` star.
- **Status:** ✅ counts reconcile; 11 FKs restored; surrogate keys preserved; `transaction_sk` DB-generated.
- **Fixes (ETL, not schema):** (a) loader `Dataset.to_batches(max_chunksize=…)` → **`batch_size=…`**;
  (b) `load_gold.sql` needs **`SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON`** because
  `fact_transactions` has **filtered indexes** (`WHERE is_declined=1` / `WHERE is_fx=1`).
- **Files:** `ingestion/load_silver_to_sqlserver.py`, `sql/load_gold.sql`.

### Phase 7A/7B — dbt quality gate + reporting view
- **Objective:** run `dbt debug` + `dbt build` against `dbo.*`.
- **Status:** ✅ **39/39** tests pass; `reporting.rpt_daily_transactions` view created; zero Gold mutation; no seeds.
- **Fixes:** (a) `_sources.yml` relationships were inline-flow `{ to: source('gold','x'), field:y }` —
  the comma inside `source(...)` broke YAML → converted to **block style**; (b) renamed deprecated
  **`tests:` → `data_tests:`** (cleared the dbt 1.8 deprecation warning).
- **Files:** `dbt/fintech/models/_sources.yml`.

### Phase 8A/8B — Full end-to-end + reliability hardening
- **Objective:** run the whole pipeline via `\.run_all.ps1` and validate end-to-end.
- **Status:** ✅ EXIT=0; raw→Bronze→Silver→`[silver]`→`dbo` reconcile; 11 FKs; `transaction_sk`
  identity 1→1,000,000; dbt 39/39; reporting view present.
- **Hardening (schema-neutral):** (a) `load_gold.sql` made **atomic** (`SET XACT_ABORT ON` +
  `BEGIN TRAN`/`COMMIT`, intermediate `GO`s removed) → all-or-nothing reload; (b) `run_gold.ps1`
  **fail-fast** (`Invoke-Sql` using `sqlcmd -b` + `$LASTEXITCODE` checks after every critical step).
- **Files:** `sql/load_gold.sql`, `run_gold.ps1`.

### Phase 9 (in progress) — Power BI readiness & docs
- Verified SQL readiness; produced build guide + model diagram. **Power BI not yet built.**
- **Files:** `docs/POWER_BI.md`, `docs/POWERBI_MODEL_DIAGRAM.md`, this `docs/PROJECT_HANDOFF.md`.

---

## 3. Final Architecture (Raw CSV → Power BI)

```
 8 CSVs (dw_data/, copied to fintech-lakehouse/data/raw/)
    │
    ▼  [Docker: Airflow orchestrates, runs in the scheduler container]
 BRONZE  ingestion/extract_to_bronze.py (pandas)  → data/lake/bronze/<t>/*.parquet  (+ lineage cols)
    │
    ▼  [Docker: PySpark local mode, spark-submit]
 SILVER  spark/bronze_to_silver.py  → data/lake/silver/<t>/*.parquet  (typed, ''→NULL, deduped, lineage dropped)
    │
    ▼  ===== hand-off via the ./data bind mount =====  (container path /opt/data == host ./data)
    │
    ▼  [Windows HOST: run_gold.ps1, Windows auth]
 [silver]  ingestion/load_silver_to_sqlserver.py (pyodbc)  → fintech_db.silver.*
    │
    ▼  drop_star_constraints.sql → load_gold.sql (atomic) → add_star_constraints.sql (sqlcmd)
 GOLD  fintech_db.dbo.*  (existing DDL star: 7 dims + fact_transactions, IDENTITY_INSERT, 11 FKs)
    │
    ▼  [Windows HOST: dbt-sqlserver, Windows auth]
 dbt  39 data-quality tests on dbo.* + builds reporting.rpt_daily_transactions (view)
    │
    ▼  [Windows HOST: Power BI Desktop, Import, Windows auth]
 POWER BI  star model over dbo.* (+ optional reporting view)
```

**Responsibilities.**
- **Docker** — runs Airflow (LocalExecutor) + its Postgres metadata DB + the PySpark runtime
  (Java 17). Hosts **Bronze + Silver only**. No SQL Server, no dbt inside.
- **Airflow** — orchestration brain for the container side. DAG `fintech_lakehouse`
  (`extract → spark silver → gold_handoff`). NOTE: `run_all.ps1` currently drives Bronze/Silver
  via `docker compose exec` (the DAG is the UI/demonstration path).
- **PySpark** — Silver layer: reads Bronze Parquet, casts to the warehouse contract, normalizes
  empties to NULL, dedupes on the key, drops Bronze metadata via explicit projection.
- **Bronze** — faithful raw landing (string-typed) of the 8 CSVs as Parquet + lineage columns.
- **Silver** — conformed, typed, deduped Parquet — the contract boundary for the warehouse.
- **SQL Server `[silver]`** — ODS/staging schema; the typed landing zone loaded from Silver Parquet
  (pyodbc, Windows auth) so the Gold load is a fast in-DB set-based operation.
- **Gold warehouse (`dbo.*`)** — the existing Kimball star (your DDL): `fact_transactions` (atomic
  transaction grain) + 7 dims, with PKs, 11 FKs, IDENTITY surrogate keys, filtered indexes.
  Loaded by truncate + `INSERT…SELECT` with `IDENTITY_INSERT`; `transaction_sk` is DB-generated.
- **dbt** — the **data-quality gate**: source tests (unique / not_null / relationships /
  accepted_values) on `dbo.*`, plus a thin reporting view. It does **not** build dimensions.
- **Reporting schema** — `reporting.rpt_daily_transactions`, a dbt-built daily-aggregate **view**
  (optional convenience for the Power BI overview trend).
- **Power BI** — Import-mode star model over `dbo.*`; relationships, DAX measures, dashboards
  (see `docs/POWER_BI.md`).

---

## 4. Current Constraints (MUST NOT change — frozen)

- **Gold schema / existing DDL** (`sql/01_create_star.sql` ≡ root `fintech_dw_create_tables.sql`) — do not alter tables/columns.
- **PKs** — every dim PK + `fact_transactions.transaction_sk`.
- **FKs** — all **11** star foreign keys (names + definitions in `add_star_constraints.sql`).
- **SKs** — surrogate keys: dim `*_key` (IDENTITY, loaded via `IDENTITY_INSERT`); `transaction_sk` (BIGINT IDENTITY, DB-generated).
- **Relationships** — the star/outrigger relationships (incl. role-playing `dim_account`, outrigger `dim_location`/`dim_date`).
- **Grain** — `fact_transactions` = one row per transaction event (incl. declined attempts). Dims at their natural grain.
- **Power BI-facing tables** — `dbo.*` structure is the BI contract; don't add/remove columns.
- **Windows Authentication only** — no SQL logins, no mixed mode (`sqlcmd -E`, `windows_login: true`, pyodbc `Trusted_Connection`).
- **`fintech_wh` isolation** — the old project's DB and its `fintech` dbt profile must stay untouched; everything here targets `fintech_db` / profile `fintech_db` / project name `fintech_lakehouse_new`.

Server `ahmed\SQLEXPRESS`, database `fintech_db`, dbt profile `fintech_db` are fixed.

---

## 5. Production Hardening Already Implemented

1. **Dedicated dbt profile `fintech_db`** — isolates from the old project's shared `fintech` profile (which targeted `fintech_wh`), preventing wrong-DB builds.
2. **Compose project name `fintech_lakehouse_new`** — avoids container/volume/network collision with the identically-named old project folder.
3. **Named Postgres volume `fintech_lakehouse_new_pgdata`** — durable, predictable metadata persistence (anonymous volumes can be pruned).
4. **`depends_on: airflow-init service_completed_successfully`** — webserver/scheduler wait for migration+seed user, removing a startup race.
5. **Idempotent `airflow-init`** — guarded `users create` so re-runs against the persistent volume don't fail on a duplicate admin (works with #4).
6. **Bronze lineage columns** (`_ingested_at_utc`, `_source_file`) — auditability/traceability; one timestamp per run.
7. **Silver explicit projection** — only contracted columns reach Silver/Gold (Bronze metadata never leaks downstream).
8. **`SET QUOTED_IDENTIFIER ON` / `ANSI_NULLS ON`** in `load_gold.sql` — required to INSERT into the filtered-index fact table regardless of `sqlcmd` defaults.
9. **Atomic Gold load** — `XACT_ABORT ON` + `BEGIN TRAN`/`COMMIT` → a mid-load failure rolls back instead of leaving partial Gold state.
10. **Fail-fast `run_gold.ps1`** — `sqlcmd -b` + `$LASTEXITCODE` checks (`Invoke-Sql`) halt the run on any SQL error instead of continuing silently.
11. **`.gitignore`** — keeps `data/lake/`, logs, `target/`, `.env` out of version control.
12. **Wired `.env`** + `docker compose exec` (no hardcoded container name) — config is real and folder-rename-safe.
13. **`data_tests:` (not deprecated `tests:`)** — dbt 1.8 deprecation cleared.

---

## 6. Validation Summary (last full run, deterministic — frozen data)

**Row-count reconciliation (raw → Bronze → Silver → `[silver]` → `dbo`) — all equal:**

| Table | Rows |
|---|---|
| dim_date | 731 |
| dim_time | 1,440 |
| dim_location | 19 |
| dim_account | 40,000 |
| dim_merchant | 1,200 |
| dim_transaction_type | 13 |
| dim_decline_reason | 8 *(2 rows intentionally deleted by the user — fact references only the remaining 8; no orphans)* |
| fact_transactions | 1,000,000 |

- **FK validation:** **11/11** restored; `ADD CONSTRAINT … WITH CHECK` succeeded → referential integrity validated (incl. `FK_fact_decline_reason` vs the 8-row dim).
- **dbt tests:** **39/39 PASS** (38 data tests + 1 view), `WARN=0 ERROR=0 SKIP=0`. Coverage: unique (PKs + `account_id/merchant_id/transaction_id`), not_null, relationships (7 fact→dim), accepted_values (`time_bucket`, `account_status`, `customer_tier`, `merchant_size`, `is_outbound/declined/fx`).
- **Reporting view:** `reporting.rpt_daily_transactions` exists (1 view).
- **Surrogate-key validation:** `account_key` 1–40,000, `location_key` 1–19 — preserved exactly via `IDENTITY_INSERT`.
- **Identity validation:** `fact_transactions.transaction_sk` `is_identity=1`, range **1 → 1,000,000** (DB-generated, contiguous).
- **NULL semantics validated:** NOT-NULL columns = 0 nulls; nullable columns NULL where "not applicable" (peer 845,333 / decline 957,522 / merchant 300,131 / fx_rate 956,217); flag↔FK consistency = 0 violations.
- **KPI benchmarks:** Gross Volume **689,181,271 EGP**, Net Flow **-392,965,266**, Avg Ticket **689.18**, Declined **42,478 (4.25%)**, FX **43,783 (4.38%)**, distinct accounts **39,795**, distinct merchants **1,200**, P2P **154,667**.

---

## 7. Remaining Work

**Immediate**
- Build the Power BI report following `docs/POWER_BI.md` + `docs/POWERBI_MODEL_DIAGRAM.md`; mark `dim_date` as Date Table; set `peer_account_key` / signup / opened relationships inactive; verify KPI cards match §6 benchmarks.

**Future improvements**
- Make Airflow the **real orchestrator** of the e2e run (trigger Bronze/Silver via the DAG, add retries/alerting/lineage).
- Incremental / MERGE loads + SCD + history (replace full truncate-reload).
- Persisted **run-audit table** + automated count-reconciliation gate that fails the run on mismatch.
- CI running `dbt build`; broader dbt tests (measure/business-rule checks: amount sign vs `is_outbound`, `abs_amount_egp >= 0`); dbt **source freshness**.
- Deterministic Silver dedupe tie-breaker (`row_number()` over key by `_ingested_at_utc`).

**Optional enterprise improvements**
- Cloud migration to Microsoft Fabric (managed identity instead of Windows auth, `COPY INTO`, fully containerized — removes the host/Docker split).
- `bcp`/`BULK INSERT` for faster loads; blue-green/partition-swap to remove the FK-less/empty load window.
- Distributed Spark (cluster) if data grows beyond single-node.
- Power BI semantic model / role tables for `dim_location` (Customer vs Merchant geo).

---

## 8. Files Modified Throughout (and why)

**Data generation (workspace root)**
- `generate_dimensions.py` — generates date/location/account/merchant dims (Cairo/Giza/Alex-weighted, correlated attributes, signup growth curve).
- `generate_fact_transactions.py` — 1M fact + dim_time/transaction_type/decline_reason; Normal(~550,225) amounts; flags/FK consistency.
- `validate_datasets.py` — schema/type/length/PK/FK validator; later made tolerant of an optional DB-generated `transaction_sk` column.

**Pipeline infra (`fintech-lakehouse/`)**
- `Dockerfile` — Airflow 2.9.3 + JDK + pyspark/pyarrow/pandas (Bronze/Silver runtime).
- `docker-compose.yml` — **`name: fintech_lakehouse_new`**, named volume `pgdata`, `depends_on service_completed_successfully`, idempotent `airflow-init`, LocalExecutor, mounts.
- `requirements.txt` — host deps (dbt-core/sqlserver/fabric pins, pyodbc, pyarrow, pandas).
- `.env.example` — wired host warehouse vars (Windows auth defaults); container paths informational.
- `.gitignore` — excludes lake/logs/target/.env.

**Bronze / Silver / loader**
- `ingestion/extract_to_bronze.py` — Bronze extract + **lineage columns**.
- `spark/bronze_to_silver.py` — Silver conform/dedupe; explicit projection (drops lineage).
- `ingestion/load_silver_to_sqlserver.py` — Silver Parquet → `[silver]` (pyodbc, Windows auth); **`batch_size` fix**.

**SQL (Gold)**
- `sql/01_create_star.sql` — copy of the existing DDL (used only if the star is missing).
- `sql/setup_warehouse.sql` — ensures `[silver]` schema.
- `sql/load_gold.sql` — `[silver]`→`dbo.*`; **`QUOTED_IDENTIFIER/ANSI_NULLS ON`** + **atomic transaction**.
- `sql/drop_star_constraints.sql` / `add_star_constraints.sql` — FK drop/re-add (11 FKs).

**Orchestration / dbt / docs**
- `dags/fintech_pipeline_dag.py` — Airflow DAG (extract → spark silver → gold_handoff).
- `dbt/fintech/dbt_project.yml` — **`profile: fintech_db`**, reporting view config.
- `dbt/fintech/profiles.yml.example` — `fintech_db` profile template (Windows auth).
- `dbt/fintech/macros/generate_schema_name.sql` — verbatim custom schema names.
- `dbt/fintech/models/_sources.yml` — gold sources + tests; **block-style relationships**, **`data_tests:`**.
- `dbt/fintech/models/reporting/rpt_daily_transactions.sql` — daily reporting view.
- `run_all.ps1` — one-command e2e; `docker compose exec` (no hardcoded name).
- `run_gold.ps1` — host Gold runner; `.env` loader; **fail-fast `Invoke-Sql`**.
- `HOW_TO_RUN.md`, `docs/POWER_BI.md`, `docs/POWERBI_MODEL_DIAGRAM.md`, `docs/PROJECT_HANDOFF.md`.

**Host (not in repo)**
- `C:\Users\ahmed\.dbt\profiles.yml` — **appended** the `fintech_db` profile (old `fintech` profile preserved).

---

## 9. Known Design Decisions (WHY)

- **Windows Authentication** — the host SQL Server allows only Windows/integrated auth; no SQL logins, no mixed mode were permitted. All SQL access uses Trusted Connection.
- **Hybrid architecture (Docker + host)** — Linux containers can't do Windows auth, so Bronze/Silver run in Docker and Gold/dbt run on the Windows host. This split is the root rationale for `run_all.ps1` glue and the Airflow-can't-own-the-whole-pipeline reality.
- **No seeds** — the dimensions (`dim_decline_reason`, `dim_transaction_type`, etc.) are **real source tables** already present in the CSVs and loaded into `dbo`. The old project's seeds would inject *inconsistent* reference data and non-DDL tables; there is no `dim_currency` to seed (FX handled per-transaction).
- **Load into existing DDL** — the user pre-created the star (with IDENTITY/FK/filtered indexes) in `fintech_db`; the schema is locked. The pipeline loads into those tables rather than letting dbt build marts.
- **Bronze lineage** — `_ingested_at_utc` (one per run) + `_source_file` give auditability without altering business columns; idiomatic medallion.
- **Silver projection** — `df.select(contracted columns)` enforces the warehouse contract and guarantees Bronze metadata never reaches Silver/Gold.
- **Host dbt** — dbt-sqlserver + Windows auth can only run on the host; dbt is a quality gate + reporting view (not a dimension builder), matching the load-then-test pattern.
- **`run_all.ps1`** — single-command, reproducible e2e that bridges the container and host worlds (the only place both run in sequence). Run it **directly** in a terminal; don't pipe with `*>&1` (PowerShell 5.1 wraps docker stderr and can throw).
- **Atomic Gold load** — a partial Gold state (seen once during the QUOTED_IDENTIFIER failure) is unacceptable; the transaction makes reload all-or-nothing.
- **Dedicated dbt profile** — `fintech` was the old project's profile → a new isolated `fintech_db` profile prevents cross-project contamination.
- **Named Docker project** — both project folders are named `fintech-lakehouse`; an explicit `name:` prevents shared containers/volumes.
- **Named Docker volume** — explicit, durable metadata persistence vs prunable anonymous volumes.

---

## 10. Current Production Readiness (scored)

| Layer | Score (/10, local hybrid) | Notes |
|---|---|---|
| Docker | **8.5** | Isolated, named volume, deterministic+idempotent startup; full JDK (could be JRE), no restart policy |
| Bronze | **8.0** | Clean, idempotent, lineage added; no partitioning/history (fine at scale) |
| Silver | **8.5** | Typed/contract-conformant, null-preserving, idempotent; dedupe tie-breaker deferred |
| SQL load (Gold) | **8.5** | Set-based, atomic, fail-fast, FK-validated, SK-preserving; full truncate-reload (no incremental) |
| dbt | **8.5** | Isolated profile, 39/39 green, deprecation-clean; coverage light on measures/business rules |
| **Overall pipeline** | **8.5** | Single-command e2e, reconciled, integrity-validated, idempotent. Enterprise-grade ≈ 6/10 (orchestration maturity, incremental, observability, cloud-auth). |

---

## ⭐ START HERE (for a future session — read this first)

**What this is.** A frozen, working **hybrid local lakehouse**: Docker (Airflow+PySpark) builds
Bronze/Silver Parquet; the Windows host loads the Gold star into SQL Server `fintech_db` and runs
dbt; Power BI reads the Gold star. **Pipeline is complete through Phase 8.** Power BI is documented
but **not yet built**.

**Run it (one command, from a normal PowerShell terminal — not piped):**
```powershell
cd "f:\NTI FOLDER\NTI FINAL PROJECTssssssssssssssss\fintech-lakehouse"
.\run_all.ps1     # Docker bronze+silver -> host gold load -> dbt build; ends "DONE - warehouse rebuilt"
```
Then verify in SQL or Power BI against the §6 benchmarks (fact = 1,000,000; 11 FKs; dbt 39/39; gross ≈ 689,181,271 EGP).

**Key facts.**
- Server `ahmed\SQLEXPRESS`, DB `fintech_db`, **Windows auth only**. dbt profile **`fintech_db`** (NOT `fintech`/`fintech_wh`).
- Docker project **`fintech_lakehouse_new`**; UI http://localhost:8081 (admin/admin); named volume `fintech_lakehouse_new_pgdata`.
- Data is **frozen** (8 CSVs in `data/raw/`; `dim_decline_reason` deliberately has 8 rows). Numbers are deterministic.
- dbt = quality gate + `reporting.rpt_daily_transactions` view; **no seeds**; does not build dims.
- `transaction_sk` is **DB-generated IDENTITY** (excluded from the load INSERT).

**FROZEN — do not change** (§4): Gold schema/DDL, PKs, FKs (11), SKs, relationships, grain,
Power BI-facing tables, Windows auth, `fintech_wh` isolation.

**If asked to fix something:** allowed = ETL code, orchestration, dbt tests/views, docs (only with
a real runtime reason). Not allowed = anything in the FROZEN list.

**Next likely task:** build/verify the Power BI report (`docs/POWER_BI.md`), or implement a
"Future improvement" from §7 (e.g., atomic-already-done; consider audit table or incremental).

**Gotchas already discovered (don't rediscover):**
- Run `run_all.ps1` **directly**; piping with `*>&1` makes PS 5.1 throw on docker stderr.
- `sqlcmd` needs `QUOTED_IDENTIFIER ON` for the filtered-index fact (already in `load_gold.sql`).
- pyarrow `Dataset.to_batches(batch_size=…)` (not `max_chunksize`).
- dbt YAML: relationships must be **block style**; use `data_tests:`.
- BIT columns import to Power BI as **True/False** (filter `= TRUE()`).
- Nullable fact FKs are **correct** NULLs ("not applicable"), not missing data.
