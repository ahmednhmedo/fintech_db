# FinTech Lakehouse — Architecture Document

> **Scope:** This document describes the architecture that is **implemented today**.  
> It does not describe aspirational design, planned features, or hypothetical improvements.  
> Every statement is verifiable against the project source files.

---

## Table of Contents

1. [High-Level Architecture](#1-high-level-architecture)
2. [Technology Responsibilities](#2-technology-responsibilities)
3. [End-to-End Data Flow](#3-end-to-end-data-flow)
4. [Medallion Architecture](#4-medallion-architecture)
5. [Kimball Star Schema](#5-kimball-star-schema)
6. [Hybrid Design Rationale](#6-hybrid-design-rationale)
7. [Orchestration Design](#7-orchestration-design)
8. [Data Quality Framework](#8-data-quality-framework)
9. [Security Model](#9-security-model)
10. [Design Trade-offs](#10-design-trade-offs)
11. [Future Evolution](#11-future-evolution)

---

## 1. High-Level Architecture

### 1.1 Pipeline Summary

The project ingests eight CSV files totalling more than 1.3 million rows, transforms them through a three-layer medallion architecture, and delivers a validated Kimball-style star schema to Power BI. The pipeline is divided across two execution environments:

- **Docker (Linux containers):** Airflow orchestration, Pandas-based CSV extraction, PySpark-based silver conformance.
- **Windows host:** SQL Server warehouse loading, FK constraint management, dbt data-quality testing, and Power BI reporting.

The split is dictated by a hard technical constraint: Windows Authentication to SQL Server cannot be used from a Linux container without a Kerberos configuration that is impractical in a local development environment.

### 1.2 Architecture Overview

```mermaid
flowchart TD
    subgraph DOCKER["🐳 Docker  —  Linux Containers (fintech_lakehouse_new)"]
        direction LR
        PG[(PostgreSQL 15\nAirflow Metadata)]
        AF[Airflow Scheduler\n+ Webserver\n:8081]
        BRZ["Bronze Layer\ndata/lake/bronze/\nParquet  ·  Pandas"]
        SLV["Silver Layer\ndata/lake/silver/\nParquet  ·  PySpark"]
        PG --> AF
        AF -->|extract_to_bronze.py| BRZ
        BRZ -->|bronze_to_silver.py\nspark-submit| SLV
    end

    subgraph HOST["🪟 Windows Host  —  ahmed\\SQLEXPRESS"]
        direction TB
        PY["load_silver_to_sqlserver.py\nPython 3.11  ·  pyodbc\npyarrow Dataset API"]
        SS["SQL Server\nfintech_db\n[silver].*"]
        SG["SQL Server\nfintech_db\n[dbo].* Star Schema"]
        DR["SQL Server\nfintech_db\n[reporting].*"]
        DBT["dbt build\ndbt-core 1.8.7\ndbt-sqlserver 1.8.4"]
        PBI["Power BI Desktop\nImport Mode\nWindows Auth"]
        PY --> SS
        SS -->|load_gold.sql\nsqlcmd| SG
        SG -->|dbt sources| DBT
        DBT -->|rpt_daily_transactions| DR
        SG --> PBI
        DR --> PBI
    end

    SLV -->|"pyodbc · ODBC Driver 18\nTrusted_Connection (Windows Auth)"| PY
```

### 1.3 Execution Boundary

```
┌──────────────────────────────────┐   ┌───────────────────────────────────┐
│  DOCKER BOUNDARY                 │   │  WINDOWS HOST BOUNDARY            │
│                                  │   │                                   │
│  • Airflow scheduler + webserver │   │  • run_gold.ps1 (PowerShell)      │
│  • Postgres metadata DB          │   │  • Python 3.11 venv               │
│  • extract_to_bronze.py          │   │  • pyodbc + ODBC Driver 18        │
│  • bronze_to_silver.py (Spark)   │   │  • sqlcmd                         │
│  • bronze/ and silver/ Parquet   │   │  • SQL Server Express             │
│                                  │   │  • dbt-core + dbt-sqlserver       │
│  Trigger: Airflow DAG or         │   │  • Power BI Desktop               │
│           run_all.ps1 direct exec│   │                                   │
└──────────────────────────────────┘   └───────────────────────────────────┘
         │                                          ▲
         │  Shared volume mount: ./data             │
         └──────────────────────────────────────────┘
              Silver Parquet is the handoff artifact
```

---

## 2. Technology Responsibilities

### 2.1 Component Responsibility Matrix

| Component | Runtime | Version | Owns | Does NOT own |
|---|---|---|---|---|
| **Apache Airflow** | Docker | 2.9.3 | DAG scheduling; task sequencing; bronze extraction trigger; Spark submit trigger | SQL Server loading; dbt; FK management |
| **PySpark** | Docker (local mode) | 3.5.1 | Silver type-casting; deduplication; string normalization | CSV reading; SQL loading; any I/O outside the lake |
| **Pandas** | Docker | 2.2.2 | CSV ingestion in 500K-row chunks; lineage column injection | Transformations; schema enforcement |
| **PyArrow** | Docker + Host | 16.1.0 | Parquet writes (bronze); Parquet Dataset reads (silver → SQL load) | Type casting (Spark does this) |
| **SQL Server Express** | Windows host | — | Warehouse storage; FK enforcement; IDENTITY key generation for `transaction_sk`; index serving | ETL; orchestration |
| **pyodbc** | Windows host | 5.1.0 | ODBC connection to SQL Server; bulk insert via `fast_executemany` | Schema inference; transformation |
| **sqlcmd** | Windows host | — | Running `setup_warehouse.sql`, `drop_star_constraints.sql`, `load_gold.sql`, `add_star_constraints.sql` | Python execution |
| **dbt-core** | Windows host | 1.8.7 | Data-quality tests (38); reporting view materialization | Data loading; schema DDL |
| **dbt-sqlserver** | Windows host | 1.8.4 | SQL Server adapter for dbt; translates dbt commands to T-SQL | Any non-testing, non-view work |
| **dbt-fabric** | Pinned, unused | 1.8.7 | Future migration target; pinned to ensure version compatibility | Nothing in the current pipeline |
| **Power BI Desktop** | Windows host | — | Import-mode semantic model; DAX measures; dashboards | Data transformation; loading |
| **PostgreSQL** | Docker | 15 | Airflow metadata database only | Any project data; warehouse |
| **PowerShell** | Windows host | 5.1+ | Gold orchestration via `run_gold.ps1` and `run_all.ps1` | Docker management beyond `docker compose` commands |

### 2.2 Docker Image

The project uses a custom image built from `Dockerfile`:

```
Base image:  apache/airflow:2.9.3-python3.11
OS packages: default-jdk (required for PySpark JVM), procps
Python pkgs: pyspark==3.5.1, pyarrow==16.1.0, pandas==2.2.2
```

This image runs as all three Airflow services (init, webserver, scheduler) via the `x-airflow-common` anchor in `docker-compose.yml`. The Java installation is required because PySpark's driver process starts a JVM even in local mode.

### 2.3 Docker Compose Services

The Compose project is named `fintech_lakehouse_new` (explicitly set in `docker-compose.yml`) to prevent namespace collisions with any prior project that might share the same working directory name.

```mermaid
flowchart LR
    PG[(postgres:15\nMetadata DB\nVolume: pgdata)]
    INIT[airflow-init\nOne-time\ndb migrate + admin user]
    WEB[airflow-webserver\n:8081]
    SCH[airflow-scheduler\nRuns tasks]

    PG -->|healthcheck| INIT
    INIT -->|completed_successfully| WEB
    INIT -->|completed_successfully| SCH
```

The `airflow-init` service is idempotent: it runs `airflow db migrate` followed by a conditional user creation (`grep -qw admin || airflow users create ...`). Re-running `docker compose up` never fails on a duplicate admin user.

---

## 3. End-to-End Data Flow

### 3.1 Data Flow Overview

```mermaid
flowchart TD
    A["📁 data/raw/*.csv\n8 source files\n1.3M+ rows total"]

    B["🥉 BRONZE\ndata/lake/bronze/&lt;table&gt;/\nParquet  ·  all values as strings\n+ _ingested_at_utc, _source_file"]

    C["🥈 SILVER\ndata/lake/silver/&lt;table&gt;/\nParquet  ·  typed, trimmed, deduped\nlineage columns removed"]

    D["🗃️ SQL Server [silver].*\n8 ODS tables\nTransient — recreated each run"]

    E1["drop_star_constraints.sql\nDrops 11 FKs"]
    E2["load_gold.sql\nTRUNCATEs all gold tables\nINSERTs in FK-dependency order\nATOMIC: XACT_ABORT ON"]
    E3["add_star_constraints.sql\nRe-adds 11 FKs"]

    F["🥇 SQL Server [dbo].*\n7 dimension tables + fact_transactions\nKimball star schema"]

    G["✅ dbt build\n38 data-quality tests\nall-or-nothing: fails fast on violation"]

    H["📊 SQL Server [reporting].*\nrpt_daily_transactions\nView — 731 rows\ndbt-materialized"]

    I["📈 Power BI Desktop\nImport mode\n11 relationships\n21 recommended DAX measures"]

    A -->|"extract_to_bronze.py\nPandas · 500K-row chunks\ndtype=str"| B
    B -->|"bronze_to_silver.py\nPySpark spark-submit\n--driver-memory 4g"| C
    C -->|"load_silver_to_sqlserver.py\npyodbc · 50K-row batches\nWindows Auth"| D
    D --> E1
    E1 --> E2
    E2 --> E3
    E3 --> F
    F --> G
    G --> H
    F --> I
    H --> I
```

### 3.2 Step-by-Step Data Flow Specification

#### Step 1 — CSV → Bronze (`extract_to_bronze.py`)

| Attribute | Detail |
|---|---|
| **Trigger** | Airflow `PythonOperator` (`extract_csv_to_bronze` task) or direct `docker compose exec` via `run_all.ps1` |
| **Input** | `data/raw/<table>.csv` (8 files; 1.3M+ total rows) |
| **Reading strategy** | `pandas.read_csv(dtype=str, chunksize=500_000)` — all columns as strings, preserves raw values exactly |
| **Transformation** | None to business data; two lineage columns appended: `_ingested_at_utc` (single UTC timestamp for the whole run) and `_source_file` (original CSV filename) |
| **Idempotency** | `shutil.rmtree()` clears the partition directory before each write; no stale part files can accumulate |
| **Output** | `data/lake/bronze/<table>/part-XXXX.parquet` (one or more chunked Parquet parts per table) |
| **Why Pandas** | Bronze is pure I/O: read CSV, add two columns, write Parquet. No distributed computation is required. Pandas is simpler, has no JVM startup overhead, and runs natively in the Airflow Python worker. |

#### Step 2 — Bronze → Silver (`bronze_to_silver.py`)

| Attribute | Detail |
|---|---|
| **Trigger** | Airflow `BashOperator` (`spark_bronze_to_silver` task): `spark-submit --driver-memory 4g --conf spark.local.dir=/opt/data/spark-tmp /opt/airflow/spark/bronze_to_silver.py` |
| **Input** | `data/lake/bronze/<table>/` Parquet |
| **Type casting** | Per-table `SCHEMAS` dictionary: every column cast to `INT`, `LONG`, `STRING`, `DECIMAL(18,2)`, or `DATE` |
| **String normalization** | `F.trim()` applied to all string columns; empty string after trim converted to `NULL` |
| **Deduplication** | `dropDuplicates([natural_key])` per table — ensures idempotency on re-runs |
| **Projection** | Explicit `df.select(*cols)` from `SCHEMAS` keys — lineage columns (`_ingested_at_utc`, `_source_file`) are excluded from the projection and never reach silver |
| **Spark config** | Local mode; `shuffle.partitions=64` (avoids the 200-partition default for a 1M-row dataset); `--driver-memory 4g` |
| **Output** | `data/lake/silver/<table>/` Parquet (mode=overwrite) |
| **Why PySpark** | Silver applies type casting and deduplication across 1M+ rows of `fact_transactions`. PySpark's columnar execution engine (backed by Arrow) handles this efficiently and makes the code trivially scalable if the dataset grows. The same `bronze_to_silver.py` works unchanged on a YARN or Kubernetes cluster. |

#### Step 3 — Silver Parquet → SQL Server `[silver].*` (`load_silver_to_sqlserver.py`)

| Attribute | Detail |
|---|---|
| **Trigger** | `run_gold.ps1` step 2/4: `& $py "ingestion\load_silver_to_sqlserver.py"` |
| **Connection** | `pyodbc.connect()` — ODBC Driver 18 for SQL Server — `Trusted_Connection=yes` (Windows Auth) |
| **Schema management** | `IF SCHEMA_ID('silver') IS NULL EXEC('CREATE SCHEMA silver')` — idempotent; `IF OBJECT_ID(...) IS NOT NULL DROP TABLE` — clean recreate each run |
| **Reading** | `pyarrow.dataset.dataset(...).to_batches(batch_size=50_000)` — columnar streaming, memory-efficient |
| **Inserting** | `cursor.fast_executemany = True` — enables array binding; `cursor.executemany(insert, rows)` per 50K batch |
| **Commit strategy** | `conn.commit()` after each table (not per batch) — table-level atomicity |
| **Output** | `fintech_db.[silver].dim_date`, `[silver].dim_time`, ..., `[silver].fact_transactions` (8 tables) |

#### Step 4 — FK Drop → Gold Load → FK Restore (three `sqlcmd` calls)

All three scripts run sequentially from `run_gold.ps1` step 3/4 via `Invoke-Sql`. The `-b` flag causes `sqlcmd` to return a non-zero exit code on any SQL error severity ≥ 11, surfaced as a PowerShell terminating error.

**`drop_star_constraints.sql`** — drops all 11 foreign key constraints so `TRUNCATE TABLE` is allowed:

| FK Name | Table | References |
|---|---|---|
| `FK_fact_date` | `fact_transactions` | `dim_date` |
| `FK_fact_time` | `fact_transactions` | `dim_time` |
| `FK_fact_account` | `fact_transactions` | `dim_account` |
| `FK_fact_peer_account` | `fact_transactions` | `dim_account` |
| `FK_fact_transaction_type` | `fact_transactions` | `dim_transaction_type` |
| `FK_fact_decline_reason` | `fact_transactions` | `dim_decline_reason` |
| `FK_fact_merchant` | `fact_transactions` | `dim_merchant` |
| `FK_dim_account_location` | `dim_account` | `dim_location` |
| `FK_dim_account_date` | `dim_account` | `dim_date` |
| `FK_dim_merchant_location` | `dim_merchant` | `dim_location` |
| `FK_dim_merchant_date` | `dim_merchant` | `dim_date` |

**`load_gold.sql`** — atomic TRUNCATE-and-INSERT:
- `SET XACT_ABORT ON` — any error rolls back the entire transaction automatically
- `BEGIN TRANSACTION ... COMMIT` — all-or-nothing; a failure never leaves partial gold state
- TRUNCATE order: `fact_transactions` first, then all dimensions (FK-unconstrained after step above)
- INSERT order: `dim_date` → `dim_time` → `dim_location` → `dim_transaction_type` → `dim_decline_reason` → `dim_account` → `dim_merchant` → `fact_transactions` (FK parents before children)
- For tables with IDENTITY columns: `SET IDENTITY_INSERT dbo.<table> ON` before INSERT, `OFF` after — surrogate keys come from silver, not auto-generated
- Exception: `fact_transactions.transaction_sk` is NOT in the INSERT column list; SQL Server IDENTITY assigns it automatically
- `dim_date` and `dim_time` have no IDENTITY column; their natural keys (`date_key`, `time_key`) are inserted directly

**`add_star_constraints.sql`** — restores all 11 foreign key constraints, re-enabling referential integrity.

#### Step 5 — dbt Build

| Attribute | Detail |
|---|---|
| **Trigger** | `run_gold.ps1` step 4/4: `& $dbt build` from `dbt/fintech/` |
| **Profile** | `fintech_db` → target `dev` → SQL Server via `windows_login: true` |
| **Test execution** | 38 tests run against `[dbo].*` sources defined in `models/_sources.yml` |
| **Model execution** | `models/reporting/rpt_daily_transactions.sql` materialized as a view in `[reporting]` schema |
| **Schema naming** | `macros/generate_schema_name.sql` passes `custom_schema_name` verbatim — produces `[reporting]`, not `[dbo_reporting]` |
| **Failure behaviour** | Any test failure causes `dbt build` to exit non-zero; `run_gold.ps1` catches this and throws a terminating error |

#### Step 6 — Power BI

Power BI Desktop connects to `ahmed\SQLEXPRESS` / `fintech_db` in **Import mode** via Windows Authentication. The analyst imports 8 gold tables from `[dbo]` and optionally the `[reporting].rpt_daily_transactions` view, configures 11 relationships, and creates 21 DAX measures as documented in [`POWER_BI.md`](POWER_BI.md).

---

## 4. Medallion Architecture

### 4.1 Layer Responsibilities

```mermaid
flowchart LR
    subgraph BRONZE["🥉 Bronze — Raw Landing"]
        B1["Immutable record\nof what was received"]
        B2["All values: string\ndtype=str"]
        B3["Lineage columns added\n_ingested_at_utc\n_source_file"]
        B4["No business logic\nNo validation"]
    end
    subgraph SILVER["🥈 Silver — Conformed ODS"]
        S1["Typed per warehouse DDL\nINT · LONG · DATE · DECIMAL"]
        S2["Strings trimmed\nEmpty → NULL"]
        S3["Deduplicated\non natural key"]
        S4["Lineage columns\nremoved"]
    end
    subgraph GOLD["🥇 Gold — Dimensional Warehouse"]
        G1["Kimball star schema\n7 dims + 1 fact"]
        G2["FK-enforced integrity\n11 constraints"]
        G3["Surrogate keys\nassigned or generated"]
        G4["38 dbt tests\nvalidate completeness"]
    end
    BRONZE --> SILVER --> GOLD
```

### 4.2 Bronze Design Principles

Bronze is a **faithful, raw landing zone**. Its only obligation is to preserve exactly what the source provided, plus sufficient metadata to audit when and from where the data came.

- `dtype=str` prevents pandas from interpreting `NULL`-like strings, numeric edge cases, or date ambiguities.
- The `_ingested_at_utc` timestamp is set once per batch execution — all rows within the same pipeline run share the same lineage timestamp, making batch identification trivial.
- Directory-clearing idempotency (`shutil.rmtree` before write) means re-running the extraction never produces mixed part files from different pipeline runs.

Bronze is deliberately **not validated**. Schema enforcement happens in silver, where bad types fail loudly with a Spark cast exception rather than silently storing wrong values.

### 4.3 Silver Design Principles

Silver is the **single source of truth for typed, clean, deduplicated data**. It mirrors the gold warehouse schema exactly, so the SQL `INSERT … SELECT FROM silver.*` in `load_gold.sql` requires no implicit type casting.

The `SCHEMAS` dictionary in `bronze_to_silver.py` defines the authoritative type mapping:

| Category | Types used |
|---|---|
| Integer keys and flags | `INT` (`int`) |
| Large measures | `LONG` (`long`) for `amount_minor`, `fx_amount_minor`, `exchange_rate_e6` |
| Decimal measures | `DECIMAL(18,2)` for `amount_egp`, `abs_amount_egp` |
| Dates | `DATE` for `full_date`, `signup_date_key` source column |
| All text | `STRING` (`string`) with `trim()` and empty-to-null normalization |

Silver does not add columns and does not join tables. The source data is already in star shape; silver's job is conformance, not integration.

### 4.4 Gold Design Principles

Gold is the **analyst-facing dimensional warehouse**. It implements Kimball-style dimensional modelling with surrogate keys, well-defined grain, and enforced referential integrity. It is loaded atomically (single transaction) and is always either fully consistent or untouched (XACT_ABORT ON prevents partial states).

---

## 5. Kimball Star Schema

### 5.1 Schema Overview

```mermaid
erDiagram
    dim_date {
        int date_key PK "YYYYMMDD natural key — no IDENTITY"
        date full_date
        tinyint day
        tinyint month
        tinyint quarter
        smallint year
        bit is_weekend
    }
    dim_time {
        int time_key PK "HHMM natural key — no IDENTITY"
        tinyint hour_of_day
        varchar time_bucket "Morning/Afternoon/Evening/Night"
        bit is_daytime
        varchar hour_label
    }
    dim_location {
        int location_key PK "IDENTITY — outrigger shared by accounts and merchants"
        nvarchar city
        nvarchar governorate
        nvarchar country
    }
    dim_account {
        int account_key PK "IDENTITY; IDENTITY_INSERT ON during load"
        varchar account_id "Business natural key — UNIQUE constraint"
        int location_key FK
        char currency "ISO 4217"
        varchar age_band
        varchar acquisition_channel
        int signup_date_key FK
        varchar customer_tier "Standard / Premium / Business"
        varchar account_status "Active / Dormant / Suspended / Closed"
    }
    dim_merchant {
        int merchant_key PK "IDENTITY; IDENTITY_INSERT ON during load"
        int location_key FK
        varchar merchant_id "Business natural key — UNIQUE constraint"
        nvarchar merchant_name
        varchar merchant_category
        varchar merchant_size "Enterprise / Mid-Market / SME"
        int opened_date_key FK
    }
    dim_transaction_type {
        int transaction_type_key PK "IDENTITY; IDENTITY_INSERT ON during load"
        varchar transaction_type_name
        varchar transaction_group "Payment / Transfer / Withdrawal"
    }
    dim_decline_reason {
        int decline_reason_key PK "IDENTITY; IDENTITY_INSERT ON during load"
        varchar decline_reason_name
    }
    fact_transactions {
        bigint transaction_sk PK "IDENTITY — auto-generated; NOT in INSERT list"
        int date_key FK
        int time_key FK
        int account_key FK "NOT NULL — primary actor"
        int peer_account_key FK "NULL for non-P2P — role-playing INACTIVE link"
        int transaction_type_key FK "NOT NULL"
        int decline_reason_key FK "NULL for approved transactions"
        int merchant_key FK "NULL for non-merchant transactions"
        varchar transaction_id "Business natural key — UNIQUE index"
        varchar mc_transaction_id "Mastercard ref — nullable"
        varchar ach_transfer_id "ACH ref — nullable"
        bigint amount_minor "In piastres — avoids float"
        decimal amount_egp "Signed EGP decimal"
        decimal abs_amount_egp "Always positive — used for gross volume"
        bigint fx_amount_minor "NULL for EGP transactions"
        bigint exchange_rate_e6 "Rate × 1000000 — avoids float"
        bit is_outbound
        bit is_declined
        bit is_fx
    }

    fact_transactions }o--|| dim_date : "date_key (active)"
    fact_transactions }o--|| dim_time : "time_key"
    fact_transactions }o--|| dim_account : "account_key (active)"
    fact_transactions }o--o| dim_account : "peer_account_key (inactive)"
    fact_transactions }o--|| dim_transaction_type : "transaction_type_key"
    fact_transactions }o--o| dim_decline_reason : "decline_reason_key (nullable)"
    fact_transactions }o--o| dim_merchant : "merchant_key (nullable)"
    dim_account }o--|| dim_location : "location_key"
    dim_account }o--|| dim_date : "signup_date_key (inactive)"
    dim_merchant }o--|| dim_location : "location_key"
    dim_merchant }o--|| dim_date : "opened_date_key (inactive)"
```

### 5.2 Dimension Design

#### `dim_date` and `dim_time` — Natural Key Dimensions

`dim_date` and `dim_time` are the only two dimensions in the schema that do **not** use IDENTITY. Their primary keys are stable, human-meaningful integers:

- `date_key` = YYYYMMDD (e.g., `20240115`) — deterministic, globally unique, readable in SQL
- `time_key` = HHMM (e.g., `1430`) — 1,440 distinct values, one per minute of the day

Because these keys are inherently non-colliding across all time and already present in the source data, surrogate key generation adds no value. `load_gold.sql` inserts them without `SET IDENTITY_INSERT ON` because the columns carry no IDENTITY definition.

#### Dimensions with Surrogate IDENTITY Keys

`dim_location`, `dim_account`, `dim_merchant`, `dim_transaction_type`, and `dim_decline_reason` all have `INT NOT NULL IDENTITY(1,1)` primary keys. During the gold load, `SET IDENTITY_INSERT dbo.<table> ON` is used to pass the surrogate key values from silver directly into the table. SQL Server does not auto-generate new IDENTITY values when `IDENTITY_INSERT` is ON.

This means surrogate keys for these dimensions originated in the source data generation stage and are preserved intact through bronze → silver → gold. They are stable across pipeline re-runs.

#### `fact_transactions.transaction_sk` — Auto-Generated IDENTITY

`transaction_sk` is the only key that SQL Server **truly auto-generates** during the gold load. It is a `BIGINT IDENTITY(1,1)` and is **excluded from the INSERT column list** in `load_gold.sql`. A fresh IDENTITY sequence is assigned on every full reload (TRUNCATE resets the seed).

The natural key `transaction_id` (VARCHAR 50, UNIQUE non-clustered index) is the business identifier used for deduplication and cross-system traceability. `transaction_sk` is the surrogate for join performance.

#### Business Natural Keys Retained in Fact

Three natural keys are retained in `fact_transactions` for auditability:
- `transaction_id` — primary event identifier
- `mc_transaction_id` — Mastercard reference (nullable)
- `ach_transfer_id` — ACH reference (nullable)

### 5.3 Role-Playing Dimension

`dim_account` is linked to `fact_transactions` through two foreign keys:

| FK | Column | Cardinality | Active in Power BI |
|---|---|---|---|
| `FK_fact_account` | `account_key` | Many-to-One | **Yes** — primary actor |
| `FK_fact_peer_account` | `peer_account_key` | Many-to-One | **No** — P2P counterparty; activate with `USERELATIONSHIP()` |

`peer_account_key` is NULL for all non-P2P transactions. Power BI allows only one active relationship between two tables, so the peer link is kept inactive and accessed via DAX `USERELATIONSHIP()` in the P2P Volume measure.

### 5.4 Outrigger Dimensions

Two dimensions serve as outriggers — they are referenced by other dimensions, not only by the fact table:

**`dim_location`** (19 rows):
- `dim_account.location_key → dim_location.location_key` (customer city)
- `dim_merchant.location_key → dim_location.location_key` (merchant city)

**`dim_date`** (731 rows):
- `fact_transactions.date_key → dim_date.date_key` (transaction date, **active**)
- `dim_account.signup_date_key → dim_date.date_key` (account opening, **inactive** in Power BI)
- `dim_merchant.opened_date_key → dim_date.date_key` (merchant opening, **inactive** in Power BI)

The two signup/opened date links are kept inactive in Power BI so that a date slicer filters transactions, not account or merchant populations.

### 5.5 Indexes on `fact_transactions`

Five indexes are defined in `01_create_star.sql`:

| Index | Type | Columns | Purpose |
|---|---|---|---|
| `PK_fact_transactions` | Clustered (implicit) | `transaction_sk` | Row identity |
| `UX_fact_transactions_natural_key` | Unique non-clustered | `transaction_id` | Idempotency; business key lookup |
| `IX_fact_transactions_date` | Non-clustered | `date_key` INCLUDE (`amount_egp`, `is_declined`, `is_fx`) | Date-range queries (most common pattern) |
| `IX_fact_transactions_account` | Non-clustered | `account_key` INCLUDE (`date_key`, `amount_egp`, `is_outbound`) | Account activity lookups |
| `IX_fact_transactions_declined` | Filtered non-clustered | `is_declined, decline_reason_key` WHERE `is_declined=1` | Decline analysis (4.25% of rows) |
| `IX_fact_transactions_fx` | Filtered non-clustered | `is_fx` WHERE `is_fx=1` | FX analysis (4.38% of rows) |

The two filtered indexes are particularly important for decline and FX reporting: they cover only the minority subset, keeping index size small while dramatically accelerating those specific query patterns.

---

## 6. Hybrid Design Rationale

### 6.1 The Core Constraint

The fundamental reason for the hybrid architecture is a single, hard technical constraint:

> **Windows Authentication (NTLM) to SQL Server cannot be performed from a Linux container without Kerberos ticket forwarding, which requires a domain controller, keytab configuration, and significant infrastructure that is incompatible with a local development environment.**

The SQL Server instance is `ahmed\SQLEXPRESS` — a Windows named instance that accepts only Windows Authentication in this configuration. The `.env.example` file explicitly documents this:

```
# Leave WH_USER / WH_PASSWORD UNSET -> Trusted_Connection (Windows auth).
# Do NOT set them: this project is Windows-auth only.
```

### 6.2 Why Docker Only Hosts Bronze and Silver

```mermaid
flowchart LR
    subgraph WHY_DOCKER["Why Docker?"]
        D1["✅ No Windows Auth needed\nParquet is filesystem I/O"]
        D2["✅ Airflow is a Linux-native workload\nImage exists: apache/airflow:2.9.3"]
        D3["✅ PySpark runs on JVM\nworks identically on any OS"]
        D4["✅ Reproducibility\nSame image everywhere"]
    end
    subgraph WHY_NOT_GOLD["Why not Gold in Docker?"]
        N1["❌ SQL Server Windows Auth\nrequires NTLM / Kerberos"]
        N2["❌ Linux container cannot\nimpersonate Windows user"]
        N3["❌ No domain controller\nin local dev setup"]
    end
```

Bronze and silver workloads have no SQL Server dependency at all. They read and write files to a shared Docker volume (`./data` mounted as `/opt/data`). This makes them fully portable across operating systems.

### 6.3 Why SQL Server Stays on the Windows Host

SQL Server Express is installed directly on the Windows host as a named instance (`ahmed\SQLEXPRESS`). Moving it into Docker would require:

1. Switching to SQL Server for Linux (a different product variant)
2. Abandoning Windows Authentication entirely (SQL Server for Linux uses SQL logins by default)
3. Reconfiguring the entire security model (credentials in environment variables, secrets management)

None of these trade-offs are appropriate for a local data warehouse that intentionally uses zero-credential authentication.

### 6.4 Why dbt Runs on the Host

dbt-sqlserver connects to `ahmed\SQLEXPRESS` via ODBC Driver 18 for SQL Server using `windows_login: true` in the profile. This connection requires the calling process to be running **as a Windows user** — the ODBC driver passes the current Windows identity to SQL Server. A Linux container process has no Windows identity. Therefore, `dbt build` must run on the Windows host.

The `profiles.yml.example` documents this:
```yaml
windows_login: true   # Windows (trusted) auth — no SQL login
```

### 6.5 Why Windows Authentication

| Property | Windows Auth | SQL Auth |
|---|---|---|
| **Credentials at rest** | None — OS token | Password in `.env` or environment variable |
| **Credentials in transit** | NTLM/Kerberos token | Password in connection string |
| **Risk on dev machine** | None | Password exposed to shell history, logs |
| **Management overhead** | None — always available | Create login, manage rotation |
| **Local dev suitability** | Ideal | Overkill for single-user local setup |

For a single-developer local project, Windows Authentication is both simpler and more secure than managing SQL logins.

---

## 7. Orchestration Design

### 7.1 Execution Modes

The project supports three execution modes:

```mermaid
flowchart TD
    A{Execution Mode}
    A --> B["run_all.ps1\nFull rebuild\nDocker + Gold + dbt"]
    A --> C["run_gold.ps1\nGold only\n(silver Parquet exists)"]
    A --> D["Airflow UI\n:8081 → trigger DAG\nthen run_gold.ps1 manually"]
```

### 7.2 `run_all.ps1` — Full Pipeline

```mermaid
flowchart TD
    R1["docker compose up -d --build\nStarts all 4 services"] --> R2
    R2["Start-Sleep -Seconds 15\nAllows Postgres to become healthy"] --> R3
    R3["docker compose exec -T airflow-scheduler bash -lc\n'python ingestion/extract_to_bronze.py &&\nspark-submit ... spark/bronze_to_silver.py'"] --> R4
    R4["run_gold.ps1\n(called as sub-script)"]
```

> **Important:** `run_all.ps1` does **not** trigger the Airflow DAG. It runs `extract_to_bronze.py` and `bronze_to_silver.py` directly inside the running `airflow-scheduler` container via `docker compose exec`. No DAG run is recorded in the Airflow metadata database when using this path. The Airflow UI at `:8081` will show no new DAG execution history.

This design was chosen for simplicity: `run_all.ps1` is a one-command rebuild that does not require the Airflow DAG scheduler to have processed a trigger or waited for a schedule interval.

### 7.3 `run_gold.ps1` — Gold Runner

```mermaid
flowchart TD
    ENV["Load .env → set WH_SERVER, WH_DATABASE\nResolve Python 3.11 interpreter via 'py -3.11'"]
    S1["Step 1/4\nsetup_warehouse.sql\nEnsures [silver] schema exists"]
    CHK{"dbo.fact_transactions\nexists?"}
    DDL["01_create_star.sql\nCreate star tables, FKs, indexes"]
    S2["Step 2/4\nload_silver_to_sqlserver.py\nSilver Parquet → fintech_db.[silver].*\n8 tables · 50K-row batches"]
    S3A["Step 3/4a\ndrop_star_constraints.sql\nDrop 11 FKs"]
    S3B["load_gold.sql\nTRUNCATE + INSERT (atomic)"]
    S3C["add_star_constraints.sql\nRe-add 11 FKs"]
    S4["Step 4/4\ndbt build\n38 tests + reporting view"]
    DONE["✅ Complete\ndbo.* star validated\nreporting.rpt_daily_transactions built"]

    ENV --> S1 --> CHK
    CHK -->|No| DDL --> S2
    CHK -->|Yes| S2
    S2 --> S3A --> S3B --> S3C --> S4 --> DONE
```

`run_gold.ps1` uses `$ErrorActionPreference = "Stop"` and a custom `Invoke-Sql` function that checks `$LASTEXITCODE` after every `sqlcmd` call. Any failure at any step terminates the script immediately, preventing partial gold states.

### 7.4 Airflow DAG — `fintech_lakehouse`

When the Airflow UI is used instead of `run_all.ps1`, the DAG provides full task-level visibility and retry management:

```mermaid
flowchart LR
    T1["extract_csv_to_bronze\nPythonOperator\nCalls extract_to_bronze.extract_csvs()"]
    T2["spark_bronze_to_silver\nBashOperator\nspark-submit --driver-memory 4g\n--conf spark.local.dir=/opt/data/spark-tmp"]
    T3["gold_handoff\nBashOperator\nEcho only — signals silver is ready"]

    T1 -->|"retries=2\nretry_delay=2min"| T2 -->|"retries=2\nretry_delay=2min"| T3
```

| Property | Value |
|---|---|
| `dag_id` | `fintech_lakehouse` |
| `schedule` | `@daily` |
| `start_date` | `2024-01-01` |
| `catchup` | `False` |
| `owner` | `data-eng` |

The `gold_handoff` task emits a message instructing the operator to run `run_gold.ps1` on the Windows host. It is a marker task, not an executor — Airflow cannot invoke host PowerShell directly from a Linux container.

### 7.5 Why Airflow Is Not the End-to-End Orchestrator

Airflow cannot complete the full pipeline alone because:

1. The gold loading step requires Windows Authentication to SQL Server — unavailable in the Linux container where Airflow runs.
2. `dbt build` requires a host Python environment with `dbt-sqlserver` and ODBC Driver 18 — not installed in the Airflow image (to avoid bloating it with host-specific dependencies).
3. `sqlcmd` is not installed in the Airflow image.

The boundary is clean: **Airflow owns the lake (Docker-native workloads), PowerShell owns the warehouse (Windows-native workloads).**

---

## 8. Data Quality Framework

### 8.1 Quality Layers Overview

Quality is enforced at every layer, with increasing strictness from bronze to gold:

```mermaid
flowchart LR
    L1["📁 Bronze\nNo validation\nPreserves raw state\nFailed casts caught in Silver"]
    L2["🥈 Silver\nPySpark cast exceptions\nStop pipeline on type mismatch\nDeduplication on natural key"]
    L3["🔑 SQL Server DDL\nNOT NULL constraints\nUNIQUE indexes\n11 FK constraints"]
    L4["✅ dbt Tests\n38 automated tests\nPost-load validation gate"]
    L5["📊 Power BI\nBenchmark verification\nDetects stale/disconnected data"]

    L1 --> L2 --> L3 --> L4 --> L5
```

### 8.2 Bronze Quality

Bronze applies **no validation intentionally**. By reading all columns as `dtype=str`, it guarantees no data loss from implicit type coercion. A source file with `"N/A"` in an integer column will be preserved exactly, rather than silently becoming `NaN` or raising an unhandled exception during ingestion.

The tradeoff: bronze can contain garbage data. This is acceptable because silver is the enforcement boundary.

### 8.3 Silver Quality

PySpark enforces the schema at cast time. If a value in bronze cannot be cast to the declared type (e.g., a non-numeric string in an `INT` column), Spark will return `NULL` for that value by default. The deduplication step then ensures each natural key appears exactly once, making silver idempotent with respect to re-runs.

### 8.4 SQL Server DDL Constraints

The star schema DDL in `01_create_star.sql` enforces:
- `NOT NULL` on all required columns (fact FKs, dimension PKs, business attributes)
- `UNIQUE` index on `fact_transactions.transaction_id` (natural key)
- `UNIQUE` implicit on all dimension PKs via PRIMARY KEY constraints
- 11 foreign key constraints restored after each gold load by `add_star_constraints.sql`

### 8.5 dbt Data Tests

The 38 dbt tests in `models/_sources.yml` form the **post-load data-quality gate**. They run after the gold load completes and before the pipeline is considered successful.

| Category | Count | Tables Covered |
|---|---|---|
| `unique` | 10 | All 7 dimensions (PKs + natural keys where defined) + `fact_transactions.transaction_id` |
| `not_null` | 14 | All dimension PKs, natural keys, and 4 non-nullable fact FKs |
| `accepted_values` | 7 | `time_bucket`, `customer_tier`, `account_status`, `merchant_size`, `is_outbound`, `is_declined`, `is_fx` |
| `relationships` | 7 | All 7 FK paths from `fact_transactions` to dimensions |

The `relationships` tests directly verify referential integrity at the data level — they confirm that every FK value in the fact table resolves to a valid row in the referenced dimension. This catches any silver → gold insert ordering issues that might slip past FK constraints.

### 8.6 Reporting Validation

`docs/POWER_BI.md` Section 8 provides a table of deterministic benchmark values that the analyst can verify after importing data into Power BI:

| Measure | Expected Value |
|---|---|
| Transaction Count | 1,000,000 |
| Gross Volume (EGP) | ≈ 689,181,271 |
| Average Ticket (EGP) | 689.18 |
| Declined Transactions | 42,478 |
| Decline Rate % | 4.25% |
| FX Transactions | 43,783 |
| Active Accounts | 39,795 |
| P2P Transactions | 154,667 |

Because the source data is frozen (CSV files committed to the repository), these values are deterministic. A mismatch indicates either a stale PBIX file, a failed pipeline run, or a wrong database connection.

---

## 9. Security Model

### 9.1 Authentication Architecture

```mermaid
flowchart LR
    subgraph HOST["Windows Host"]
        WIN["Windows User\n(running the session)"]
        PY["Python\nload_silver_to_sqlserver.py"]
        DBT["dbt build"]
        PSH["PowerShell\nrun_gold.ps1"]
        SQL["SQL Server Express\nauthenticates via NTLM"]
        WIN -->|"Trusted_Connection=yes\nODBC Driver 18"| PY --> SQL
        WIN -->|"windows_login: true\ndbt-sqlserver"| DBT --> SQL
        WIN --> PSH -->|"-E flag (Windows auth)\nsqlcmd"| SQL
    end
    subgraph DOCKER["Docker"]
        AF["Airflow\n(Linux processes)"]
        PG["PostgreSQL\nairflow/airflow (internal only)"]
        AF -->|"psycopg2\nSQL Alchemy"| PG
    end
```

### 9.2 No SQL Logins

The project contains no SQL Server logins, no passwords, and no credential storage of any kind for the warehouse layer:

- `load_silver_to_sqlserver.py` uses `Trusted_Connection=yes` (Windows Auth)
- `run_gold.ps1` uses `sqlcmd -E` flag (Windows Auth)
- `profiles.yml.example` uses `windows_login: true`
- `.env.example` explicitly says: `# Do NOT set them: this project is Windows-auth only`
- `WH_USER` and `WH_PASSWORD` environment variables exist as a code-path option in `load_silver_to_sqlserver.py` but are never set or used

### 9.3 Docker Isolation

The Compose project is named `fintech_lakehouse_new`, which scopes all containers, networks, and volumes to this name. This prevents resource collisions with any other Compose project that might share the same directory name.

The Postgres database used for Airflow metadata is accessible only within the Docker network (`postgresql+psycopg2://airflow:airflow@postgres/airflow`). It is not published to the host network.

### 9.4 dbt Profile Isolation

The dbt profile `fintech_db` is distinct from any other dbt profile the user might have (e.g., a legacy `fintech` profile from a prior project). The profile name is documented in `dbt_project.yml`:

```yaml
profile: fintech_db
```

This ensures `dbt build` in this project cannot accidentally connect to the wrong target.

### 9.5 Secret-Free Repository

The repository is designed to contain no secrets:

- `.env` is gitignored; only `.env.example` is committed
- `~/.dbt/profiles.yml` is the user's home directory; only `profiles.yml.example` is committed
- No passwords appear in any committed file
- The Airflow admin credentials (`admin`/`admin`) are for local development only and are not used to protect any sensitive data

---

## 10. Design Trade-offs

### 10.1 Full Refresh vs Incremental Loading

| Aspect | Current (Full Refresh) | Alternative (Incremental MERGE) |
|---|---|---|
| **Complexity** | Low — TRUNCATE + INSERT | High — need watermark, CDC, or change detection |
| **Runtime** | ~5–10 min for 1M rows | Seconds for small deltas |
| **Idempotency** | Perfect — re-run = same result | Requires dedup logic for late-arriving data |
| **Risk** | Pipeline failure leaves gold empty until re-run | Partial MERGE can leave inconsistent state |
| **Data volume** | Suitable for ≤ 10M rows | Required above that threshold |

**Decision:** Full refresh was chosen because the dataset (1M rows) processes in acceptable time, the TRUNCATE-and-reload pattern is simple to reason about and verify, and the `XACT_ABORT ON` transaction ensures the warehouse is never in a partially-loaded state. Incremental loading is a Phase 2 improvement.

### 10.2 Local PySpark vs Alternative Compute

| Aspect | Local PySpark | Pandas-only | Cloud Spark (EMR, Databricks) |
|---|---|---|---|
| **Performance at 1M rows** | ✅ Adequate | ✅ Adequate | Overkill (cluster startup overhead) |
| **Scalability** | ✅ Same code scales to 100M+ | ❌ Memory-bound | ✅ Scales further |
| **Portability** | ✅ Same Spark API everywhere | N/A | ✅ |
| **Local setup complexity** | Moderate (Java required) | Low | High (cloud account, cost) |
| **Portfolio signal** | ✅ Demonstrates Spark skills | Limited | ✅ Higher signal |

**Decision:** PySpark in local mode was chosen for its combination of scalability headroom and portfolio value. The silver transformation code is identical to what would run on a Spark cluster — only the `SparkSession` configuration changes.

### 10.3 Hybrid Execution vs Pure Docker

| Aspect | Hybrid (current) | Pure Docker |
|---|---|---|
| **Windows Auth** | ✅ Available on host | ❌ Not available in Linux container |
| **SQL Server** | ✅ Named instance, local | Would require SQL Server for Linux + SQL Auth |
| **Credential management** | ✅ Zero credentials | Requires SQL login + secrets management |
| **Portability** | ❌ Windows-only host | ✅ Cross-platform |
| **Complexity** | Moderate (two execution contexts) | Higher (SQL Server in Docker + credential config) |

**Decision:** Hybrid is the pragmatic choice for a local Windows development environment. The trade-off is reduced portability: the project currently requires Windows as the host OS for the gold layer.

### 10.4 No dbt Seeds

dbt Seeds are designed for small, static reference data that is version-controlled inside the dbt project itself. This project intentionally does not use seeds for the dimensional CSVs because:

1. The dimensional data (dim_account.csv, dim_merchant.csv, etc.) contains 40,000+ rows of account data — far beyond what seeds are designed to handle efficiently.
2. The dimensional data participates in the full medallion pipeline (bronze → silver → SQL Server silver → gold). Seeds would create an alternative, parallel data path that bypasses bronze/silver entirely.
3. Seeds would materialize directly into the `dbo` schema as dbt tables, conflicting with the DDL-defined gold tables that have IDENTITY, FKs, and filtered indexes already.

Reference data (`dim_transaction_type`, `dim_decline_reason`) is also sourced through the pipeline rather than seeds, maintaining the single data path principle.

### 10.5 No Fabric Migration (Yet)

`dbt-fabric==1.8.7` is pinned in `requirements.txt` for forward compatibility. The comment in `requirements.txt` explains the pinning constraint:

```
dbt-fabric==1.8.7   # pinned: dbt-sqlserver 1.8.4 needs <1.8.8
```

The migration path to Microsoft Fabric is documented and requires two changes only:
1. Swap `type: sqlserver` to `type: fabric` in `~/.dbt/profiles.yml`
2. Replace `load_silver_to_sqlserver.py` with a Fabric `COPY INTO` loader

The Airflow DAG, PySpark jobs, SQL star schema DDL, and dbt models (`_sources.yml`, `rpt_daily_transactions.sql`) are all unchanged by a Fabric migration.

### 10.6 No SQL Authentication

SQL Authentication was deliberately excluded rather than omitted. The `.env.example` contains explicit guidance against using it:

```
# Leave WH_USER / WH_PASSWORD UNSET -> Trusted_Connection (Windows auth).
# Do NOT set them: this project is Windows-auth only.
```

For a local development warehouse on a single-user Windows machine, SQL Authentication provides no security benefit over Windows Authentication while introducing the risk of credential storage in environment files.

### 10.7 Reporting View vs Additional Fact Tables

`rpt_daily_transactions` is a thin aggregation view, not a separate aggregate fact table. It is materialized as a `VIEW` (not a `TABLE`) in the `[reporting]` schema, meaning:

- It computes on-demand from `fact_transactions` and `dim_date`
- It consumes no additional storage
- At 1M rows in Import mode, the aggregation happens at Power BI import time in under a second
- The view is provided as a convenience demonstration of the dbt reporting layer, not as a performance optimization

The `docs/POWER_BI.md` explicitly marks it as optional for Power BI consumers.

---

## 11. Future Evolution

These are realistic next steps that do not change the current architecture. Each item is independently implementable.

### 11.1 Incremental Gold Loading

Replace the TRUNCATE-and-reload pattern in `load_gold.sql` with SQL Server `MERGE` statements:

```sql
-- Conceptual pattern (not yet implemented)
MERGE dbo.dim_account AS target
USING silver.dim_account AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

This would reduce nightly processing time from ~5–10 minutes to seconds for small delta loads.

### 11.2 SCD Type 2 for Slowly Changing Attributes

Three columns are natural SCD Type 2 candidates based on the domain model:
- `dim_account.age_band`
- `dim_account.customer_tier`
- `dim_merchant.merchant_size`

Implementing Type 2 requires adding `effective_date`, `expiry_date`, and `is_current` columns to these dimensions, and updating the silver-to-gold merge logic to version changes rather than overwrite them.

### 11.3 Microsoft Fabric Migration

The migration path is already prepared:

```
Step 1: Update ~/.dbt/profiles.yml
        type: sqlserver → type: fabric
        Add: account, database, schema, warehouse

Step 2: Replace ingestion/load_silver_to_sqlserver.py
        with a Fabric COPY INTO loader

Unchanged: dags/, spark/, sql/01_create_star.sql,
           dbt/fintech/models/, dbt/fintech/macros/
```

### 11.4 Streaming Silver Layer

The silver conformance logic in `bronze_to_silver.py` can be adapted to Spark Structured Streaming by replacing `spark.read.parquet()` with `spark.readStream.parquet()` and pointing it at a Kafka or Azure Event Hubs source. The type-casting logic and `SCHEMAS` dictionary are unchanged.

### 11.5 dbt Semantic Layer

The 21 DAX measures defined in `docs/POWER_BI.md` can be formalized as dbt Metrics (via the dbt Semantic Layer / MetricFlow). This would make the metric definitions vendor-neutral and consumable by any tool that supports the Semantic Layer API.

### 11.6 Cross-Platform Gold (Alternative Path)

If Windows-only execution becomes a limitation, an alternative architecture could use SQL Server for Linux in a container with SQL Authentication and a secrets manager (e.g., Azure Key Vault, HashiCorp Vault). This would make the entire pipeline run in Docker on any OS. The trade-off is the introduction of SQL logins and a secrets management dependency.

---

*Architecture version: 1.0 | Project version: 1.0 | Last verified: 2026-06-29*  
*All statements verified against: `Dockerfile`, `docker-compose.yml`, `requirements.txt`, `fintech_pipeline_dag.py`, `extract_to_bronze.py`, `bronze_to_silver.py`, `load_silver_to_sqlserver.py`, `01_create_star.sql`, `load_gold.sql`, `drop_star_constraints.sql`, `add_star_constraints.sql`, `setup_warehouse.sql`, `dbt_project.yml`, `_sources.yml`, `rpt_daily_transactions.sql`, `generate_schema_name.sql`, `profiles.yml.example`, `run_all.ps1`, `run_gold.ps1`, `.env.example`*
