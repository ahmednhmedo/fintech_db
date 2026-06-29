# FinTech Lakehouse — Local Medallion Data Warehouse

> A portfolio-quality, end-to-end data engineering project built on a hybrid medallion architecture.  
> Raw CSV → Bronze → Silver → SQL Server Gold → dbt → Power BI.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Business Problem](#2-business-problem)
3. [Architecture Overview](#3-architecture-overview)
4. [Technology Stack](#4-technology-stack)
5. [Project Features](#5-project-features)
6. [Medallion Architecture](#6-medallion-architecture)
7. [Kimball Dimensional Model](#7-kimball-dimensional-model)
8. [Folder Structure](#8-folder-structure)
9. [Prerequisites](#9-prerequisites)
10. [Installation](#10-installation)
11. [Configuration](#11-configuration)
12. [How to Run](#12-how-to-run)
13. [Pipeline Flow](#13-pipeline-flow)
14. [dbt Layer](#14-dbt-layer)
15. [Power BI](#15-power-bi)
16. [Screenshots](#16-screenshots)
17. [Future Improvements](#17-future-improvements)
18. [Acknowledgements](#18-acknowledgements)

---

## 1. Project Overview

**FinTech Lakehouse** is a complete, local data lakehouse built to demonstrate enterprise-grade data engineering practices using a modern open-source stack. It ingests 1 million financial transactions across 8 structured datasets, transforms them through a three-layer medallion architecture (Bronze → Silver → Gold), enforces data quality with dbt, and delivers analytical dashboards via Power BI.

The project is designed as a hybrid system: containerized workloads (Apache Airflow, PySpark) handle the lake layers; a local Windows-hosted SQL Server houses the dimensional warehouse; dbt runs tests and builds reporting views; Power BI connects via Windows Authentication for zero-credential-management reporting.

| Attribute | Detail |
|---|---|
| **Data Volume** | 1,000,000 transactions, 8 source tables |
| **Architecture** | Medallion (Bronze / Silver / Gold) + Kimball Star Schema |
| **Orchestration** | Apache Airflow 2.9.3 (Dockerized) |
| **Compute** | PySpark 3.5.1 (local mode in Docker) |
| **Warehouse** | SQL Server Express (`ahmed\SQLEXPRESS`) |
| **Transformation** | dbt-core 1.8.7 + dbt-sqlserver 1.8.4 |
| **BI Tool** | Power BI Desktop (Import mode) |
| **Date Range** | 2023-01-01 → 2024-12-31 (731 days) |

---

## 2. Business Problem

A fintech company operating a digital payments platform in Egypt needs a unified data warehouse to answer operational and strategic questions:

- **Volume & Growth**: How much money moved through the platform each day, week, and month?
- **Decline Analysis**: Which transaction types and merchants have the highest failure rates?
- **FX Exposure**: What portion of volume is cross-border? What is the exchange rate distribution?
- **Customer Segmentation**: How do Standard, Premium, and Business tier customers differ in behaviour?
- **Geographic Distribution**: Which governorates drive the most transaction volume?
- **P2P Activity**: How does peer-to-peer transfer activity trend over time?

The raw operational data lives in flat CSV exports. Without a structured warehouse, answering these questions requires ad-hoc SQL on denormalized tables — slow, error-prone, and not scalable. This project transforms those raw exports into a star schema warehouse with pre-validated, analyst-ready data.

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        DOCKER (Linux)                           │
│                                                                 │
│  ┌──────────┐   ┌─────────────────┐   ┌─────────────────────┐  │
│  │ Airflow  │──▶│  Bronze Layer   │──▶│   Silver Layer      │  │
│  │ Scheduler│   │  (Parquet)      │   │   (Parquet, Spark)  │  │
│  └──────────┘   └─────────────────┘   └──────────┬──────────┘  │
│                                                   │             │
└───────────────────────────────────────────────────┼─────────────┘
                                                    │ pyodbc / Windows Auth
                                          ┌─────────▼──────────────┐
                                          │   WINDOWS HOST          │
                                          │                         │
                                          │  SQL Server [silver]    │
                                          │       ↓ load_gold.sql   │
                                          │  SQL Server [dbo]       │
                                          │    (Star Schema)        │
                                          │       ↓ dbt build       │
                                          │  SQL Server [reporting] │
                                          │       ↓                 │
                                          │    Power BI Desktop     │
                                          └─────────────────────────┘
```

The architecture is intentionally **hybrid**: Docker provides portability and compute isolation for data lake layers; the Windows host handles the warehouse because Windows Authentication to SQL Server cannot be performed from a Linux container without Kerberos configuration.

---

## 4. Technology Stack

| Layer | Technology | Version | Purpose |
|---|---|---|---|
| Orchestration | Apache Airflow | 2.9.3 | DAG scheduling, task dependency management |
| Containerization | Docker + Docker Compose | Latest | Service isolation and reproducibility |
| Lake Storage | Apache Parquet | — | Columnar, compressed immutable lake format |
| Compute | PySpark | 3.5.1 | Distributed type casting, deduplication, trimming |
| Ingestion | Python + Pandas | 3.11 / 2.2.2 | CSV reading with chunked processing (500K rows/chunk) |
| Lake I/O | PyArrow | 16.1.0 | Parquet read/write, columnar dataset API |
| Warehouse | SQL Server Express | 2019+ | Relational star schema, IDENTITY keys, indexed |
| DB Connectivity | pyodbc + ODBC Driver 18 | — | Windows-authenticated warehouse loading |
| Transformation | dbt-core + dbt-sqlserver | 1.8.7 / 1.8.4 | Data-quality tests, reporting views, schema management |
| Cloud Migration | dbt-fabric | 1.8.7 | Future Fabric/OneLake swap via profile change |
| BI Reporting | Power BI Desktop | — | Semantic model, DAX measures, dashboards |
| Metadata DB | PostgreSQL | 15 | Airflow internal metadata store |
| Automation | PowerShell | 5.1+ | Gold load orchestration on Windows host |

---

## 5. Project Features

- **Full medallion pipeline** — Raw CSV → Bronze Parquet → Silver Parquet → SQL Server Gold
- **PySpark transformation** — Type casting, string trimming, and deduplication on 1M+ rows
- **Kimball star schema** — 1 fact table, 7 dimensions, 2 outrigger dimensions
- **Role-playing dimension** — `dim_account` used twice in `fact_transactions` (primary actor + P2P peer)
- **dbt data-quality gate** — 38 automated tests covering uniqueness, NOT NULL, referential integrity, and value domains
- **Custom dbt reporting schema** — `rpt_daily_transactions` view materialized in a dedicated `[reporting]` schema
- **Idempotent pipeline** — TRUNCATE-and-reload design; re-runs produce identical results without data duplication
- **FK-safe reload** — Constraints dropped before truncate, re-added after insert for full referential integrity
- **Windows Auth integration** — Zero-credential warehouse access via Trusted_Connection
- **Import-mode Power BI** — 1M rows loaded into memory for fast DAX evaluation
- **Cloud migration path** — dbt-fabric pinned; swap profile `type: fabric` to migrate without changing models
- **Automated orchestration** — `run_all.ps1` and `run_gold.ps1` scripts for one-command execution

---

## 6. Medallion Architecture

The project implements a classic three-layer medallion pattern adapted for a local hybrid environment.

### Bronze Layer — Raw Landing Zone

| Attribute | Value |
|---|---|
| **Location** | `data/lake/bronze/<table>/` |
| **Format** | Apache Parquet (chunked parts) |
| **Populated by** | `ingestion/extract_to_bronze.py` via Airflow |
| **Transformations** | None — raw strings preserved exactly as-is |
| **Added columns** | `_ingested_at_utc`, `_source_file` (lineage) |
| **Idempotency** | Directory cleared before each run |

The bronze layer is the **immutable record of what was received**. All values are stored as strings using `dtype=str` to prevent any data loss or implicit type coercion during CSV reading. Data is written in 500,000-row Parquet chunks, making it suitable for processing by downstream Spark jobs.

### Silver Layer — Conformed ODS

| Attribute | Value |
|---|---|
| **Location** | `data/lake/silver/<table>/` |
| **Format** | Apache Parquet |
| **Populated by** | `spark/bronze_to_silver.py` via Spark Submit in Airflow |
| **Transformations** | Type casting, string trimming, deduplication |
| **Schema** | Explicit per-table column projections (lineage columns dropped) |

The silver layer is the **single source of truth for typed, clean data**. PySpark reads bronze Parquet, applies a schema dictionary (`SCHEMAS`) to cast every column to its correct SQL type, trims whitespace, converts empty strings to null, and deduplicates on each table's natural key. The result is a conformed ODS that mirrors the SQL Server warehouse schema exactly.

### Gold Layer — Dimensional Warehouse

| Attribute | Value |
|---|---|
| **Location** | SQL Server `fintech_db`, `[dbo]` schema |
| **Format** | Relational tables (star schema) |
| **Populated by** | `ingestion/load_silver_to_sqlserver.py` + `sql/load_gold.sql` |
| **Transformations** | FK-ordered insert; dimension surrogate keys preserved via `IDENTITY_INSERT ON`; `fact.transaction_sk` auto-generated by SQL Server IDENTITY; `dim_date` and `dim_time` use natural integer keys |
| **Integrity** | 11 foreign key constraints enforced post-load |

The gold layer is the **analyst-facing dimensional warehouse**. Silver Parquet is first loaded into a transient `[silver]` schema in SQL Server, then `load_gold.sql` inserts from silver into the star schema dimensions and fact table in foreign-key dependency order.

---

## 7. Kimball Dimensional Model

The warehouse follows the **Kimball bus architecture** with a single conformed bus (the `fact_transactions` table) and shared dimension keys across subject areas.

### Star Schema Diagram

```
                    ┌─────────────────┐
                    │   dim_date      │
                    │ PK: date_key    │◄─────────────────────┐
                    └────────┬────────┘                      │
                             │                               │
              ┌──────────────▼──────────────┐               │
              │         dim_account         │               │
              │  PK: account_key            │               │
              │  FK: location_key           │               │
              │  FK: signup_date_key ───────┼───────────────┘
              └──────────────┬──────────────┘
                             │ (account_key)
                             │ (peer_account_key, INACTIVE)
┌──────────────┐             │              ┌─────────────────────┐
│  dim_time    │             │              │   dim_transaction   │
│ PK: time_key │◄────────────▼──────────────│        _type        │
└──────────────┘    fact_transactions       │ PK: trans_type_key  │
                    ──────────────────      └─────────────────────┘
┌──────────────┐    PK: transaction_sk
│ dim_merchant │    FK: date_key           ┌─────────────────────┐
│PK:merchant_key◄───FK: time_key           │  dim_decline_reason │
│FK:location_key     FK: account_key       │ PK: decline_rsn_key │
│FK:opened_date──────FK: peer_account_key  └─────────────────────┘
└──────────────┘    FK: transaction_type_key
       │            FK: decline_reason_key
       │            FK: merchant_key
       ▼
┌──────────────┐
│ dim_location │
│PK:location_key (shared outrigger:
└──────────────┘  dim_account + dim_merchant)
```

### Dimension Summary

| Table | Rows | Grain | Key Design |
|---|---|---|---|
| `dim_date` | 731 | One row per calendar date (2023–2024) | Natural key `date_key` (YYYYMMDD) |
| `dim_time` | 1,440 | One row per minute of the day | Natural key `time_key` (HHMM) |
| `dim_location` | 19 | One row per city | Surrogate IDENTITY key; shared by accounts and merchants |
| `dim_account` | 40,000 | One row per customer account | Surrogate + natural (`account_id`) UNIQUE |
| `dim_merchant` | 1,200 | One row per merchant | Surrogate + natural (`merchant_id`) UNIQUE |
| `dim_transaction_type` | 13 | One row per transaction type | Surrogate IDENTITY |
| `dim_decline_reason` | 8 | One row per decline reason | Surrogate IDENTITY |
| `fact_transactions` | 1,000,000 | One row per transaction event | Surrogate `transaction_sk` IDENTITY + natural `transaction_id` UNIQUE |

### Key Modeling Decisions

**Role-Playing Dimension** — `dim_account` is linked to `fact_transactions` twice:
- `account_key` (ACTIVE): the account initiating the transaction
- `peer_account_key` (INACTIVE): the P2P counterparty; activated in DAX with `USERELATIONSHIP()`

**Outrigger Dimensions** — `dim_location` is shared by both `dim_account` (customer city) and `dim_merchant` (merchant city), reducing storage and ensuring geographic consistency.

**Integer Amount Storage** — `amount_minor` stores amounts in piastres (minor currency units) as BIGINT to avoid floating-point precision errors. `amount_egp` stores the human-readable EGP decimal for reporting.

**Exchange Rate Precision** — `exchange_rate_e6` stores the exchange rate multiplied by 1,000,000 as BIGINT, avoiding float storage in a financial context.

---

## 8. Folder Structure

```
fintech-lakehouse/
│
├── dags/
│   └── fintech_pipeline_dag.py        # Airflow DAG — extract + spark + handoff
│
├── ingestion/
│   ├── extract_to_bronze.py           # CSV → Bronze Parquet (chunked, with lineage)
│   └── load_silver_to_sqlserver.py    # Silver Parquet → SQL Server [silver] schema
│
├── spark/
│   └── bronze_to_silver.py            # PySpark: type cast, trim, deduplicate
│
├── sql/
│   ├── 01_create_star.sql             # Gold DDL: tables, IDENTITY, FKs, indexes
│   ├── setup_warehouse.sql            # One-time: create [silver] schema
│   ├── load_gold.sql                  # Silver → dbo.* star (FK-ordered, atomic)
│   ├── drop_star_constraints.sql      # Drop 11 FKs before TRUNCATE
│   └── add_star_constraints.sql       # Re-add 11 FKs after INSERT
│
├── dbt/fintech/
│   ├── dbt_project.yml                # dbt project config
│   ├── profiles.yml.example           # dbt-sqlserver profile template
│   ├── models/
│   │   ├── _sources.yml               # Source definitions + 38 data-quality tests
│   │   └── reporting/
│   │       └── rpt_daily_transactions.sql  # Daily aggregation view
│   ├── macros/
│   │   └── generate_schema_name.sql   # Custom schema naming (reporting schema)
│   └── target/                        # dbt build artifacts (gitignored)
│
├── data/
│   ├── raw/                           # Source CSV files (8 tables, 1.3M+ rows)
│   └── lake/
│       ├── bronze/                    # Bronze Parquet outputs
│       └── silver/                    # Silver Parquet outputs
│
├── powerbi/
│   └── fintech_db_powerbi.pbix        # Power BI Desktop model
│
├── docs/
│   ├── POWER_BI.md                    # Power BI build guide
│   └── POWERBI_MODEL_DIAGRAM.md       # Visual schema and relationship map
│
├── docker-compose.yml                 # 4-service Airflow + Postgres stack
├── Dockerfile                         # Airflow + Java + PySpark image
├── requirements.txt                   # Host Python packages
├── run_all.ps1                        # Full pipeline: Docker + gold + dbt
├── run_gold.ps1                       # Gold load + dbt only
├── .env.example                       # Environment variable template
├── HOW_TO_RUN.md                      # Quick-start guide
└── .gitignore                         # Excludes lake data, logs, .env, venv
```

---

## 9. Prerequisites

### Windows Host

| Requirement | Version | Notes |
|---|---|---|
| Windows 10/11 | 22H2+ | Windows Authentication required |
| Python | 3.11 | Must be on PATH |
| Docker Desktop | Latest | WSL2 backend enabled |
| SQL Server Express | 2019+ | Instance: `ahmed\SQLEXPRESS` (or update `.env`) |
| ODBC Driver 18 for SQL Server | Latest | Download from Microsoft |
| dbt-core | 1.8.7 | Installed via `requirements.txt` |
| PowerShell | 5.1+ | For `run_all.ps1` / `run_gold.ps1` |
| sqlcmd | Latest | Bundled with SQL Server tools |

### Network & Permissions

- SQL Server must allow Windows Authentication (Mixed Mode or Windows-only)
- The running Windows user must have `db_owner` or equivalent on `fintech_db`
- Docker Desktop must have access to the project directory (volume mounts)

---

## 10. Installation

```powershell
# 1. Clone the repository
git clone <repo-url>
cd "fintech-lakehouse"

# 2. Create a Python virtual environment on the host
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1

# 3. Install host dependencies (dbt, pyodbc, pyarrow, pandas)
pip install -r requirements.txt

# 4. Copy and configure environment variables
Copy-Item .env.example .env
# Edit .env — set WH_SERVER, WH_DATABASE, WH_DRIVER if different from defaults

# 5. Copy dbt profile to the user home directory
Copy-Item dbt\fintech\profiles.yml.example "$env:USERPROFILE\.dbt\profiles.yml"
# Edit profiles.yml — update server name if your SQL Server instance differs

# 6. Create the SQL Server database (run once)
sqlcmd -S "ahmed\SQLEXPRESS" -E -Q "CREATE DATABASE fintech_db"

# 7. Create the gold star schema (run once)
sqlcmd -S "ahmed\SQLEXPRESS" -E -d fintech_db -i sql\01_create_star.sql

# 8. Build the Docker image
docker compose build
```

---

## 11. Configuration

### `.env` File

| Variable | Default | Description |
|---|---|---|
| `WH_SERVER` | `ahmed\SQLEXPRESS` | SQL Server instance name |
| `WH_DATABASE` | `fintech_db` | Target database name |
| `WH_DRIVER` | `ODBC Driver 18 for SQL Server` | ODBC driver string |
| `WH_USER` | *(unset)* | SQL login (leave unset for Windows auth) |
| `WH_PASSWORD` | *(unset)* | SQL password (leave unset for Windows auth) |
| `LAKE_DIR` | `<repo>/data/lake` | Path to bronze/silver Parquet outputs |

### `~/.dbt/profiles.yml`

```yaml
fintech_db:
  target: dev
  outputs:
    dev:
      type: sqlserver
      driver: "ODBC Driver 18 for SQL Server"
      server: ahmed\\SQLEXPRESS
      database: fintech_db
      schema: dbo
      windows_login: true
      encrypt: true
      trust_cert: true
      threads: 4
```

---

## 12. How to Run

### Full Pipeline (Recommended)

Runs everything: Docker startup → bronze extraction → Spark silver → gold load → dbt tests.

```powershell
.\run_all.ps1
```

### Gold Layer Only

If bronze/silver Parquet already exists, skip Docker and go straight to warehouse loading.

```powershell
.\run_gold.ps1
```

### Step-by-Step Manual Run

```powershell
# Start Docker services
docker compose up -d --build

# Access Airflow UI — http://localhost:8081 (admin / admin)
# Trigger DAG: fintech_lakehouse
# Wait for all tasks to complete (extract → spark → handoff)

# Then on the host:
.\run_gold.ps1
```

### Verification

After a successful run, confirm these benchmarks in Power BI or SQL:

| Metric | Expected Value |
|---|---|
| Transaction Count | 1,000,000 |
| Gross Volume (EGP) | ≈ 689,181,271 |
| Average Ticket (EGP) | ≈ 689.18 |
| Declined Transactions | 42,478 |
| Decline Rate | ≈ 4.25% |
| FX Transactions | 43,783 |
| Distinct Active Accounts | 39,795 |
| P2P Transactions | 154,667 |

---

## 13. Pipeline Flow

```
data/raw/*.csv  (8 files, 1.3M+ rows)
        │
        │  extract_to_bronze.py
        │  • reads in 500K-row chunks
        │  • adds _ingested_at_utc, _source_file
        │  • writes Parquet → data/lake/bronze/
        ▼
data/lake/bronze/  (8 Parquet datasets)
        │
        │  bronze_to_silver.py (spark-submit)
        │  • casts types per SCHEMAS dict
        │  • trims strings, nullifies empties
        │  • deduplicates on natural key
        ▼
data/lake/silver/  (8 conformed Parquet datasets)
        │
        │  load_silver_to_sqlserver.py
        │  • reads with PyArrow Dataset API
        │  • bulk inserts in 50K-row batches
        │  • Windows auth via pyodbc
        ▼
SQL Server fintech_db [silver].*  (8 ODS tables)
        │
        │  drop_star_constraints.sql (via sqlcmd)
        │  • drops all 11 foreign key constraints
        │          ↓
        │  load_gold.sql (via sqlcmd)
        │  • TRUNCATEs all gold tables (safe: FKs already dropped)
        │  • INSERTs dimensions then fact in FK-dependency order
        │  • atomic: XACT_ABORT ON, single BEGIN…COMMIT transaction
        │          ↓
        │  add_star_constraints.sql (via sqlcmd)
        │  • re-adds all 11 foreign key constraints
        ▼
SQL Server fintech_db [dbo].*  (star schema: 7 dims + 1 fact)
        │
        │  dbt build
        │  • runs 38 data-quality tests
        │  • materializes rpt_daily_transactions view
        ▼
SQL Server fintech_db [reporting].rpt_daily_transactions
        │
        │  Power BI Desktop (Import mode)
        │  • Windows auth connection
        │  • 11 relationships defined
        │  • 21 recommended DAX measures
        ▼
Interactive Dashboards (6 pages)
```

---

## 14. dbt Layer

dbt plays two roles in this project:

### 1. Data-Quality Gate (38 Tests)

Tests run against the **gold star schema** after each load, acting as a post-load validation layer:

| Test Category | Count | What It Checks |
|---|---|---|
| Uniqueness | 10 | Every primary and natural key is distinct |
| Not Null | 14 | Required keys are never NULL |
| Accepted Values | 7 | Enums like `customer_tier`, `time_bucket`, boolean flags |
| Relationships | 7 | Every FK in `fact_transactions` resolves to a valid dimension row |

```bash
# Run tests manually
cd dbt/fintech
dbt build
```

### 2. Reporting View

`models/reporting/rpt_daily_transactions.sql` materializes a pre-aggregated daily summary view in the `[reporting]` schema:

| Column | Description |
|---|---|
| `full_date` | Calendar date |
| `year`, `month` | Date components |
| `is_weekend` | Weekend flag |
| `txn_count` | Total transactions |
| `declined_count` | Declined transactions |
| `fx_count` | FX transactions |
| `gross_volume_egp` | Total absolute amount |
| `avg_ticket_egp` | Average transaction size |

---

## 15. Power BI

The Power BI file (`powerbi/fintech_db_powerbi.pbix`) connects to SQL Server in **Import mode** via Windows Authentication.

**Connection:** Server `ahmed\SQLEXPRESS`, Database `fintech_db`

**Imported Objects:** 8 dimension/fact tables from `[dbo]` schema + optional `[reporting].rpt_daily_transactions`

**Key Relationships (11 total):**
- 7 fact-to-dimension links (standard)
- 1 inactive peer account link (activated in DAX with `USERELATIONSHIP`)
- 2 inactive date links (signup/opened dates on dimensions)

**Recommended DAX Measures** *(to be created in Power BI Desktop — see [`docs/POWER_BI.md`](docs/POWER_BI.md) Section 6 for the complete set of 21 measures)*:

```dax
Transaction Count = COUNTROWS(fact_transactions)

Gross Volume (EGP) = SUM(fact_transactions[abs_amount_egp])

Decline Rate % = DIVIDE(
    CALCULATE([Transaction Count], fact_transactions[is_declined] = TRUE()),
    [Transaction Count]
)

Volume YTD = TOTALYTD([Gross Volume (EGP)], dim_date[full_date])

P2P Volume to Peer (EGP) = CALCULATE(
    [Gross Volume (EGP)],
    USERELATIONSHIP(fact_transactions[peer_account_key], dim_account[account_key])
)
```

See [`docs/POWER_BI.md`](docs/POWER_BI.md) for the full guide including all 21 recommended DAX measures to create in Power BI Desktop, relationship setup, and troubleshooting.

---

## 16. Screenshots

> **Note:** Screenshots to be added after Power BI dashboard is fully built and published.

| Page | Placeholder |
|---|---|
| Executive Overview | `docs/screenshots/01_executive_overview.png` |
| Transactions & Channels | `docs/screenshots/02_transactions_channels.png` |
| Geography | `docs/screenshots/03_geography.png` |
| Merchants | `docs/screenshots/04_merchants.png` |
| Customer Analysis | `docs/screenshots/05_customers.png` |
| Risk & FX | `docs/screenshots/06_risk_fx.png` |

---

## 17. Future Improvements

| Priority | Improvement | Description |
|---|---|---|
| High | **Cloud Migration** | Swap dbt profile `type: sqlserver` → `type: fabric`; replace host loader with Fabric COPY INTO. dbt-fabric is already pinned in requirements. |
| High | **SCD Type 2** | Implement versioned history for slowly changing attributes: `age_band`, `customer_tier`, `merchant_size`. |
| Medium | **Incremental Loading** | Replace TRUNCATE+reload with MERGE statements for faster nightly runs and partial refresh support. |
| Medium | **Streaming Silver** | Add Kafka or Azure Event Hubs consumer to feed the silver layer in near-real-time instead of batch CSV. |
| Medium | **dbt Semantic Layer** | Formalize KPIs as dbt Metrics for a vendor-neutral semantic layer consumable by multiple BI tools. |
| Low | **Data Masking** | Implement column-level masking on `account_id` and customer PII for non-privileged analyst access. |
| Low | **Kubernetes** | Replace Docker Compose with Kubernetes manifests (Helm chart for Airflow) for production-scale orchestration. |

---

## 18. Acknowledgements

This project was developed as a capstone data engineering project demonstrating:

- Modern data lakehouse patterns (medallion architecture)
- Kimball dimensional modelling principles
- Hybrid on-premises / containerized infrastructure design
- Production-grade data quality validation with dbt
- End-to-end Power BI semantic modelling

Built with Apache Airflow, PySpark, SQL Server, dbt, and Power BI Desktop.

---

*Generated: 2026-06-29 | Architecture: Medallion (Bronze/Silver/Gold) | Stack: Airflow · Spark · SQL Server · dbt · Power BI*
