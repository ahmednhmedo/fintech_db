# How to Run — FinTech Lakehouse (local, hybrid)

A medallion lakehouse on the modern stack, fully local:
**Airflow + PySpark in Docker** (bronze/silver) → **SQL Server `fintech_db` via dbt + PowerShell on the host** (gold), Windows authentication.

```
8 CSVs ─[Airflow]→ bronze Parquet ─[PySpark]→ silver Parquet
                                                  │ (host, Windows auth)
                                  load_silver_to_sqlserver.py → [silver] schema
                                                  │
                                  load_gold.sql → dbo.* star (your DDL) ─ dbt tests
                                                  │
                                            Power BI Desktop
```
**Why the split:** Windows auth to `ahmed\SQLEXPRESS` can't be done from a Linux container, so Docker does bronze+silver and the **host** does gold.

---

---
# PART 1 — BATCH / INITIAL LOAD PIPELINE
> Everything in this section covers the **one-time bulk load**: 8 CSVs → Bronze → Silver → SQL Server star schema → dbt.
> If you are looking for the streaming incremental pipeline, jump to [Part 2 — Incremental Load Pipeline](#part-2--incremental-load-pipeline).
---

## One-time setup

1. **SQL Server**: `fintech_db` exists on `ahmed\SQLEXPRESS`. The star tables are
   created automatically from `sql\01_create_star.sql` on first gold run (or run it yourself in SSMS).
2. **ODBC**: install **"ODBC Driver 18 for SQL Server"**.
3. **Host Python 3.11 venv** with the deps:
   ```powershell
   py -3.11 -m pip install -r requirements.txt
   ```
4. **dbt profile**: copy `dbt\fintech\profiles.yml.example` → `%USERPROFILE%\.dbt\profiles.yml`.
5. **Docker Desktop** running.

---

## Quick Commands

Everything you need at a glance — run all commands from the `fintech-lakehouse` folder.
(Full explanations are in the sections below.)

| Task | Command |
|---|---|
| Run the complete pipeline | `.\run_all.ps1` |
| Stop the project (recommended) | `docker compose down` |
| Start Docker services only | `docker compose up -d` |
| Restart after stopping | `.\run_all.ps1` (rebuild data) **or** `docker compose up -d` (runtime only) — see [Restarting](#restarting-the-project) |
| View running containers | `docker compose ps` |
| View scheduler logs | `docker compose logs -f airflow-scheduler` |
| View webserver logs | `docker compose logs -f airflow-webserver` |
| Stop without removing containers | `docker compose stop` |
| Execute dbt manually | `cd dbt\fintech; dbt build` (host Python 3.11 venv) |
| Gold load + dbt only (silver already built) | `.\run_gold.ps1` |
| Open Airflow UI | `http://localhost:8081` (admin / admin) |
| Full metadata reset (advanced, destructive) | `docker compose down -v` — see [When to use `docker compose down -v`](#when-to-use-docker-compose-down--v) |

---

## Typical Daily Usage

The three workflows you'll actually use, in order of how often you'll need them.

### First time running the project

1. **Clone the repository.**
2. **Configure prerequisites** (see [One-time setup](#one-time-setup): SQL Server `fintech_db`, ODBC Driver 18,
   host Python 3.11 venv, dbt profile, Docker Desktop running).
3. **Run the full pipeline:**
   ```powershell
   .\run_all.ps1
   ```

Expected result:

- Docker starts (Airflow scheduler + webserver + Postgres).
- Bronze executes (CSV → bronze Parquet).
- Silver executes (PySpark conform/type/dedupe → silver Parquet).
- The SQL Server warehouse loads (`[silver]` → `dbo.*` star, FK-safe atomic reload).
- dbt validates the warehouse (38 data tests + the reporting view; a 39-node `dbt build`).
- Airflow becomes available on **http://localhost:8081** (admin / admin).

### Continuing work the next day

If the warehouse already exists and you just want the runtime back up (e.g., the Airflow UI):

```powershell
docker compose up -d
```

If the **source data changed** (or you want to rebuild the warehouse and refresh Power BI):

```powershell
.\run_all.ps1
```

### Finished working

```powershell
docker compose down
```

Recommended because it cleanly removes the disposable containers while keeping every piece of persistent
data (the `pgdata` volume, SQL Server warehouse, and host-side lake/docs/`.pbix`) intact.

---

## Expected Runtime

Approximate, on a typical local developer machine — actual times vary with hardware (CPU, RAM, disk) and
whether the Docker image is already built. Spark's JVM startup dominates the Silver step.

| Operation | Approximate Time |
|---|---|
| Docker startup | ~15–30 seconds |
| Bronze (CSV → Parquet) | ~30–60 seconds |
| Silver (PySpark conform) | ~1–2 minutes |
| SQL Server load (silver → gold) | ~3–6 minutes |
| dbt validation | ~30–60 seconds |
| **Full pipeline (`run_all.ps1`)** | **~5–10 minutes** |

---

## Run the whole pipeline (rebuild)

```powershell
cd "f:\NTI FOLDER\NTI FINAL PROJECTssssssssssssssss\fintech-lakehouse"
.\run_all.ps1
```
Docker (Airflow+Spark) builds bronze+silver, then the host loads gold and runs `dbt build`. Finishes with the fact row count + average ticket. Then open Power BI → **Refresh**.

### Or drive it from the Airflow UI
- `docker compose up -d --build` → open **http://localhost:8081** (admin/admin) → trigger DAG **`fintech_lakehouse`** (runs `extract → spark silver → gold_handoff`).
- Then run `.\run_gold.ps1` on the host for the gold + dbt step.

---

## Just show it (no Docker)
Your warehouse persists in SQL Server. Open **SSMS** (`fintech_db` → Database Diagrams)
and **Power BI Desktop** (open the `.pbix`, Refresh). That's it.

---

## Layout
| Path | Role |
|------|------|
| `dags/` | Airflow DAG (bronze + silver orchestration) |
| `ingestion/extract_to_bronze.py` | 8 CSV → bronze Parquet |
| `spark/bronze_to_silver.py` | conform/type/dedupe → silver Parquet |
| `ingestion/load_silver_to_sqlserver.py` | silver Parquet → `[silver]` (Windows auth) |
| `sql/01_create_star.sql` | your gold DDL (IDENTITY/FK/indexes) |
| `sql/load_gold.sql` | `[silver]` → `dbo.*` star (IDENTITY_INSERT, FK-ordered) |
| `sql/drop_/add_star_constraints.sql` | FK drop/re-add around the reload |
| `dbt/fintech/` | data-quality tests on the star + a reporting view |
| `run_gold.ps1` / `run_all.ps1` | host gold runner / full pipeline |

---

## Stopping the Project

When you're done for the session, stop the Docker stack from the `fintech-lakehouse` folder:

```powershell
docker compose down
```

**This is the recommended command for this project.** It stops and removes the containers and the
project network, but it **keeps the named volume `pgdata`** (the Airflow metadata database). Named
volumes are only deleted when you explicitly pass `-v` (see [When to use `docker compose down -v`](#when-to-use-docker-compose-down--v)).

### `docker compose down` vs `docker compose stop`

| Command | What it does | Resume with |
|---|---|---|
| `docker compose down` *(recommended)* | Stops **and removes** the containers + network. Keeps the `pgdata` named volume. Leaves a clean slate; next start recreates fresh containers. | `docker compose up -d` (or `.\run_all.ps1`) |
| `docker compose stop` | Only **pauses** the containers — they are kept, not removed. Fastest to resume. Also keeps `pgdata`. | `docker compose start` |

Use `docker compose down` for a clean stop that pairs naturally with this project's
`up -d --build` / `run_all.ps1` startup. Use `docker compose stop` if you just want to pause briefly
and resume the exact same containers quickly. **Both keep all your data** — neither one deletes the
`pgdata` volume.

### Why this project recommends `docker compose down` (not `stop`)

- **`run_all.ps1` is designed to recreate the runtime cleanly.** It always does `docker compose up -d --build`,
  so it expects to (re)build and (re)start fresh containers. Starting from a fully-removed state is exactly
  what it's built for — there's no benefit to keeping paused containers around between runs.
- **Containers are disposable.** They hold no irreplaceable state. Removing them on shutdown and recreating
  them on the next run is the normal, intended lifecycle here — not something to avoid.
- **The project intentionally separates runtime containers from persistent data.** Anything you care about
  lives *outside* the containers: Airflow metadata in the named `pgdata` volume, the warehouse in SQL Server,
  and Bronze/Silver/CSV/docs/`.pbix` on the host filesystem. So tearing the containers down is safe by design.
- **`docker compose down` removes only the containers and the Docker network** — nothing else. Specifically, it
  does **NOT** delete:
  - the named **`pgdata`** volume (Airflow metadata),
  - **SQL Server** / the `fintech_db` warehouse,
  - **Bronze/Silver** bind-mounted lake data under `./data`,
  - **documentation** in the repo,
  - **Power BI** `.pbix` files.

Because the only thing it discards is disposable runtime, `docker compose down` is the clean, recommended
shutdown for normal daily development.

Using the recommended `docker compose down` does **NOT** delete any of the following:

- **Airflow metadata** — preserved in the named `pgdata` volume (survives `down`).
- **SQL Server warehouse** — `fintech_db` lives in SQL Server on the Windows host, completely outside Docker.
- **Bronze/Silver lake data** — stored on the host bind mount `./data` (mounted as `/opt/data`), not in a Docker volume.
- **Power BI files** — the `.pbix` lives on the host filesystem.
- **dbt project** — the `dbt/fintech` folder is plain source on the host.

---

## Restarting the Project

To start again after stopping, open Docker Desktop, then from the `fintech-lakehouse` folder choose the
option that matches what you want to do:

### Option A — `.\run_all.ps1`  (rebuild the data)

```powershell
.\run_all.ps1
```

- Runs the **entire pipeline** from Bronze through Silver, Gold, and `dbt build`.
- Brings the Docker runtime up (`up -d --build`) **and** produces/refreshes the warehouse.
- **Use this** after changing source data, after a `down`/`down -v`, or whenever you want to rebuild the
  warehouse and refresh Power BI. This is the normal "I want current data" restart.

### Option B — `docker compose up -d`  (runtime only)

```powershell
docker compose up -d
```

- Starts **only the Docker runtime** (Airflow scheduler + webserver + Postgres).
- Does **NOT** execute the ETL pipeline — no Bronze/Silver/Gold is (re)built, nothing is loaded to SQL Server.
- **Use this** when you only want the Airflow UI or the container environment up (e.g., to trigger the DAG
  manually from the UI, or to inspect logs) without rebuilding data.

> If you only paused with `docker compose stop`, resume the exact same containers with `docker compose start`.

- The named **`pgdata`** volume preserves the **Airflow metadata** across restarts — your DAG, connections,
  and run history come back exactly as they were. (`run_all.ps1` does not record DAG runs; it executes
  bronze+silver directly in the scheduler container — see the run section above.)
- The **SQL Server warehouse is completely independent from Docker**. `fintech_db` on `ahmed\SQLEXPRESS`
  keeps the gold star, the `[silver]` schema, and the `[reporting]` view whether Docker is up or down. You
  can open Power BI and query the warehouse with the containers stopped — Docker only matters when you need
  to rebuild bronze/silver.

---

## When to use `docker compose down -v`

```powershell
docker compose down -v
```

The `-v` flag **removes the Docker volumes**, including the named **`pgdata`** volume. This is a
**destructive reset** and is **NOT part of normal operation**. Use it only when you deliberately want a
**complete reset of the Airflow metadata database** (for example, the metadata DB is corrupted, or you want
to wipe DAG run history and start Airflow from scratch).

**What `-v` deletes:**

- The `pgdata` named volume → **all Airflow metadata** (DAG run history, task instances, connections,
  the `admin` user — recreated on next `up` by the idempotent `airflow-init`).

**What `-v` does NOT delete** (it cannot reach any of these — they live outside Docker volumes):

- **SQL Server database `fintech_db`** — on the Windows host, untouched.
- **The warehouse schema** — `[dbo]` star, `[silver]`, `[reporting]` all remain in SQL Server.
- **Bronze/Silver data** — stored on the host bind mount `./data`, not in a Docker volume.
- **Source CSV files** — `data/raw/*.csv` on the host.
- **Documentation** — the `docs/` and root markdown files on the host.
- **Power BI reports** — the `.pbix` on the host filesystem.

> After a `down -v`, the next `docker compose up -d` (or `.\run_all.ps1`) recreates the metadata DB and the
> `admin` user automatically. Your warehouse and lake data are unaffected.

---

## Daily Development Workflow

The expected day-to-day loop:

1. **Open Docker Desktop** (wait until it reports running).
2. **Navigate to the repository** in PowerShell:
   ```powershell
   cd "f:\NTI FOLDER\NTI FINAL PROJECTssssssssssssssss\fintech-lakehouse"
   ```
3. **Run the full pipeline:**
   ```powershell
   .\run_all.ps1
   ```
   This builds bronze + silver in Docker, then loads gold and runs `dbt build` on the host.
4. **Build or update the Power BI report** — open the `.pbix` and **Refresh** to pull the latest gold data.
5. **Commit your changes** (code, SQL, dbt, docs — never `.env`, lake data, or `target/`; these are gitignored).
6. **Stop Docker** with the recommended command:
   ```powershell
   docker compose down
   ```

This is the normal workflow. Because every stage is idempotent, you can re-run `.\run_all.ps1` as often as
you like and always land on the same result.

---

## Common Operational Commands

All commands run from the `fintech-lakehouse` folder. The Compose project name is **`fintech_lakehouse_new`**.

| Task | Command |
|---|---|
| Start project (containers only) | `docker compose up -d` |
| Run full pipeline (Docker + gold + dbt) | `.\run_all.ps1` |
| Run gold load + dbt only (silver already built) | `.\run_gold.ps1` |
| Stop project (recommended) | `docker compose down` |
| Pause project (keep containers) | `docker compose stop` |
| Restart after pause | `docker compose start` |
| Check running containers | `docker compose ps` |
| View Docker logs (all services, follow) | `docker compose logs -f` |
| View scheduler logs only | `docker compose logs -f airflow-scheduler` |
| Check Airflow status (list DAGs) | `docker compose exec airflow-scheduler airflow dags list` |
| Open Airflow UI | Browse to **http://localhost:8081** (admin / admin) |
| Trigger the DAG from the UI | UI → DAG **`fintech_lakehouse`** → Trigger |
| Run dbt manually | `cd dbt\fintech; dbt build` (host Python 3.11 venv) |
| Check SQL Server connection (Windows auth) | `sqlcmd -S "ahmed\SQLEXPRESS" -E -d fintech_db -Q "SELECT COUNT(*) FROM dbo.fact_transactions;"` |
| Full metadata reset (destructive) | `docker compose down -v` |

---

## Operational Notes

- **Docker containers are disposable.** They hold no irreplaceable state — stop, remove, and recreate them
  freely. Only the `pgdata` named volume (Airflow metadata) persists inside Docker.
- **SQL Server is the system of record.** The warehouse (`fintech_db` on `ahmed\SQLEXPRESS`) is the
  authoritative, durable store and lives entirely outside Docker.
- **Docker stores only Airflow metadata.** The single named volume `pgdata` backs the Airflow Postgres
  database — nothing else of yours is kept in a Docker volume.
- **Bronze/Silver are persisted on host bind mounts.** They live in `./data/lake/` on the host (mounted into
  the container as `/opt/data`), so they survive container removal and are rebuilt from `data/raw` on each run.
- **The project is idempotent and safe to rerun.** Bronze clears its output before writing, Silver
  deduplicates and overwrites, and the gold load TRUNCATEs-and-reloads inside a single atomic transaction —
  re-running `.\run_all.ps1` always yields the same result, never duplicates.
- **Power BI reads from SQL Server after Gold is loaded.** Open the `.pbix` and **Refresh** once a run
  finishes; Power BI connects to `ahmed\SQLEXPRESS` / `fintech_db` via Windows Authentication in Import mode.

---

## Common Mistakes

- **Running `docker compose up -d` and expecting fresh warehouse data.**
  That command only starts the Docker containers — it does **not** run the ETL. No Bronze/Silver is rebuilt
  and nothing is loaded into SQL Server. To rebuild the warehouse, run `.\run_all.ps1` instead.

- **Using `docker compose down -v` during normal work.**
  The `-v` flag deletes the `pgdata` volume — your entire Airflow metadata database (DAG history, the admin
  user). It is **almost never** what you want. Use plain `docker compose down` to stop; reserve `down -v` only
  for a deliberate, complete reset of the Airflow metadata DB (e.g., it's corrupted). It does **not** touch
  SQL Server, the warehouse, or the host-side lake data — but losing metadata for no reason is still avoidable.

- **Forgetting to start SQL Server.**
  If the `ahmed\SQLEXPRESS` service isn't running, the gold steps fail at connection time. Symptom: the host
  steps (silver loader, `sqlcmd`, or `dbt build`) error with a connection/login failure such as *"server was
  not found or was not accessible."* Start the SQL Server (SQLEXPRESS) service and re-run `.\run_gold.ps1`.

- **Expecting Airflow to orchestrate the full end-to-end pipeline.**
  It doesn't — by design. Airflow (in Docker) owns **Bronze/Silver only**; the **Gold load and dbt run on the
  Windows host**. This split exists because **Windows Authentication to SQL Server cannot be performed from
  inside a Linux container**. The Airflow DAG's final task is a handoff marker; the host's `run_gold.ps1`
  finishes the pipeline. Use `.\run_all.ps1` (or the DAG for Bronze/Silver, then `.\run_gold.ps1`) — don't wait
  for Airflow to load the warehouse on its own.

---

## Migrating to the cloud later (phase 2)
Swap the dbt `type` (→ `fabric`) and replace the host loader with `COPY INTO`
(Fabric/OneLake). The DAG, Spark jobs, SQL star, and dbt tests are unchanged.

---

---
# PART 2 — INCREMENTAL LOAD PIPELINE
> Everything below this point covers the **event-driven streaming pipeline**: FastAPI → Kafka → Bronze JSONL → Silver Parquet → SQL MERGE → dbt.
> The batch pipeline (Part 1) must have been run at least once before starting this — the dimension tables must exist in SQL Server.
---

## Running the Incremental Pipeline

The incremental pipeline streams new transactions event-by-event through Kafka into the warehouse,
on top of the existing batch Gold layer. It is completely independent — it does not modify or rerun
the batch pipeline.

```
FastAPI (port 8000)
  → producer.py → Kafka (port 9092, Docker KRaft)
  → consumer.py → data/incremental/bronze/transactions_raw.jsonl
  → silver_incremental.py (Docker Spark)
  → load_silver_incremental_to_sql.py → stg.* staging + MERGE → dbo.fact_transactions
  → dbt build (same fintech project, validates the full warehouse)
```

All commands below assume you are in the `fintech-lakehouse` folder unless otherwise noted.

### Prerequisites
- Batch pipeline already run at least once (dimensions must exist in SQL Server).
- Docker Desktop running.
- Incremental venv already created (one-time):
  ```powershell
  cd incremental
  py -3.11 -m venv .venv-incremental
  .\.venv-incremental\Scripts\Activate.ps1
  pip install -r requirements-incremental.txt
  ```

### Step 1 — Start Kafka (Terminal A)

```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\incremental"
docker compose up -d

# Create the topic (safe to re-run; skipped if it already exists)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 `
  --create --if-not-exists --topic transactions_raw --partitions 1 --replication-factor 1

# Verify
docker compose ps        # kafka should show "running"
```

### Step 2 — Start FastAPI source (Terminal B)

```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\incremental\api"
..\.venv-incremental\Scripts\Activate.ps1
uvicorn source_api:app --host 0.0.0.0 --port 8000
```

Verify: open `http://localhost:8000/transactions/next` in a browser — you should see a JSON transaction.

### Step 3 — Start the consumer FIRST (Terminal C)

Always start the consumer before the producer so no messages are missed.

```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\incremental"
.\.venv-incremental\Scripts\Activate.ps1
python consumer.py
```

Expected output:
```
Starting consumer | broker=localhost:9092 | topic=transactions_raw
Consumed transaction_id=TXN... -> Bronze [partition 0 @ offset 0]
```

### Step 4 — Start the producer (Terminal D)

```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\incremental"
.\.venv-incremental\Scripts\Activate.ps1
python producer.py
```

Expected output (1 event per second):
```
Published #1 transaction_id=TXN...
Delivered -> transactions_raw [partition 0 @ offset 0]
```

Let it run for 30–60 seconds, then press **CTRL+C** in the producer terminal to stop it.
Press **CTRL+C** in the consumer terminal once it has drained (a few seconds after the producer stops).

### Step 5 — Run Silver incremental (Docker Spark)

> **Important:** PySpark cannot run directly on this host (no Java installed).
> Always use the Docker Spark command below — never `python silver_incremental.py` directly.

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

Takes ~30–45 seconds (Spark JVM startup). Look for the final summary line:
```
[silver] batch=b_000000000_000000098 | new=99 | silver=99 | quarantined=0 | checkpoint advanced.
```

### Step 6 — Load Silver into SQL Server (MERGE)

```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse"
& (& py -3.11 -c "import sys; print(sys.executable)").Trim() "incremental\load_silver_incremental_to_sql.py"
```

Expected output:
```
Silver batches: 1 on disk | 0 already loaded | 1 to process
staged 99 rows for batch b_000000000_000000098
batch b_... -> staged=99 valid=99 inserted=N updated=M rejected=0
Done in 0.4s | inserted=N updated=M rejected=0 | failed_batches=0
```

> **Note on `inserted=0`:** If these transaction IDs were already loaded by the initial batch pipeline,
> the MERGE correctly skips them (idempotency). This is expected behaviour, not an error.

### Step 7 — Run dbt build

```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\dbt\fintech"
dbt build
```

Expected result:
```
Done. PASS=47 WARN=0 ERROR=0 SKIP=0 TOTAL=47
```

### Incremental Pipeline Quick Reference

| Task | Command |
|---|---|
| Start Kafka | `cd incremental && docker compose up -d` |
| Create Kafka topic | `docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic transactions_raw --partitions 1 --replication-factor 1` |
| Start FastAPI | `cd incremental\api && uvicorn source_api:app --host 0.0.0.0 --port 8000` |
| Start consumer | `cd incremental && python consumer.py` |
| Start producer | `cd incremental && python producer.py` |
| Run Silver (Docker Spark) | See Step 5 above |
| Run SQL MERGE loader | `py -3.11 incremental\load_silver_incremental_to_sql.py` |
| Run dbt | `cd dbt\fintech && dbt build` |
| Check Bronze records | `(Get-Content data\incremental\bronze\transactions_raw.jsonl \| Measure-Object -Line).Lines` |
| Check audit log | `sqlcmd -S "ahmed\SQLEXPRESS" -E -C -d fintech_db -Q "SELECT * FROM stg.incremental_load_log ORDER BY log_id DESC;"` |

---

## Clean Shutdown (Full Project — Batch + Incremental)

Follow this order to shut everything down cleanly before closing your PC.
Running in the wrong order is harmless but this order is the cleanest.

### Step 1 — Stop the producer and consumer

In each terminal where `producer.py` or `consumer.py` is running, press **CTRL+C**.
Both scripts shut down cleanly: the producer flushes in-flight messages, the consumer commits
its final offset and triggers a group rebalance.

### Step 2 — Stop FastAPI and dbt docs

If you have FastAPI (port 8000) or dbt docs (port 8082) running, stop them:

```powershell
# Stop FastAPI (port 8000)
$pid8000 = Get-NetTCPConnection -LocalPort 8000 -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess
if ($pid8000) { Stop-Process -Id $pid8000 -Force; Write-Host "FastAPI stopped" }

# Stop dbt docs (port 8082)
$pid8082 = Get-NetTCPConnection -LocalPort 8082 -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess
if ($pid8082) { Stop-Process -Id $pid8082 -Force; Write-Host "dbt docs stopped" }
```

### Step 3 — Stop Kafka

```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\incremental"
docker compose down
```

This stops and removes the Kafka container and its network. The `kafka_kraft` volume (broker data)
is **kept** by default — add `-v` only if you want to wipe the topic and start completely fresh.

### Step 4 — Stop Airflow (batch pipeline)

```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse"
docker compose down
```

This stops Airflow webserver, scheduler, and Postgres. The `pgdata` volume (Airflow metadata,
DAG run history) is **kept** — add `-v` only for a full metadata reset (see above).

### Step 5 — Verify everything is stopped

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"

# All four ports should be free:
@(8000, 8081, 8082, 9092) | ForEach-Object {
    $c = Get-NetTCPConnection -LocalPort $_ -ErrorAction SilentlyContinue
    if ($c) { Write-Host "Port $_ still in use" } else { Write-Host "Port $_ free" }
}
```

All four ports should report **free**. You can now safely close VSCode and shut down your PC.

### What is safe after shutdown

| Data | Survives shutdown | Location |
|---|---|---|
| SQL Server warehouse (`fintech_db`) | Yes | Windows host — SQL Server service |
| Bronze/Silver lake files | Yes | `data/` on host filesystem |
| Incremental Bronze JSONL | Yes | `data/incremental/bronze/` |
| Silver Parquet + checkpoint | Yes | `data/incremental/silver/` + `_checkpoints/` |
| Airflow metadata (DAG history) | Yes | `pgdata` Docker named volume |
| Kafka broker volume | Yes | `kafka_kraft` Docker named volume |
| Screenshots | Yes | `presentation/screenshots/` |
| dbt project | Yes | `dbt/fintech/` on host |

Nothing is lost by shutting down — all data is persisted to disk or Docker named volumes.
