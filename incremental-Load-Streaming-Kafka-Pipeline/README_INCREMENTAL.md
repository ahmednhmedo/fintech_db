# Incremental Loading Pipeline (Kafka) — `fintech-lakehouse`

Event-driven incremental ingestion of **new transactions only**, bolted onto the
existing batch pipeline at the Bronze boundary. Nothing in the batch project is
modified — everything here lives under `incremental/` and `data/incremental/`.

```
fact_transactions.csv
        │
        ▼
FastAPI  (api/source_api.py — simulated source system)   :8000/transactions/next
        │
        ▼
producer.py  ──►  Apache Kafka  (topic: transactions_raw)  ──►  consumer.py
                                                                      │
                                                                      ▼
                              data/incremental/bronze/transactions_raw.jsonl   (Bronze Incremental)
                              data/incremental/dlq/transactions_bad.jsonl      (poison messages)
```

> **Why streaming only the fact, why FastAPI, why Kafka, and how this rejoins the
> batch Silver→SQL→dbt→Power BI stages** — see the "Architecture Explanation"
> notes that accompany this pipeline. Dimensions are already batch-loaded; only
> the append-only fact table streams.

---

## Folder structure (added by this task)

```
fintech-lakehouse/
├── incremental/
│   ├── docker-compose.yml            # Kafka (KRaft, no ZooKeeper)
│   ├── producer.py                   # API -> Kafka
│   ├── consumer.py                   # Kafka -> Bronze Incremental + DLQ
│   ├── requirements-incremental.txt
│   ├── README_INCREMENTAL.md         # this file
│   └── api/
│       └── source_api.py             # FastAPI simulated source system
└── data/
    └── incremental/
        ├── .gitignore
        ├── bronze/transactions_raw.jsonl   # created on first consumed event
        └── dlq/transactions_bad.jsonl      # created on first bad event
```

---

## Required Python packages

```
confluent-kafka, fastapi, uvicorn[standard], requests
```

(pinned in `requirements-incremental.txt`)

---

## Prerequisites
* Docker Desktop running
* **Python 3.11 or 3.12 only** — do **not** use Python 3.13/3.14: `confluent-kafka`
  has no prebuilt wheels for them yet, so `pip install` tries (and usually fails)
  to build from source. Create the venv explicitly with 3.11/3.12.
* The batch initial load already done (dimensions present). This pipeline only
  reads `data/raw/fact_transactions.csv` as its event source.

---

## Step-by-step execution guide (PowerShell, 4 terminals)

All commands assume you start at the project root: `f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse`.

### 0) One-time: create an isolated venv and install deps
Use Python 3.11 or 3.12 (the `py -3.11` launcher selects it explicitly):
```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\incremental"
py -3.11 -m venv .venv-incremental
.\.venv-incremental\Scripts\Activate.ps1
pip install -r requirements-incremental.txt
```

### 1) Terminal A — start Kafka
```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\incremental"
docker compose up -d
docker compose ps          # kafka should be "running"
```

### 2) Create the Kafka topic
Auto-creation is enabled by default on this image, but create it explicitly so
the partition count is intentional. `--if-not-exists` makes this safe to re-run
(e.g. after `docker compose down` without `-v`, when the topic still exists):
```powershell
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 `
  --create --if-not-exists --topic transactions_raw --partitions 1 --replication-factor 1

# verify
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 `
  --describe --topic transactions_raw
```

### 3) Terminal B — start the FastAPI source system
```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\incremental\api"
..\.venv-incremental\Scripts\Activate.ps1
uvicorn source_api:app --host 0.0.0.0 --port 8000
# quick check (new shell or browser): http://localhost:8000/transactions/next
```

### 4) Terminal C — start the consumer (start it BEFORE the producer)
```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\incremental"
.\.venv-incremental\Scripts\Activate.ps1
python consumer.py
```

### 5) Terminal D — start the producer (1 event / second)
```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\incremental"
.\.venv-incremental\Scripts\Activate.ps1
python producer.py
```

Stop producer/consumer with **CTRL+C** (both shut down cleanly).
Stop Kafka with `docker compose down` (add `-v` to also wipe the broker volume).

---

## Validation checklist

- [ ] `docker compose ps` shows `kafka` running.
- [ ] `kafka-topics --describe` lists `transactions_raw` with 1 partition.
- [ ] `GET http://localhost:8000/transactions/next` returns a JSON transaction,
      and a second call returns a **different** row (cursor advances).
- [ ] `http://localhost:8000/health` shows `rows_served` increasing.
- [ ] Producer logs `Published #N ...` then `Delivered -> transactions_raw [...]`.
- [ ] Consumer logs `Consumed transaction_id=... -> Bronze [...]`.
- [ ] `data/incremental/bronze/transactions_raw.jsonl` grows by one line/second,
      and each line contains `_kafka_topic`, `_kafka_partition`, `_kafka_offset`,
      `_consumed_at_utc` plus the original `payload`.
- [ ] Stop the consumer, let the producer publish a few more, restart the
      consumer → it resumes from the committed offset (no loss, at-least-once).
- [ ] Inspect end-to-end from the broker:
      ```powershell
      docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 `
        --topic transactions_raw --from-beginning --max-messages 3
      ```
- [ ] DLQ: `data/incremental/dlq/transactions_bad.jsonl` stays empty during a
      normal run (only fills if a non-JSON message is published).

---

## Cleanup
```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\incremental"
docker compose down -v     # stop Kafka and remove its volume
```

---

## Future integration (explanation only — NOT implemented here)

```
Bronze Incremental (transactions_raw.jsonl)
   │  read new lines past the last watermark
   ▼
Silver Incremental Cleaning  — reuse the existing Silver typing/validation on streamed rows
   ▼
SQL Incremental Staging      — bulk-load cleaned rows into stg_fact_transactions
   ▼
SQL MERGE                    — MERGE staging INTO the warehouse fact ON transaction_id
   ▼                            (insert new, skip existing -> idempotent under replay)
dbt Incremental Models       — materialized='incremental', unique_key='transaction_id'
   ▼
Power BI Desktop Refresh     — incremental refresh picks up only the new Gold rows
```

Idempotency (no double-counting if Kafka replays an event) is enforced
downstream by **SQL MERGE on `transaction_id`** and **dbt's incremental
`unique_key`** — which is exactly why at-least-once delivery here is safe.
