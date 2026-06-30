# dbt — Final Validation, Documentation & Reporting (Gold)

Local **SQL Server / SQL Server Express** only. No Azure, Fabric, cloud, or Power BI Service.

## dbt's role in this architecture

The warehouse (`fintech_db.dbo.*` star schema) is **written** by two loaders:

* **Initial batch** — `sql/load_gold.sql` (full historical load), and
* **Incremental** — the SQL `MERGE` in `incremental/sql/merge_incremental_fact.sql`
  (`stg.usp_merge_incremental_fact`), driven by `load_silver_incremental_to_sql.py`.

**dbt does not load or MERGE anything.** It sits on top as the **validation,
documentation, and reporting layer**:

1. **Validate** the warehouse is still correct after each incremental load
   (uniqueness, not-null, no orphan FKs, value domains, row-count/measure sanity).
2. **Document** the Gold star (sources + generated docs site).
3. **Report** — thin reporting **views** for Power BI Desktop.

```
Initial Batch  ─┐
                ├─►  dbo.* star (fact + dims)  ──►  dbt: validate + document + reporting views
Incremental MERGE ┘        (gold, loaded by SQL)        (this stage — read-only on the star)
```

The star tables are exposed to dbt as **sources** (`models/_sources.yml`); dbt's
only *writes* are the reporting views in the `[reporting]` schema.

## What this stage adds (only where needed)

The schema tests requested were **already present** in `_sources.yml` and were left
unchanged:
* `fact_transactions.transaction_id` — `unique` + `not_null`
* **No orphan FKs** — `relationships` from every fact FK to its dimension
  (date, time, account, peer_account, transaction_type, decline_reason, merchant)
* **Accepted values** — `is_outbound` / `is_declined` / `is_fx` ∈ {0,1}; plus
  status domains on `account_status`, `customer_tier`, `merchant_size`, `time_bucket`.

New in this stage:

| File | Purpose |
|---|---|
| `tests/assert_fact_transactions_not_empty.sql` | **Row-count sanity** — fact must not be empty after a load (catches an accidental truncate). |
| `tests/assert_abs_amount_egp_nonnegative.sql` | Measure sanity — `abs_amount_egp >= 0`. |
| `tests/assert_abs_amount_egp_matches_amount.sql` | Measure integrity — `abs_amount_egp = ABS(amount_egp)`. |
| `tests/assert_declined_have_decline_reason.sql` | Business-rule sanity (severity **warn**) — a declined txn should have a `decline_reason_key`. |
| `models/reporting/rpt_warehouse_health.sql` | One-row health/reconciliation **view**: total vs distinct txns, declined/fx/outbound counts, date span, gross volume. Aggregate-only — **no MERGE logic duplicated**. |
| `models/reporting/_reporting.yml` | Docs (+ light not_null tests) for both reporting views. |

`total_transactions == distinct_transaction_ids` in the health view is the at-a-glance
"no duplicate facts" check after each incremental load.

## Commands (run from `dbt/fintech/`)

```powershell
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\dbt\fintech"

dbt debug            # verify profile 'fintech_db' + SQL Server connection
dbt run              # build the reporting views in [reporting]
dbt test             # run all data tests (schema + singular)
dbt build            # run + test together (recommended single command)
dbt docs generate    # build the documentation site (target/)
dbt docs serve       # (optional) open the docs locally at http://localhost:8080
```

Prereqs: dbt-core 1.8 + dbt-sqlserver (installed in the host Python 3.11 venv, per
`requirements.txt`), and the `fintech_db` profile in `%USERPROFILE%\.dbt\profiles.yml`
(see `profiles.yml.example`). The warehouse must already be loaded (batch and/or
incremental) — dbt validates it, it does not populate it.

## Final validation output checklist

Validated locally against `fintech_db` (1,000,000 fact rows):

- [x] `dbt debug` → **All checks passed** (connection OK).
- [x] `dbt build` → **PASS=47, WARN=0, ERROR=0, SKIP=0**.
- [x] `transaction_id` unique + not_null → PASS.
- [x] No orphan FKs (7 `relationships` tests) → PASS.
- [x] Accepted values (`is_outbound`/`is_declined`/`is_fx` ∈ {0,1}, status domains) → PASS.
- [x] Row-count sanity (`assert_fact_transactions_not_empty`) → PASS.
- [x] Measure sanity (`abs_amount_egp >= 0`, `= ABS(amount_egp)`) → PASS.
- [x] Declined-have-reason (warn) → PASS (0 violations).
- [x] Reporting views created: `reporting.rpt_warehouse_health`, `reporting.rpt_daily_transactions`.
- [x] Health view reconciliation: `total_transactions (1,000,000) == distinct_transaction_ids (1,000,000)` → no duplicate facts.
- [x] `dbt docs generate` → `target/manifest.json`, `target/catalog.json`, `target/index.html` written.

## Re-validate after an incremental load

```powershell
# 1) run the incremental SQL load (separate stage)
python ..\..\incremental\load_silver_incremental_to_sql.py
# 2) re-validate + refresh reporting views + docs
cd "f:\NTI FOLDER\NTI INCREMENTAL\fintech-lakehouse\dbt\fintech"
dbt build
dbt docs generate
```
A green `dbt build` after the MERGE is the final gate confirming the warehouse
remains correct (unique keys, intact FKs, valid domains, sane measures) before
Power BI Desktop refreshes off the `[reporting]` views.

> **Optional extension (not built):** to surface the incremental *load audit* in
> the reporting layer, expose `stg.incremental_load_log` as a dbt source and add a
> view summarizing rows inserted/updated/rejected per `silver_batch_id`. It was
> left out of the default build so `dbt run` never hard-depends on the `stg`
> schema existing (the gold validation must work even on a fresh warehouse).
