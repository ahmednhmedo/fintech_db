# Power BI — Readiness, Build Guide & Verification

How to connect Power BI Desktop to the Gold star in SQL Server, build the model and
dashboard, and **verify it's working** against known-good benchmarks.

> Source of truth: `fintech_db` Gold star (`dbo.*`) + `reporting.rpt_daily_transactions`.
> The data pipeline is frozen — do **not** change the Gold schema, PKs/FKs/SKs, relationships,
> grain, or Power BI-facing tables.

---

## 1. Connect (SQL Server, Import mode, Windows auth)

1. Power BI Desktop → **Get Data → SQL Server**.
2. **Server:** `ahmed\SQLEXPRESS`  ·  **Database:** `fintech_db`.
3. **Data Connectivity mode:** **Import** (recommended for 1M rows; faster interactivity than DirectQuery).
4. **Authentication:** Windows (use your current credentials).
5. In the Navigator, select the objects in section 2, then **Load**.

---

## 2. Tables / views to import

| Object | Role |
|--------|------|
| `dbo.dim_date` | Date dimension → **Mark as Date Table** |
| `dbo.dim_time` | Time-of-day dimension |
| `dbo.dim_location` | Geography |
| `dbo.dim_account` | Customer / account |
| `dbo.dim_merchant` | Merchant |
| `dbo.dim_transaction_type` | Transaction type / group |
| `dbo.dim_decline_reason` | Decline reasons |
| `dbo.fact_transactions` | **Fact** (1,000,000 rows) |
| `reporting.rpt_daily_transactions` | **Optional** — pre-aggregated daily summary (see note below) |

**Do NOT import:** `dbo.sysdiagrams`, any `silver.*` staging table.

### Is `rpt_daily_transactions` required? No — it's optional.
It's a dbt-built convenience view (one row per day, 731 rows) that pre-computes
`txn_count`, `declined_count`, `fx_count`, `gross_volume_egp`, `avg_ticket_egp`. Everything
it provides is already derivable from `fact_transactions` + `dim_date` via the DAX measures
in section 6. At 1M rows in **Import mode** the pre-aggregation gives **no meaningful
performance benefit**.

- **Skip it (cleaner, recommended):** single source of truth from the fact. Build the daily
  trend with a line chart → Axis `dim_date[full_date]`, Values `[Gross Volume (EGP)]`.
- **Import it:** handy if you want a one-drag daily trend (use its columns directly) or to
  visibly demonstrate the dbt reporting layer. If you do, keep it **standalone** — don't mix
  its columns with fact measures in the same visual (double-count risk), and its total
  `gross_volume_egp` must equal **689,181,271** (else it's stale → Refresh).

---

## 3. Two modeling realities to handle

1. **`dim_account` is role-playing.** `fact.account_key` (actor) **and** `fact.peer_account_key`
   (P2P counterparty) both reference `dim_account`. Power BI allows only **one active**
   relationship → keep `account_key` **active**, `peer_account_key` **inactive**, and surface
   peer analysis with `USERELATIONSHIP` (see the P2P measure).
2. **`dim_date` & `dim_location` are reused (outriggers).** `dim_account`/`dim_merchant`
   reference `dim_date` (signup/opened) and `dim_location`. Make `fact.date_key → dim_date`
   the **single active** date path; set `signup_date_key`/`opened_date_key → dim_date`
   **inactive** so a date slicer filters *transactions*, not signups/openings.
   `dim_location` is shared by accounts and merchants — a location slicer filters both; if you
   need them separate, duplicate it into **Customer Location** / **Merchant Location** role tables.

---

## 4. Relationships (the 11 FKs)

| Relationship | Cardinality | Active? | Filter dir |
|--------------|-------------|---------|-----------|
| fact[date_key] → dim_date[date_key] | *:1 | **Active** | Single |
| fact[time_key] → dim_time[time_key] | *:1 | Active | Single |
| fact[account_key] → dim_account[account_key] | *:1 | **Active** | Single |
| fact[peer_account_key] → dim_account[account_key] | *:1 | **Inactive** (USERELATIONSHIP) | Single |
| fact[transaction_type_key] → dim_transaction_type | *:1 | Active | Single |
| fact[decline_reason_key] → dim_decline_reason | *:1 | Active (nullable → Blank) | Single |
| fact[merchant_key] → dim_merchant | *:1 | Active (nullable → Blank) | Single |
| dim_account[location_key] → dim_location | *:1 | Active (customer geo) | Single |
| dim_merchant[location_key] → dim_location | *:1 | Active (merchant geo) | Single |
| dim_account[signup_date_key] → dim_date | *:1 | **Inactive (recommended)** | Single |
| dim_merchant[opened_date_key] → dim_date | *:1 | **Inactive (recommended)** | Single |

---

## 5. Model View checklist (verify after import)

- [ ] **Cardinality:** every fact→dim is **Many-to-One (\*:1)**; outriggers too. No many-to-many.
- [ ] **Active relationships:** exactly one active to `dim_account` (`account_key`); `peer_account_key` **inactive**. One active date path `fact.date_key → dim_date`; `signup_date_key`/`opened_date_key → dim_date` **inactive**.
- [ ] **Filter direction:** **Single** (dim → fact) everywhere; avoid bidirectional unless justified.
- [ ] **Date table:** select `dim_date` → **Table tools → Mark as Date Table** → use `full_date` (continuous 2023-01-01 → 2024-12-31, 731 rows).
- [ ] **Layout:** `fact_transactions` central; 7 dims around it; `dim_location`/`dim_date` behind account/merchant; `rpt_daily_transactions` standalone (optionally relate to `dim_date[full_date]`, single direction).
- [ ] **Hygiene:** nullable FKs (peer/decline/merchant) show a **(Blank)** member (rows not dropped); hide raw `*_key` columns, `amount_minor`, `exchange_rate_e6` from report view (use measures).

---

## 6. DAX measures (create a `_Measures` table, then paste)

> BIT columns import as **True/False** — filter with `= TRUE()`, not `= 1`.

```DAX
-- Core
Transaction Count    = COUNTROWS ( fact_transactions )
Gross Volume (EGP)   = SUM ( fact_transactions[abs_amount_egp] )
Net Flow (EGP)       = SUM ( fact_transactions[amount_egp] )
Average Ticket (EGP) = DIVIDE ( [Gross Volume (EGP)], [Transaction Count] )

-- Declines
Declined Transactions = CALCULATE ( [Transaction Count], fact_transactions[is_declined] = TRUE() )
Approved Transactions = [Transaction Count] - [Declined Transactions]
Decline Rate %        = DIVIDE ( [Declined Transactions], [Transaction Count] )

-- FX
FX Transactions = CALCULATE ( [Transaction Count], fact_transactions[is_fx] = TRUE() )
FX Share %      = DIVIDE ( [FX Transactions], [Transaction Count] )
FX Volume (EGP) = CALCULATE ( [Gross Volume (EGP)], fact_transactions[is_fx] = TRUE() )
Avg Exchange Rate = AVERAGE ( fact_transactions[exchange_rate_e6] ) / 1000000   -- non-additive; FX context only

-- Direction
Outbound Volume (EGP) = CALCULATE ( [Gross Volume (EGP)], fact_transactions[is_outbound] = TRUE() )
Inbound Volume (EGP)  = CALCULATE ( [Gross Volume (EGP)], fact_transactions[is_outbound] = FALSE() )

-- Accounts / merchants
Active Accounts          = DISTINCTCOUNT ( fact_transactions[account_key] )
Distinct Merchants       = DISTINCTCOUNT ( fact_transactions[merchant_key] )
Avg Transactions/Account = DIVIDE ( [Transaction Count], [Active Accounts] )

-- P2P via the INACTIVE peer relationship (role-playing)
P2P Volume to Peer (EGP) =
CALCULATE (
    [Gross Volume (EGP)],
    USERELATIONSHIP ( fact_transactions[peer_account_key], dim_account[account_key] )
)

-- Time intelligence (requires dim_date marked as Date Table on full_date)
Volume YTD        = TOTALYTD ( [Gross Volume (EGP)], dim_date[full_date] )
Volume MTD        = TOTALMTD ( [Gross Volume (EGP)], dim_date[full_date] )
Volume Last Month = CALCULATE ( [Gross Volume (EGP)], DATEADD ( dim_date[full_date], -1, MONTH ) )
MoM Growth %      = DIVIDE ( [Gross Volume (EGP)] - [Volume Last Month], [Volume Last Month] )
```

---

## 7. Dashboard pages

| Page | KPI cards | Visuals | Slicers |
|------|-----------|---------|---------|
| **1. Executive Overview** | Gross Volume, Transaction Count, Average Ticket, Decline Rate %, FX Share %, Active Accounts | Daily volume line (fact by `full_date` with `[Gross Volume (EGP)]`; or the optional `rpt_daily_transactions`); volume by `transaction_group` (donut); volume by `governorate` (map/bar); top `merchant_category` (bar) | Date range, `customer_tier`, `governorate` |
| **2. Transactions & Channels** | Transaction Count, Approved vs Declined, Outbound/Inbound Volume | By `transaction_type`/`group` (bar); approved vs declined (clustered column); time-of-day by `dim_time[hour_of_day]`/`time_bucket` (column/heatmap) | `transaction_group`, `is_declined`, `time_bucket` |
| **3. Geography** | Gross Volume, Active Accounts | Volume/count by `governorate` → `city` (map + drill bar); customer-location vs merchant-location | `governorate`, `city` |
| **4. Merchants** | Distinct Merchants, Gross Volume | Top merchants by volume; by `merchant_category`; `merchant_size` mix; decline rate by category | `merchant_category`, `merchant_size` |
| **5. Customers / Accounts** | Active Accounts, Avg Transactions/Account | By `customer_tier`, `age_band`, `account_status`, `acquisition_channel`; avg txns/account | `customer_tier`, `age_band`, `account_status`, `acquisition_channel` |
| **6. Risk & FX** | Decline Rate %, FX Share %, FX Volume | Decline rate trend; declines by `decline_reason` (bar); decline rate by type/location; FX volume; Avg Exchange Rate | `decline_reason`, `is_fx` |

---

## 8. ✅ Verification benchmarks — "is it working?"

After loading and adding the measures, drop these on a blank page and confirm the numbers
**exactly match** (the data is frozen, so they're deterministic):

| Measure / check | Expected value |
|-----------------|----------------|
| Transaction Count | **1,000,000** |
| Gross Volume (EGP) | **≈ 689,181,271** |
| Net Flow (EGP) | **≈ -392,965,266** (negative: more outbound than inbound) |
| Average Ticket (EGP) | **689.18** |
| Declined Transactions | **42,478** |
| Decline Rate % | **4.25%** |
| FX Transactions | **43,783** |
| FX Share % | **4.38%** |
| Active Accounts (distinct in fact) | **39,795** (of 40,000 — 205 accounts had no transactions) |
| Distinct Merchants | **1,200** |
| P2P transactions (peer_account_key not blank) | **154,667** |

**Row-count sanity (Model/Data view):**
- `fact_transactions` = 1,000,000 · `dim_account` = 40,000 · `dim_merchant` = 1,200 ·
  `dim_date` = 731 · `dim_time` = 1,440 · `dim_location` = 19 ·
  `dim_transaction_type` = 13 · `dim_decline_reason` = 8.

**Distribution sanity (should reflect the design):**
- `governorate` volume concentrated in **Cairo + Giza + Alexandria (~62%)**.
- A `time_bucket` column chart should peak at **Afternoon/Evening**.
- `customer_tier` ≈ Standard 70% / Premium 22% / Business 8%.

If your cards match the table above, the model, relationships, and measures are wired correctly. ✅

---

## 9. Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| Decline/FX measures error or return blank | BIT imported as Boolean — use `= TRUE()` not `= 1`. |
| Time-intelligence measures (YTD/MTD/MoM) blank | `dim_date` not **Marked as Date Table** on `full_date`. |
| Date slicer also filters accounts/merchants | Deactivate `dim_account[signup_date_key]` / `dim_merchant[opened_date_key]` → `dim_date`. |
| "Ambiguous"/inactive relationship to `dim_account` | Expected — `peer_account_key` must be **inactive**; use the P2P `USERELATIONSHIP` measure. |
| Location slicer mixes customer & merchant geo | Duplicate `dim_location` into Customer/Merchant role tables. |
| Numbers don't match section 8 | Re-run the pipeline (`.\run_all.ps1`) and **Refresh** in Power BI; confirm you're on `fintech_db` (not `fintech_wh`). |
| Can't connect | Confirm SQL Server is running, server = `ahmed\SQLEXPRESS`, Windows auth, and `fintech_db` exists. |

---

*Connection: server `ahmed\SQLEXPRESS`, database `fintech_db`, Import mode, Windows auth. Old `fintech_wh` project is unrelated — do not point Power BI at it.*
