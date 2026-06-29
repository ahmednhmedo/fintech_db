/* ============================================================
   GOLD load: silver.* -> dbo.* (your existing DDL star tables).
   Assumes star FOREIGN KEYs were dropped first (drop_star_constraints.sql).
   Idempotent: truncates then reloads. IDENTITY_INSERT for the surrogate-key dims;
   fact_transactions excludes transaction_sk (DB-generated IDENTITY).

   Run:  sqlcmd -S ahmed\SQLEXPRESS -E -C -d fintech_db -i sql\load_gold.sql
   ============================================================ */
USE fintech_db;
SET NOCOUNT ON;
-- Required to INSERT into tables that carry FILTERED INDEXES (fact_transactions has
-- WHERE is_declined=1 / WHERE is_fx=1). sqlcmd defaults QUOTED_IDENTIFIER OFF.
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
-- Atomicity: any error aborts the batch and rolls back the whole transaction,
-- so a failure never leaves partial Gold state (all-or-nothing reload).
SET XACT_ABORT ON;
GO

BEGIN TRANSACTION;

-- 1) Clear (FKs are dropped, so order is unconstrained) --------------------
TRUNCATE TABLE dbo.fact_transactions;
TRUNCATE TABLE dbo.dim_account;
TRUNCATE TABLE dbo.dim_merchant;
TRUNCATE TABLE dbo.dim_transaction_type;
TRUNCATE TABLE dbo.dim_decline_reason;
TRUNCATE TABLE dbo.dim_location;
TRUNCATE TABLE dbo.dim_time;
TRUNCATE TABLE dbo.dim_date;

-- 2) Dimensions (FK parents first) ----------------------------------------
INSERT INTO dbo.dim_date (date_key, full_date, [day], [month], [quarter], [year], is_weekend)
SELECT date_key, full_date, [day], [month], [quarter], [year], is_weekend FROM silver.dim_date;

INSERT INTO dbo.dim_time (time_key, hour_of_day, time_bucket, is_daytime, hour_label)
SELECT time_key, hour_of_day, time_bucket, is_daytime, hour_label FROM silver.dim_time;

SET IDENTITY_INSERT dbo.dim_location ON;
INSERT INTO dbo.dim_location (location_key, city, governorate, country)
SELECT location_key, city, governorate, country FROM silver.dim_location;
SET IDENTITY_INSERT dbo.dim_location OFF;

SET IDENTITY_INSERT dbo.dim_transaction_type ON;
INSERT INTO dbo.dim_transaction_type (transaction_type_key, transaction_type_name, transaction_group)
SELECT transaction_type_key, transaction_type_name, transaction_group FROM silver.dim_transaction_type;
SET IDENTITY_INSERT dbo.dim_transaction_type OFF;

SET IDENTITY_INSERT dbo.dim_decline_reason ON;
INSERT INTO dbo.dim_decline_reason (decline_reason_key, decline_reason_name)
SELECT decline_reason_key, decline_reason_name FROM silver.dim_decline_reason;
SET IDENTITY_INSERT dbo.dim_decline_reason OFF;

SET IDENTITY_INSERT dbo.dim_account ON;
INSERT INTO dbo.dim_account (account_key, account_id, location_key, currency, age_band,
                            acquisition_channel, signup_date_key, customer_tier, account_status)
SELECT account_key, account_id, location_key, currency, age_band,
       acquisition_channel, signup_date_key, customer_tier, account_status FROM silver.dim_account;
SET IDENTITY_INSERT dbo.dim_account OFF;

SET IDENTITY_INSERT dbo.dim_merchant ON;
INSERT INTO dbo.dim_merchant (merchant_key, location_key, merchant_id, merchant_name,
                             merchant_category, merchant_size, opened_date_key)
SELECT merchant_key, location_key, merchant_id, merchant_name,
       merchant_category, merchant_size, opened_date_key FROM silver.dim_merchant;
SET IDENTITY_INSERT dbo.dim_merchant OFF;

-- 3) Fact (transaction_sk is IDENTITY -> not inserted) ---------------------
INSERT INTO dbo.fact_transactions (
    date_key, time_key, account_key, peer_account_key, transaction_type_key,
    decline_reason_key, merchant_key, transaction_id, mc_transaction_id, ach_transfer_id,
    amount_minor, amount_egp, abs_amount_egp, fx_amount_minor, exchange_rate_e6,
    is_outbound, is_declined, is_fx)
SELECT
    date_key, time_key, account_key, peer_account_key, transaction_type_key,
    decline_reason_key, merchant_key, transaction_id, mc_transaction_id, ach_transfer_id,
    amount_minor, amount_egp, abs_amount_egp, fx_amount_minor, exchange_rate_e6,
    is_outbound, is_declined, is_fx
FROM silver.fact_transactions;

COMMIT TRANSACTION;
GO

PRINT 'gold load complete.';
SELECT COUNT(*) AS fact_rows, CAST(AVG(abs_amount_egp) AS DECIMAL(10,2)) AS avg_ticket_egp
FROM dbo.fact_transactions;
GO
