-- ============================================================
-- FinTech Data Warehouse - SQL Server DDL
-- ============================================================

-- ------------------------------------------------------------
-- dim_date
-- ------------------------------------------------------------
CREATE TABLE dim_date (
    date_key        INT             NOT NULL,   -- Format: YYYYMMDD (e.g. 20240115)
    full_date       DATE            NOT NULL,
    day             TINYINT         NOT NULL,
    month           TINYINT         NOT NULL,
    quarter         TINYINT         NOT NULL,
    year            SMALLINT        NOT NULL,
    is_weekend      BIT             NOT NULL DEFAULT 0,

    CONSTRAINT PK_dim_date PRIMARY KEY (date_key)
);

-- ------------------------------------------------------------
-- dim_time
-- ------------------------------------------------------------
CREATE TABLE dim_time (
    time_key        INT             NOT NULL,   -- Format: HHMM (e.g. 1430)
    hour_of_day     TINYINT         NOT NULL,
    time_bucket     VARCHAR(20)     NOT NULL,   -- e.g. 'Morning', 'Afternoon'
    is_daytime      BIT             NOT NULL DEFAULT 1,
    hour_label      VARCHAR(10)     NOT NULL,   -- e.g. '02:00 PM'

    CONSTRAINT PK_dim_time PRIMARY KEY (time_key)
);

-- ------------------------------------------------------------
-- dim_location
-- ------------------------------------------------------------
CREATE TABLE dim_location (
    location_key    INT             NOT NULL IDENTITY(1,1),
    city            NVARCHAR(100)   NOT NULL,
    governorate     NVARCHAR(100)   NOT NULL,
    country         NVARCHAR(100)   NOT NULL,

    CONSTRAINT PK_dim_location PRIMARY KEY (location_key)
);

-- ------------------------------------------------------------
-- dim_account
-- ------------------------------------------------------------
CREATE TABLE dim_account (
    account_key         INT             NOT NULL IDENTITY(1,1),
    account_id          VARCHAR(50)     NOT NULL,   -- Business/natural key
    location_key        INT             NOT NULL,
    currency            CHAR(3)         NOT NULL,   -- ISO 4217 e.g. 'EGP'
    age_band            VARCHAR(20)     NULL,        -- e.g. '25-34' -- NOTE: consider SCD Type 2 if this changes
    acquisition_channel VARCHAR(50)     NULL,
    signup_date_key     INT             NOT NULL,
    customer_tier       VARCHAR(20)     NULL,        -- e.g. 'Standard', 'Premium' -- NOTE: consider SCD Type 2
    account_status      VARCHAR(20)     NOT NULL,    -- e.g. 'Active', 'Suspended'

    CONSTRAINT PK_dim_account          PRIMARY KEY (account_key),
    CONSTRAINT FK_dim_account_location FOREIGN KEY (location_key)    REFERENCES dim_location(location_key),
    CONSTRAINT FK_dim_account_date     FOREIGN KEY (signup_date_key) REFERENCES dim_date(date_key)
);

-- ------------------------------------------------------------
-- dim_merchant
-- ------------------------------------------------------------
CREATE TABLE dim_merchant (
    merchant_key        INT             NOT NULL IDENTITY(1,1),
    location_key        INT             NOT NULL,
    merchant_id         VARCHAR(50)     NOT NULL,   -- Business/natural key
    merchant_name       NVARCHAR(200)   NOT NULL,
    merchant_category   VARCHAR(100)    NULL,        -- e.g. 'Grocery', 'Electronics'
    merchant_size       VARCHAR(20)     NULL,        -- e.g. 'SME', 'Enterprise' -- NOTE: consider SCD Type 2
    opened_date_key     INT             NOT NULL,

    CONSTRAINT PK_dim_merchant          PRIMARY KEY (merchant_key),
    CONSTRAINT FK_dim_merchant_location FOREIGN KEY (location_key)    REFERENCES dim_location(location_key),
    CONSTRAINT FK_dim_merchant_date     FOREIGN KEY (opened_date_key) REFERENCES dim_date(date_key)
);

-- ------------------------------------------------------------
-- dim_transaction_type
-- ------------------------------------------------------------
CREATE TABLE dim_transaction_type (
    transaction_type_key    INT             NOT NULL IDENTITY(1,1),
    transaction_type_name   VARCHAR(100)    NOT NULL,   -- e.g. 'Card Payment', 'ACH Transfer'
    transaction_group       VARCHAR(50)     NOT NULL,   -- e.g. 'Payment', 'Transfer', 'Withdrawal'

    CONSTRAINT PK_dim_transaction_type PRIMARY KEY (transaction_type_key)
);

-- ------------------------------------------------------------
-- dim_decline_reason
-- ------------------------------------------------------------
CREATE TABLE dim_decline_reason (
    decline_reason_key      INT             NOT NULL IDENTITY(1,1),
    decline_reason_name     VARCHAR(200)    NOT NULL,   -- e.g. 'Insufficient Funds', 'Card Expired'

    CONSTRAINT PK_dim_decline_reason PRIMARY KEY (decline_reason_key)
);

-- ------------------------------------------------------------
-- fact_transactions
-- ------------------------------------------------------------
CREATE TABLE fact_transactions (
    transaction_sk          BIGINT          NOT NULL IDENTITY(1,1),

    -- Dimension foreign keys
    date_key                INT             NOT NULL,
    time_key                INT             NOT NULL,
    account_key             INT             NOT NULL,
    peer_account_key        INT             NULL,        -- NULL for non-P2P transactions
    transaction_type_key    INT             NOT NULL,
    decline_reason_key      INT             NULL,        -- NULL for successful transactions
    merchant_key            INT             NULL,        -- NULL for non-merchant transactions

    -- Natural/business keys (for traceability)
    transaction_id          VARCHAR(50)     NOT NULL,
    mc_transaction_id       VARCHAR(50)     NULL,        -- Mastercard transaction ID
    ach_transfer_id         VARCHAR(50)     NULL,        -- ACH transfer ID

    -- Measures
    amount_minor            BIGINT          NOT NULL,    -- Amount in minor currency units (e.g. piastres)
    amount_egp              DECIMAL(18,2)   NOT NULL,    -- Amount in EGP
    abs_amount_egp          DECIMAL(18,2)   NOT NULL,    -- Absolute value (always positive)
    fx_amount_minor         BIGINT          NULL,        -- Foreign currency amount in minor units
    exchange_rate_e6        BIGINT          NULL,        -- Exchange rate × 1,000,000 (avoids floats)

    -- Flags
    is_outbound             BIT             NOT NULL DEFAULT 0,
    is_declined             BIT             NOT NULL DEFAULT 0,
    is_fx                   BIT             NOT NULL DEFAULT 0,

    CONSTRAINT PK_fact_transactions             PRIMARY KEY (transaction_sk),
    CONSTRAINT FK_fact_date                     FOREIGN KEY (date_key)             REFERENCES dim_date(date_key),
    CONSTRAINT FK_fact_time                     FOREIGN KEY (time_key)             REFERENCES dim_time(time_key),
    CONSTRAINT FK_fact_account                  FOREIGN KEY (account_key)          REFERENCES dim_account(account_key),
    CONSTRAINT FK_fact_peer_account             FOREIGN KEY (peer_account_key)     REFERENCES dim_account(account_key),
    CONSTRAINT FK_fact_transaction_type         FOREIGN KEY (transaction_type_key) REFERENCES dim_transaction_type(transaction_type_key),
    CONSTRAINT FK_fact_decline_reason           FOREIGN KEY (decline_reason_key)   REFERENCES dim_decline_reason(decline_reason_key),
    CONSTRAINT FK_fact_merchant                 FOREIGN KEY (merchant_key)         REFERENCES dim_merchant(merchant_key)
);

-- ------------------------------------------------------------
-- Recommended indexes for common query patterns
-- ------------------------------------------------------------

-- Filter by date range (most common warehouse query pattern)
CREATE NONCLUSTERED INDEX IX_fact_transactions_date
    ON fact_transactions (date_key)
    INCLUDE (amount_egp, is_declined, is_fx);

-- Filter by account (account activity lookups)
CREATE NONCLUSTERED INDEX IX_fact_transactions_account
    ON fact_transactions (account_key)
    INCLUDE (date_key, amount_egp, is_outbound);

-- Filter declined transactions
CREATE NONCLUSTERED INDEX IX_fact_transactions_declined
    ON fact_transactions (is_declined, decline_reason_key)
    WHERE is_declined = 1;

-- Filter FX transactions
CREATE NONCLUSTERED INDEX IX_fact_transactions_fx
    ON fact_transactions (is_fx)
    WHERE is_fx = 1;

-- Natural key lookup (idempotent loads / deduplication)
CREATE UNIQUE NONCLUSTERED INDEX UX_fact_transactions_natural_key
    ON fact_transactions (transaction_id);
