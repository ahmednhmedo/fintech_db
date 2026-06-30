/* ============================================================================
   merge_incremental_fact.sql
   Stored procedure that loads ONE staged Silver batch into the warehouse fact.

   Pipeline inside the proc (all-or-nothing per batch):
       stg.fact_transactions_stage  (one batch, loaded by Python)
            -> validate  -> stg.fact_transactions_reject   (bad rows quarantined)
            -> MERGE      -> dbo.fact_transactions          (idempotent upsert)
            -> audit      -> stg.incremental_load_log       (SUCCESS / FAILED)

   Business key: transaction_id (UNIQUE index UX_fact_transactions_natural_key).
   Re-running the same batch is a no-op: MATCHED rows are only updated when a
   value actually differs, so identical replays touch zero warehouse rows.

   CREATE OR ALTER => safe to re-run.
   ============================================================================ */
-- These must be ON when the proc is CREATED: it does DML against
-- stg.incremental_load_log, which carries a FILTERED unique index, and the
-- proc captures these options at creation time. They sit in their OWN batch
-- (terminated by GO) because CREATE PROCEDURE must be the first statement in a
-- batch. pyodbc defaults them ON; sqlcmd defaults QUOTED_IDENTIFIER OFF, so set
-- them explicitly to make this script create the proc correctly from ANY client.
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

CREATE OR ALTER PROCEDURE stg.usp_merge_incremental_fact
    @silver_batch_id VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;
    -- XACT_ABORT ON: on ANY runtime error the whole transaction is aborted and
    -- rolled back, and locks are released. Essential here because one batch runs
    -- several statements in a single transaction -- without it, a mid-batch error
    -- could leave the transaction open and partially applied. ON = all-or-nothing.
    SET XACT_ABORT ON;

    DECLARE @started DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @rows_staged   INT = 0,
            @rows_rejected  INT = 0,
            @rows_valid     INT = 0,
            @rows_inserted  INT = 0,
            @rows_updated   INT = 0;

    BEGIN TRY
        -- ----------------------------------------------------------------
        -- Snapshot this batch into a temp table with (a) a dedupe rank and
        -- (b) a reject_reason. Dedupe keeps the LATEST Silver row per
        -- transaction_id; this is also what makes the MERGE legal -- MERGE
        -- errors if its source has >1 row for the same target key.
        -- ----------------------------------------------------------------
        IF OBJECT_ID('tempdb..#batch') IS NOT NULL DROP TABLE #batch;

        SELECT s.*,
               rn = ROW_NUMBER() OVER (PARTITION BY s.transaction_id
                                       ORDER BY s._silver_processed_at_utc DESC),
               CAST(NULL AS VARCHAR(100)) AS reject_reason
        INTO #batch
        FROM stg.fact_transactions_stage s
        WHERE s.silver_batch_id = @silver_batch_id;

        SELECT @rows_staged = COUNT(*) FROM #batch;

        -- ----------------------------------------------------------------
        -- Validation. First failing rule wins. FK rules resolve the
        -- (already-conformed) surrogate keys against the dbo dimensions, so a
        -- fact that points at a missing dimension is quarantined here instead
        -- of aborting the MERGE with a foreign-key violation.
        -- ----------------------------------------------------------------
        UPDATE b SET reject_reason =
            CASE
              WHEN b.transaction_id IS NULL OR LTRIM(RTRIM(b.transaction_id)) = ''
                   THEN 'null_transaction_id'
              WHEN b.rn > 1
                   THEN 'duplicate_transaction_id_in_staging'
              WHEN b.date_key IS NULL OR b.time_key IS NULL OR b.account_key IS NULL
                   OR b.transaction_type_key IS NULL
                   THEN 'missing_required_key'
              WHEN b.amount_minor IS NULL OR b.amount_egp IS NULL OR b.abs_amount_egp IS NULL
                   THEN 'null_measure'
              WHEN NOT EXISTS (SELECT 1 FROM dbo.dim_date d WHERE d.date_key = b.date_key)
                   THEN 'fk_date_not_found'
              WHEN NOT EXISTS (SELECT 1 FROM dbo.dim_time t WHERE t.time_key = b.time_key)
                   THEN 'fk_time_not_found'
              WHEN NOT EXISTS (SELECT 1 FROM dbo.dim_account a WHERE a.account_key = b.account_key)
                   THEN 'fk_account_not_found'
              WHEN b.peer_account_key IS NOT NULL
                   AND NOT EXISTS (SELECT 1 FROM dbo.dim_account a WHERE a.account_key = b.peer_account_key)
                   THEN 'fk_peer_account_not_found'
              WHEN NOT EXISTS (SELECT 1 FROM dbo.dim_transaction_type tt WHERE tt.transaction_type_key = b.transaction_type_key)
                   THEN 'fk_txn_type_not_found'
              WHEN b.decline_reason_key IS NOT NULL
                   AND NOT EXISTS (SELECT 1 FROM dbo.dim_decline_reason dr WHERE dr.decline_reason_key = b.decline_reason_key)
                   THEN 'fk_decline_reason_not_found'
              WHEN b.merchant_key IS NOT NULL
                   AND NOT EXISTS (SELECT 1 FROM dbo.dim_merchant m WHERE m.merchant_key = b.merchant_key)
                   THEN 'fk_merchant_not_found'
              ELSE NULL
            END
        FROM #batch b;

        BEGIN TRANSACTION;

        -- 1) Quarantine invalid rows (auditable; never silently dropped) --------
        INSERT INTO stg.fact_transactions_reject (
            date_key, time_key, account_key, peer_account_key, transaction_type_key,
            decline_reason_key, merchant_key, transaction_id, mc_transaction_id, ach_transfer_id,
            amount_minor, amount_egp, abs_amount_egp, fx_amount_minor, exchange_rate_e6,
            is_outbound, is_declined, is_fx, silver_batch_id, _silver_processed_at_utc, reject_reason)
        SELECT
            date_key, time_key, account_key, peer_account_key, transaction_type_key,
            decline_reason_key, merchant_key, transaction_id, mc_transaction_id, ach_transfer_id,
            amount_minor, amount_egp, abs_amount_egp, fx_amount_minor, exchange_rate_e6,
            is_outbound, is_declined, is_fx, silver_batch_id, _silver_processed_at_utc, reject_reason
        FROM #batch
        WHERE reject_reason IS NOT NULL;
        SET @rows_rejected = @@ROWCOUNT;

        SELECT @rows_valid = COUNT(*) FROM #batch WHERE reject_reason IS NULL;

        -- 2) Idempotent UPSERT into the warehouse fact on transaction_id --------
        --    HOLDLOCK (serializable) closes the classic MERGE race between the
        --    existence check and the INSERT under concurrency.
        DECLARE @actions TABLE (act NVARCHAR(10));

        MERGE dbo.fact_transactions WITH (HOLDLOCK) AS tgt
        USING (
            SELECT
                date_key, time_key, account_key, peer_account_key, transaction_type_key,
                decline_reason_key, merchant_key, transaction_id, mc_transaction_id, ach_transfer_id,
                amount_minor, amount_egp, abs_amount_egp, fx_amount_minor, exchange_rate_e6,
                is_outbound, is_declined, is_fx
            FROM #batch
            WHERE reject_reason IS NULL
        ) AS src
            ON tgt.transaction_id = src.transaction_id

        -- Existing transaction: UPDATE only if at least one column actually
        -- differs (NULL-safe). Transactions are largely immutable, so a pure
        -- at-least-once replay matches with identical values and updates NOTHING
        -- (no churn). The update path exists for genuine late corrections.
        WHEN MATCHED AND (
               NULLIF(tgt.date_key, src.date_key) IS NOT NULL OR NULLIF(src.date_key, tgt.date_key) IS NOT NULL
            OR NULLIF(tgt.time_key, src.time_key) IS NOT NULL OR NULLIF(src.time_key, tgt.time_key) IS NOT NULL
            OR NULLIF(tgt.account_key, src.account_key) IS NOT NULL OR NULLIF(src.account_key, tgt.account_key) IS NOT NULL
            OR NULLIF(tgt.peer_account_key, src.peer_account_key) IS NOT NULL OR NULLIF(src.peer_account_key, tgt.peer_account_key) IS NOT NULL
            OR NULLIF(tgt.transaction_type_key, src.transaction_type_key) IS NOT NULL OR NULLIF(src.transaction_type_key, tgt.transaction_type_key) IS NOT NULL
            OR NULLIF(tgt.decline_reason_key, src.decline_reason_key) IS NOT NULL OR NULLIF(src.decline_reason_key, tgt.decline_reason_key) IS NOT NULL
            OR NULLIF(tgt.merchant_key, src.merchant_key) IS NOT NULL OR NULLIF(src.merchant_key, tgt.merchant_key) IS NOT NULL
            OR NULLIF(tgt.mc_transaction_id, src.mc_transaction_id) IS NOT NULL OR NULLIF(src.mc_transaction_id, tgt.mc_transaction_id) IS NOT NULL
            OR NULLIF(tgt.ach_transfer_id, src.ach_transfer_id) IS NOT NULL OR NULLIF(src.ach_transfer_id, tgt.ach_transfer_id) IS NOT NULL
            OR NULLIF(tgt.amount_minor, src.amount_minor) IS NOT NULL OR NULLIF(src.amount_minor, tgt.amount_minor) IS NOT NULL
            OR NULLIF(tgt.amount_egp, src.amount_egp) IS NOT NULL OR NULLIF(src.amount_egp, tgt.amount_egp) IS NOT NULL
            OR NULLIF(tgt.abs_amount_egp, src.abs_amount_egp) IS NOT NULL OR NULLIF(src.abs_amount_egp, tgt.abs_amount_egp) IS NOT NULL
            OR NULLIF(tgt.fx_amount_minor, src.fx_amount_minor) IS NOT NULL OR NULLIF(src.fx_amount_minor, tgt.fx_amount_minor) IS NOT NULL
            OR NULLIF(tgt.exchange_rate_e6, src.exchange_rate_e6) IS NOT NULL OR NULLIF(src.exchange_rate_e6, tgt.exchange_rate_e6) IS NOT NULL
            OR NULLIF(tgt.is_outbound, src.is_outbound) IS NOT NULL OR NULLIF(src.is_outbound, tgt.is_outbound) IS NOT NULL
            OR NULLIF(tgt.is_declined, src.is_declined) IS NOT NULL OR NULLIF(src.is_declined, tgt.is_declined) IS NOT NULL
            OR NULLIF(tgt.is_fx, src.is_fx) IS NOT NULL OR NULLIF(src.is_fx, tgt.is_fx) IS NOT NULL
        ) THEN UPDATE SET
            tgt.date_key             = src.date_key,
            tgt.time_key             = src.time_key,
            tgt.account_key          = src.account_key,
            tgt.peer_account_key     = src.peer_account_key,
            tgt.transaction_type_key = src.transaction_type_key,
            tgt.decline_reason_key   = src.decline_reason_key,
            tgt.merchant_key         = src.merchant_key,
            tgt.mc_transaction_id    = src.mc_transaction_id,
            tgt.ach_transfer_id      = src.ach_transfer_id,
            tgt.amount_minor         = src.amount_minor,
            tgt.amount_egp           = src.amount_egp,
            tgt.abs_amount_egp       = src.abs_amount_egp,
            tgt.fx_amount_minor      = src.fx_amount_minor,
            tgt.exchange_rate_e6     = src.exchange_rate_e6,
            tgt.is_outbound          = src.is_outbound,
            tgt.is_declined          = src.is_declined,
            tgt.is_fx                = src.is_fx

        -- New transaction: INSERT. transaction_sk is the DB IDENTITY -> omitted.
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (date_key, time_key, account_key, peer_account_key, transaction_type_key,
                    decline_reason_key, merchant_key, transaction_id, mc_transaction_id, ach_transfer_id,
                    amount_minor, amount_egp, abs_amount_egp, fx_amount_minor, exchange_rate_e6,
                    is_outbound, is_declined, is_fx)
            VALUES (src.date_key, src.time_key, src.account_key, src.peer_account_key, src.transaction_type_key,
                    src.decline_reason_key, src.merchant_key, src.transaction_id, src.mc_transaction_id, src.ach_transfer_id,
                    src.amount_minor, src.amount_egp, src.abs_amount_egp, src.fx_amount_minor, src.exchange_rate_e6,
                    src.is_outbound, src.is_declined, src.is_fx)

        -- NOTE: deliberately NO "WHEN NOT MATCHED BY SOURCE" clause -- we must
        -- never delete warehouse rows that are simply absent from this batch.
        OUTPUT $action INTO @actions;

        SELECT @rows_inserted = ISNULL(SUM(CASE WHEN act = 'INSERT' THEN 1 ELSE 0 END), 0),
               @rows_updated  = ISNULL(SUM(CASE WHEN act = 'UPDATE' THEN 1 ELSE 0 END), 0)
        FROM @actions;

        -- 3) Record SUCCESS atomically with the MERGE (same transaction) --------
        INSERT INTO stg.incremental_load_log (
            silver_batch_id, status, rows_staged, rows_valid, rows_inserted,
            rows_updated, rows_rejected, started_at_utc, finished_at_utc, duration_ms)
        VALUES (
            @silver_batch_id, 'SUCCESS', @rows_staged, @rows_valid, @rows_inserted,
            @rows_updated, @rows_rejected, @started, SYSUTCDATETIME(),
            DATEDIFF(MILLISECOND, @started, SYSUTCDATETIME()));

        COMMIT TRANSACTION;

        -- Return a one-row summary so the Python loader can log the outcome.
        SELECT @silver_batch_id AS silver_batch_id, @rows_staged AS rows_staged,
               @rows_valid AS rows_valid, @rows_inserted AS rows_inserted,
               @rows_updated AS rows_updated, @rows_rejected AS rows_rejected;
    END TRY
    BEGIN CATCH
        -- Roll back any partial work, then record the failure in its OWN
        -- (auto-committed) statement so the audit trail survives the rollback.
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;

        INSERT INTO stg.incremental_load_log (
            silver_batch_id, status, started_at_utc, finished_at_utc, duration_ms, error_message)
        VALUES (
            @silver_batch_id, 'FAILED', @started, SYSUTCDATETIME(),
            DATEDIFF(MILLISECOND, @started, SYSUTCDATETIME()), ERROR_MESSAGE());

        THROW;   -- re-raise so the loader sees the failure
    END CATCH
END
