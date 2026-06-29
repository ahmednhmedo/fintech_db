/* Drop the star FOREIGN KEYs so the gold tables can be TRUNCATEd and reloaded.
   PKs / indexes stay. Re-added afterwards by add_star_constraints.sql.
   Run:  sqlcmd -S ahmed\SQLEXPRESS -E -C -d fintech_db -i sql\drop_star_constraints.sql */
USE fintech_db;
GO
ALTER TABLE dbo.fact_transactions DROP CONSTRAINT IF EXISTS FK_fact_date;
ALTER TABLE dbo.fact_transactions DROP CONSTRAINT IF EXISTS FK_fact_time;
ALTER TABLE dbo.fact_transactions DROP CONSTRAINT IF EXISTS FK_fact_account;
ALTER TABLE dbo.fact_transactions DROP CONSTRAINT IF EXISTS FK_fact_peer_account;
ALTER TABLE dbo.fact_transactions DROP CONSTRAINT IF EXISTS FK_fact_transaction_type;
ALTER TABLE dbo.fact_transactions DROP CONSTRAINT IF EXISTS FK_fact_decline_reason;
ALTER TABLE dbo.fact_transactions DROP CONSTRAINT IF EXISTS FK_fact_merchant;
ALTER TABLE dbo.dim_account  DROP CONSTRAINT IF EXISTS FK_dim_account_location;
ALTER TABLE dbo.dim_account  DROP CONSTRAINT IF EXISTS FK_dim_account_date;
ALTER TABLE dbo.dim_merchant DROP CONSTRAINT IF EXISTS FK_dim_merchant_location;
ALTER TABLE dbo.dim_merchant DROP CONSTRAINT IF EXISTS FK_dim_merchant_date;
GO
PRINT 'star foreign keys dropped.';
GO
