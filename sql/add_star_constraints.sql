/* Re-add the star FOREIGN KEYs after the gold reload (mirrors sql/01_create_star.sql).
   Restores referential integrity + the SSMS database diagram.
   Run:  sqlcmd -S ahmed\SQLEXPRESS -E -C -d fintech_db -i sql\add_star_constraints.sql */
USE fintech_db;
GO
ALTER TABLE dbo.dim_account  ADD CONSTRAINT FK_dim_account_location  FOREIGN KEY (location_key)    REFERENCES dbo.dim_location(location_key);
ALTER TABLE dbo.dim_account  ADD CONSTRAINT FK_dim_account_date      FOREIGN KEY (signup_date_key) REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.dim_merchant ADD CONSTRAINT FK_dim_merchant_location FOREIGN KEY (location_key)    REFERENCES dbo.dim_location(location_key);
ALTER TABLE dbo.dim_merchant ADD CONSTRAINT FK_dim_merchant_date     FOREIGN KEY (opened_date_key) REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_transactions ADD CONSTRAINT FK_fact_date             FOREIGN KEY (date_key)             REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_transactions ADD CONSTRAINT FK_fact_time             FOREIGN KEY (time_key)             REFERENCES dbo.dim_time(time_key);
ALTER TABLE dbo.fact_transactions ADD CONSTRAINT FK_fact_account          FOREIGN KEY (account_key)          REFERENCES dbo.dim_account(account_key);
ALTER TABLE dbo.fact_transactions ADD CONSTRAINT FK_fact_peer_account     FOREIGN KEY (peer_account_key)     REFERENCES dbo.dim_account(account_key);
ALTER TABLE dbo.fact_transactions ADD CONSTRAINT FK_fact_transaction_type FOREIGN KEY (transaction_type_key) REFERENCES dbo.dim_transaction_type(transaction_type_key);
ALTER TABLE dbo.fact_transactions ADD CONSTRAINT FK_fact_decline_reason   FOREIGN KEY (decline_reason_key)   REFERENCES dbo.dim_decline_reason(decline_reason_key);
ALTER TABLE dbo.fact_transactions ADD CONSTRAINT FK_fact_merchant         FOREIGN KEY (merchant_key)         REFERENCES dbo.dim_merchant(merchant_key);
GO
PRINT 'star foreign keys re-added.';
GO
