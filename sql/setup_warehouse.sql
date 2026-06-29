/* ============================================================
   One-time warehouse setup for the local SQL Server target.
   The fintech_db database already exists (created in SSMS). This just adds the
   medallion warehouse-side schema:

     bronze : raw Parquet in the lake (NOT in SQL Server)
     silver : SQL Server schema [silver]  (loaded from PySpark by the host loader)
     gold   : the dbo.* star from sql/01_create_star.sql (your DDL, with IDENTITY/FK)

   Run (Windows auth):
     sqlcmd -S ahmed\SQLEXPRESS -E -C -d fintech_db -i sql\setup_warehouse.sql
   ============================================================ */
USE fintech_db;
GO

IF SCHEMA_ID(N'silver') IS NULL EXEC(N'CREATE SCHEMA silver');   -- ODS: conformed detail (from Spark)
GO

PRINT 'fintech_db ready: schema [silver] present; gold = dbo.* star (run 01_create_star.sql if missing).';
GO
