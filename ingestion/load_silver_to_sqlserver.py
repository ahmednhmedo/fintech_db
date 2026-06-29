"""
SILVER -> WAREHOUSE load (HOST side, Windows auth).

Reads the PySpark silver Parquet and lands it into fintech_db.silver.* so the
gold load (sql/load_gold.sql) can INSERT it into the existing DDL star tables.

Runs on the Windows host (not in Docker) because it uses the local
"ODBC Driver 18 for SQL Server" + Windows (Trusted) auth to ahmed\\SQLEXPRESS.

Env (all optional; defaults = local Windows auth):
  WH_SERVER   default  ahmed\\SQLEXPRESS
  WH_DATABASE default  fintech_db
  WH_DRIVER   default  ODBC Driver 18 for SQL Server
  WH_USER / WH_PASSWORD  -> if set, SQL auth; else Trusted_Connection
  LAKE_DIR    default  <repo>/data/lake

Run:  python ingestion/load_silver_to_sqlserver.py
"""
import os
from pathlib import Path

import pyarrow.dataset as ds
import pyodbc

REPO = Path(__file__).resolve().parents[1]
LAKE = os.environ.get("LAKE_DIR", str(REPO / "data" / "lake"))
SILVER = Path(LAKE) / "silver"
BATCH = 50_000

# Ordered columns + a [silver] DDL per table. Types mirror the gold DDL so the
# downstream INSERT into dbo.* is a clean, implicit-cast-free copy.
TABLES = {
    "dim_date": ("""
        date_key INT, full_date DATE, [day] TINYINT, [month] TINYINT,
        [quarter] TINYINT, [year] SMALLINT, is_weekend BIT""",
        ["date_key", "full_date", "day", "month", "quarter", "year", "is_weekend"]),
    "dim_time": ("""
        time_key INT, hour_of_day TINYINT, time_bucket VARCHAR(20),
        is_daytime BIT, hour_label VARCHAR(10)""",
        ["time_key", "hour_of_day", "time_bucket", "is_daytime", "hour_label"]),
    "dim_location": ("""
        location_key INT, city NVARCHAR(100), governorate NVARCHAR(100),
        country NVARCHAR(100)""",
        ["location_key", "city", "governorate", "country"]),
    "dim_account": ("""
        account_key INT, account_id VARCHAR(50), location_key INT, currency CHAR(3),
        age_band VARCHAR(20), acquisition_channel VARCHAR(50), signup_date_key INT,
        customer_tier VARCHAR(20), account_status VARCHAR(20)""",
        ["account_key", "account_id", "location_key", "currency", "age_band",
         "acquisition_channel", "signup_date_key", "customer_tier", "account_status"]),
    "dim_merchant": ("""
        merchant_key INT, location_key INT, merchant_id VARCHAR(50),
        merchant_name NVARCHAR(200), merchant_category VARCHAR(100),
        merchant_size VARCHAR(20), opened_date_key INT""",
        ["merchant_key", "location_key", "merchant_id", "merchant_name",
         "merchant_category", "merchant_size", "opened_date_key"]),
    "dim_transaction_type": ("""
        transaction_type_key INT, transaction_type_name VARCHAR(100),
        transaction_group VARCHAR(50)""",
        ["transaction_type_key", "transaction_type_name", "transaction_group"]),
    "dim_decline_reason": ("""
        decline_reason_key INT, decline_reason_name VARCHAR(200)""",
        ["decline_reason_key", "decline_reason_name"]),
    "fact_transactions": ("""
        date_key INT, time_key INT, account_key INT, peer_account_key INT,
        transaction_type_key INT, decline_reason_key INT, merchant_key INT,
        transaction_id VARCHAR(50), mc_transaction_id VARCHAR(50),
        ach_transfer_id VARCHAR(50), amount_minor BIGINT,
        amount_egp DECIMAL(18,2), abs_amount_egp DECIMAL(18,2),
        fx_amount_minor BIGINT, exchange_rate_e6 BIGINT,
        is_outbound BIT, is_declined BIT, is_fx BIT""",
        ["date_key", "time_key", "account_key", "peer_account_key",
         "transaction_type_key", "decline_reason_key", "merchant_key",
         "transaction_id", "mc_transaction_id", "ach_transfer_id", "amount_minor",
         "amount_egp", "abs_amount_egp", "fx_amount_minor", "exchange_rate_e6",
         "is_outbound", "is_declined", "is_fx"]),
}


def connect():
    driver = os.environ.get("WH_DRIVER", "ODBC Driver 18 for SQL Server")
    server = os.environ.get("WH_SERVER", r"ahmed\SQLEXPRESS")
    database = os.environ.get("WH_DATABASE", "fintech_db")
    parts = [f"DRIVER={{{driver}}}", f"SERVER={server}", f"DATABASE={database}",
             "Encrypt=yes", "TrustServerCertificate=yes"]
    user, pwd = os.environ.get("WH_USER"), os.environ.get("WH_PASSWORD")
    parts += [f"UID={user}", f"PWD={pwd}"] if (user and pwd) else ["Trusted_Connection=yes"]
    return pyodbc.connect(";".join(parts) + ";", autocommit=False)


def load_table(cur, table, ddl, cols):
    src = SILVER / table
    if not src.exists():
        raise FileNotFoundError(f"silver parquet not found: {src}")
    cur.execute("IF SCHEMA_ID('silver') IS NULL EXEC('CREATE SCHEMA silver');")
    cur.execute(f"IF OBJECT_ID('silver.{table}','U') IS NOT NULL DROP TABLE silver.{table};")
    cur.execute(f"CREATE TABLE silver.{table} ({ddl});")
    placeholders = ",".join("?" * len(cols))
    collist = ",".join(f"[{c}]" for c in cols)
    insert = f"INSERT INTO silver.{table} ({collist}) VALUES ({placeholders})"
    cur.fast_executemany = True
    total = 0
    for batch in ds.dataset(str(src), format="parquet").to_batches(batch_size=BATCH):
        recs = batch.to_pylist()  # native python types + None for nulls
        rows = [[r[c] for c in cols] for r in recs]
        if rows:
            cur.executemany(insert, rows)
            total += len(rows)
            print(f"  silver.{table}: {total:,} rows", flush=True)
    return total


def main():
    conn = connect()
    cur = conn.cursor()
    for table, (ddl, cols) in TABLES.items():
        n = load_table(cur, table, " ".join(ddl.split()), cols)
        conn.commit()
        print(f"loaded silver.{table}: {n:,} rows")
    cur.close()
    conn.close()
    print("silver load complete.")


if __name__ == "__main__":
    main()
