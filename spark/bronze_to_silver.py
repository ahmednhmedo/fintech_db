"""
SILVER layer (PySpark owns this). The source is already in star shape, so silver
is a CONFORMING pass, not an integration pass:
  * cast every column to its warehouse type (per fintech_dw_create_tables.sql)
  * trim strings and turn '' into NULL  (so the warehouse gets real NULLs)
  * dedupe each table on its primary/natural key (idempotency)

Reads  bronze/<table>  ->  writes silver/<table>  (Parquet).

Run:  spark-submit bronze_to_silver.py
"""
import os
from pyspark.sql import SparkSession, functions as F, types as T

LAKE = os.environ.get("LAKE_DIR", "/opt/data/lake")
BRONZE = f"{LAKE}/bronze"
SILVER = f"{LAKE}/silver"

# table -> { column: spark_type }  (order preserved -> drives select)
INT, LONG, STR = "int", "long", "string"
DEC = "decimal(18,2)"
DATE = "date"

SCHEMAS = {
    "dim_date": {
        "date_key": INT, "full_date": DATE, "day": INT, "month": INT,
        "quarter": INT, "year": INT, "is_weekend": INT,
    },
    "dim_time": {
        "time_key": INT, "hour_of_day": INT, "time_bucket": STR,
        "is_daytime": INT, "hour_label": STR,
    },
    "dim_location": {
        "location_key": INT, "city": STR, "governorate": STR, "country": STR,
    },
    "dim_account": {
        "account_key": INT, "account_id": STR, "location_key": INT,
        "currency": STR, "age_band": STR, "acquisition_channel": STR,
        "signup_date_key": INT, "customer_tier": STR, "account_status": STR,
    },
    "dim_merchant": {
        "merchant_key": INT, "location_key": INT, "merchant_id": STR,
        "merchant_name": STR, "merchant_category": STR, "merchant_size": STR,
        "opened_date_key": INT,
    },
    "dim_transaction_type": {
        "transaction_type_key": INT, "transaction_type_name": STR,
        "transaction_group": STR,
    },
    "dim_decline_reason": {
        "decline_reason_key": INT, "decline_reason_name": STR,
    },
    "fact_transactions": {
        "date_key": INT, "time_key": INT, "account_key": INT,
        "peer_account_key": INT, "transaction_type_key": INT,
        "decline_reason_key": INT, "merchant_key": INT,
        "transaction_id": STR, "mc_transaction_id": STR, "ach_transfer_id": STR,
        "amount_minor": LONG, "amount_egp": DEC, "abs_amount_egp": DEC,
        "fx_amount_minor": LONG, "exchange_rate_e6": LONG,
        "is_outbound": INT, "is_declined": INT, "is_fx": INT,
    },
}

# Natural / primary key used for dedupe per table.
DEDUPE_KEY = {
    "dim_date": "date_key", "dim_time": "time_key", "dim_location": "location_key",
    "dim_account": "account_key", "dim_merchant": "merchant_key",
    "dim_transaction_type": "transaction_type_key",
    "dim_decline_reason": "decline_reason_key",
    "fact_transactions": "transaction_id",
}


def conform(df, schema):
    cols = []
    for col, typ in schema.items():
        c = F.col(col)
        if typ == STR:
            c = F.trim(c)
            c = F.when(c == "", None).otherwise(c)         # '' -> NULL
        else:
            c = F.when(F.trim(c) == "", None).otherwise(c)  # empty -> NULL before cast
            c = c.cast(typ)
        cols.append(c.alias(col))
    # Explicit projection: selects ONLY contracted warehouse columns, so any extra
    # Bronze lineage columns (_ingested_at_utc, _source_file) never reach Silver.
    return df.select(*cols)


def main(spark):
    os.makedirs(SILVER, exist_ok=True)
    for table, schema in SCHEMAS.items():
        df = spark.read.parquet(f"{BRONZE}/{table}")
        df = conform(df, schema)
        df = df.dropDuplicates([DEDUPE_KEY[table]])   # idempotent dedupe
        (df.write.mode("overwrite").parquet(f"{SILVER}/{table}"))
        print(f"silver/{table}: {df.count():,} rows", flush=True)


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("bronze_to_silver")
             .config("spark.sql.shuffle.partitions", "64")
             .getOrCreate())
    main(spark)
    spark.stop()
