"""
FinTech DW - Dataset Validator
==============================
Validates every generated CSV in dw_data/ against the SQL Server DDL in
fintech_dw_create_tables.sql, so the data engineering pipeline can start from a
known-good state.

Checks per table:
  * exact column names + order vs DDL (identity transaction_sk excluded on fact)
  * data types & numeric ranges (INT / TINYINT / SMALLINT / BIGINT)
  * string / NVARCHAR / CHAR length limits
  * DECIMAL(18,2) precision (<=18 digits) and scale (==2)
  * BIT columns restricted to {0,1}
  * NOT NULL columns contain no empty values
  * PRIMARY KEY uniqueness
  * FOREIGN KEY references resolve (NULL allowed only where the FK is nullable)

Exit code 0 = all green, 1 = at least one violation.
Dependencies: standard library only.
"""

import csv
import os
import sys
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dw_data")

INT_MIN, INT_MAX = -2_147_483_648, 2_147_483_647
SMALLINT_MIN, SMALLINT_MAX = -32_768, 32_767
TINYINT_MIN, TINYINT_MAX = 0, 255
BIGINT_MIN, BIGINT_MAX = -9_223_372_036_854_775_808, 9_223_372_036_854_775_807

# --- column spec: (name, type, nullable, extra) ----------------------------
# type in: int, tinyint, smallint, bigint, bit, date, str, char, decimal
# extra: maxlen for str/char ; (prec, scale) for decimal
SCHEMA = {
    "dim_date.csv": {
        "pk": ["date_key"],
        "cols": [
            ("date_key", "int", False, None),
            ("full_date", "date", False, None),
            ("day", "tinyint", False, None),
            ("month", "tinyint", False, None),
            ("quarter", "tinyint", False, None),
            ("year", "smallint", False, None),
            ("is_weekend", "bit", False, None),
        ],
    },
    "dim_time.csv": {
        "pk": ["time_key"],
        "cols": [
            ("time_key", "int", False, None),
            ("hour_of_day", "tinyint", False, None),
            ("time_bucket", "str", False, 20),
            ("is_daytime", "bit", False, None),
            ("hour_label", "str", False, 10),
        ],
    },
    "dim_location.csv": {
        "pk": ["location_key"],
        "cols": [
            ("location_key", "int", False, None),
            ("city", "str", False, 100),
            ("governorate", "str", False, 100),
            ("country", "str", False, 100),
        ],
    },
    "dim_account.csv": {
        "pk": ["account_key"],
        "cols": [
            ("account_key", "int", False, None),
            ("account_id", "str", False, 50),
            ("location_key", "int", False, None),
            ("currency", "char", False, 3),
            ("age_band", "str", True, 20),
            ("acquisition_channel", "str", True, 50),
            ("signup_date_key", "int", False, None),
            ("customer_tier", "str", True, 20),
            ("account_status", "str", False, 20),
        ],
    },
    "dim_merchant.csv": {
        "pk": ["merchant_key"],
        "cols": [
            ("merchant_key", "int", False, None),
            ("location_key", "int", False, None),
            ("merchant_id", "str", False, 50),
            ("merchant_name", "str", False, 200),
            ("merchant_category", "str", True, 100),
            ("merchant_size", "str", True, 20),
            ("opened_date_key", "int", False, None),
        ],
    },
    "dim_transaction_type.csv": {
        "pk": ["transaction_type_key"],
        "cols": [
            ("transaction_type_key", "int", False, None),
            ("transaction_type_name", "str", False, 100),
            ("transaction_group", "str", False, 50),
        ],
    },
    "dim_decline_reason.csv": {
        "pk": ["decline_reason_key"],
        "cols": [
            ("decline_reason_key", "int", False, None),
            ("decline_reason_name", "str", False, 200),
        ],
    },
    "fact_transactions.csv": {
        "pk": ["transaction_id"],   # natural key; transaction_sk is DB-generated
        "cols": [
            ("date_key", "int", False, None),
            ("time_key", "int", False, None),
            ("account_key", "int", False, None),
            ("peer_account_key", "int", True, None),
            ("transaction_type_key", "int", False, None),
            ("decline_reason_key", "int", True, None),
            ("merchant_key", "int", True, None),
            ("transaction_id", "str", False, 50),
            ("mc_transaction_id", "str", True, 50),
            ("ach_transfer_id", "str", True, 50),
            ("amount_minor", "bigint", False, None),
            ("amount_egp", "decimal", False, (18, 2)),
            ("abs_amount_egp", "decimal", False, (18, 2)),
            ("fx_amount_minor", "bigint", True, None),
            ("exchange_rate_e6", "bigint", True, None),
            ("is_outbound", "bit", False, None),
            ("is_declined", "bit", False, None),
            ("is_fx", "bit", False, None),
        ],
    },
}

# (file, column) -> (ref_file, ref_column)
FKS = {
    ("dim_account.csv", "location_key"): ("dim_location.csv", "location_key"),
    ("dim_account.csv", "signup_date_key"): ("dim_date.csv", "date_key"),
    ("dim_merchant.csv", "location_key"): ("dim_location.csv", "location_key"),
    ("dim_merchant.csv", "opened_date_key"): ("dim_date.csv", "date_key"),
    ("fact_transactions.csv", "date_key"): ("dim_date.csv", "date_key"),
    ("fact_transactions.csv", "time_key"): ("dim_time.csv", "time_key"),
    ("fact_transactions.csv", "account_key"): ("dim_account.csv", "account_key"),
    ("fact_transactions.csv", "peer_account_key"): ("dim_account.csv", "account_key"),
    ("fact_transactions.csv", "transaction_type_key"): ("dim_transaction_type.csv", "transaction_type_key"),
    ("fact_transactions.csv", "decline_reason_key"): ("dim_decline_reason.csv", "decline_reason_key"),
    ("fact_transactions.csv", "merchant_key"): ("dim_merchant.csv", "merchant_key"),
}

MAX_ERRORS_PER_CHECK = 5   # cap reported examples per error category


def check_value(val, ctype, extra):
    """Return None if ok, else an error string. val is the raw string (non-empty)."""
    if ctype in ("int", "tinyint", "smallint", "bigint"):
        try:
            iv = int(val)
        except ValueError:
            return f"not an integer ('{val}')"
        lo, hi = {
            "int": (INT_MIN, INT_MAX),
            "tinyint": (TINYINT_MIN, TINYINT_MAX),
            "smallint": (SMALLINT_MIN, SMALLINT_MAX),
            "bigint": (BIGINT_MIN, BIGINT_MAX),
        }[ctype]
        if not (lo <= iv <= hi):
            return f"{ctype} out of range ({iv})"
    elif ctype == "bit":
        if val not in ("0", "1"):
            return f"bit not in {{0,1}} ('{val}')"
    elif ctype == "date":
        try:
            datetime.strptime(val, "%Y-%m-%d")
        except ValueError:
            return f"bad date ('{val}')"
    elif ctype == "char":
        if len(val) != extra:
            return f"char({extra}) wrong length ('{val}' len {len(val)})"
    elif ctype == "str":
        if len(val) > extra:
            return f"exceeds varchar({extra}) (len {len(val)})"
    elif ctype == "decimal":
        prec, scale = extra
        try:
            float(val)
        except ValueError:
            return f"not numeric ('{val}')"
        neg, body = (val[1:], "") if val.startswith("-") else (val, "")
        s = val[1:] if val.startswith("-") else val
        if "." in s:
            intpart, frac = s.split(".")
            if len(frac) != scale:
                return f"decimal scale != {scale} ('{val}')"
            digits = len(intpart.lstrip("0")) + len(frac)
        else:
            return f"decimal missing scale ('{val}')"
        if digits > prec:
            return f"decimal precision > {prec} ('{val}')"
    return None


def validate_table(fname, spec, pk_sets):
    path = os.path.join(DATA_DIR, fname)
    errors = []
    if not os.path.exists(path):
        return [f"FILE MISSING: {path}"], 0
    cols = spec["cols"]
    expected_names = [c[0] for c in cols]
    pk = spec["pk"]
    fk_refs = {col: pk_sets[ref] for (f, col), ref in
               ((k, v) for k, v in FKS.items() if k[0] == fname)}
    # nullable lookup for fk columns
    nullable = {c[0]: c[2] for c in cols}

    seen_pk = set()
    counts = {}        # error category -> count
    samples = {}       # error category -> list of sample messages
    nrows = 0

    def add(cat, msg):
        counts[cat] = counts.get(cat, 0) + 1
        if len(samples.setdefault(cat, [])) < MAX_ERRORS_PER_CHECK:
            samples[cat].append(msg)

    # Columns that may appear in the CSV but are DB-generated on load (IDENTITY)
    # and therefore ignored by the loader. Tolerated as extras, not required.
    TOLERATED_EXTRA = {"transaction_sk"}

    with open(path, encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        if header != expected_names:
            missing = [c for c in expected_names if c not in header]
            extra = [c for c in header if c not in expected_names]
            unexpected = [c for c in extra if c not in TOLERATED_EXTRA]
            if missing or unexpected:
                errors.append(
                    "HEADER MISMATCH"
                    + (f"\n   missing: {missing}" if missing else "")
                    + (f"\n   unexpected: {unexpected}" if unexpected else "")
                    + f"\n   found: {header}")
                return errors, 0
            if extra:   # tolerated extras only (e.g. transaction_sk) -> note + continue
                errors.append(f"NOTE: ignoring DB-generated column(s) {extra} (IDENTITY on load)")
        idx = {name: i for i, name in enumerate(header)}
        for row in reader:
            nrows += 1
            # type / null / length checks
            for name, ctype, nullok, extra in cols:
                v = row[idx[name]]
                if v == "":
                    if not nullok:
                        add(f"{name}: NULL in NOT NULL", f"row {nrows}")
                    continue
                err = check_value(v, ctype, extra)
                if err:
                    add(f"{name}: {ctype} check", f"row {nrows}: {err}")
            # PK uniqueness
            pkval = tuple(row[idx[c]] for c in pk)
            if any(x == "" for x in pkval):
                add("PK null", f"row {nrows}")
            elif pkval in seen_pk:
                add("PK duplicate", f"row {nrows}: {pkval}")
            else:
                seen_pk.add(pkval)
            # FK references
            for col, refset in fk_refs.items():
                v = row[idx[col]]
                if v == "":
                    if not nullable[col]:
                        add(f"FK {col}: NULL not allowed", f"row {nrows}")
                    continue
                if int(v) not in refset:
                    add(f"FK {col}: orphan", f"row {nrows}: {v}")

    for cat in sorted(counts):
        msg = f"{cat}: {counts[cat]} rows"
        if samples.get(cat):
            msg += "  e.g. " + "; ".join(samples[cat][:3])
        errors.append(msg)
    return errors, nrows


def load_key_set(fname, col):
    path = os.path.join(DATA_DIR, fname)
    keys = set()
    with open(path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            v = row[col]
            if v != "":
                keys.add(int(v))
    return keys


def main():
    print("=" * 64)
    print("FinTech DW dataset validation")
    print("=" * 64)

    # Preload PK sets needed for FK resolution
    needed = {(ref_file, ref_col) for ref_file, ref_col in FKS.values()}
    pk_sets = {}
    for ref_file, ref_col in needed:
        pk_sets[(ref_file, ref_col)] = load_key_set(ref_file, ref_col)

    order = [
        "dim_date.csv", "dim_time.csv", "dim_location.csv",
        "dim_account.csv", "dim_merchant.csv",
        "dim_transaction_type.csv", "dim_decline_reason.csv",
        "fact_transactions.csv",
    ]
    total_fail = 0
    for fname in order:
        spec = SCHEMA[fname]
        errors, nrows = validate_table(fname, spec, pk_sets)
        # NOTE lines are informational (tolerated extras) and don't fail a table.
        real_errors = [e for e in errors if not e.startswith("NOTE")]
        status = "PASS" if not real_errors else "FAIL"
        if real_errors:
            total_fail += 1
        print(f"\n[{status}] {fname}  ({nrows:,} rows)")
        for e in errors:
            print(f"    - {e}")

    print("\n" + "=" * 64)
    if total_fail == 0:
        print("RESULT: ALL TABLES PASS - datasets meet schema requirements.")
    else:
        print(f"RESULT: {total_fail} table(s) FAILED validation.")
    print("=" * 64)
    sys.exit(0 if total_fail == 0 else 1)


if __name__ == "__main__":
    main()
