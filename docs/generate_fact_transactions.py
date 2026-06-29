"""
FinTech Data Warehouse - Fact + remaining dimensions generator
==============================================================
Consumes the already-generated dimension CSVs:
    dim_date.csv, dim_location.csv, dim_account.csv, dim_merchant.csv

Generates (in dependency order):
    1. dim_time.csv             (supplied HHMM key, 1440 rows)
    2. dim_transaction_type.csv (IDENTITY key, supplied here)
    3. dim_decline_reason.csv   (IDENTITY key, supplied here)
    4. fact_transactions.csv    (1,000,000 rows; transaction_sk OMITTED - it is
                                 an IDENTITY column the database assigns on load)

Amount model (per request)
--------------------------
The dominant retail transaction types (Card / POS / Online payments, ~54% of
volume) draw amounts from a NORMAL distribution centred on typical Egyptian
spend (~550 EGP, std ~225) so that roughly 95% of those amounts fall in the
100-1000 EGP band and the tails are unlikely. A few special types provide the
realistic low tail (Mobile Recharge ~60 EGP) and high tail (Salary ~9000,
Wire ~5000), which stay rare.

NULL handling for the fact CSV: NULL foreign keys / ids are written as EMPTY
fields. Load with BULK INSERT ... WITH (KEEPNULLS) (and a column list that skips
transaction_sk), or stage into a table without the IDENTITY column first.

Dependencies: Python 3.8+, numpy.
"""

import csv
import os
import numpy as np

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
SEED = 42
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dw_data")
NUM_TXNS = 1_000_000

# Normal-distribution centre/spread for typical Egyptian retail spend (EGP).
SPEND_MEAN = 550.0
SPEND_STD = 225.0
MIN_AMOUNT = 5.0          # floor so the normal's left tail never goes <= 0

# Approx EGP exchange rates for FX transactions (EGP per 1 foreign unit).
FX_RATES = {"USD": 48.0, "EUR": 52.0, "SAR": 12.8, "AED": 13.1}

CHUNK = 100_000           # rows per CSV write flush

rng = np.random.default_rng(SEED)

# ------------------------------------------------------------------
# dim_transaction_type: (name, group, weight, outbound, channel,
#                        is_card, is_ach, fx_eligible, decline_rate,
#                        amt_mean, amt_std, merchant_pool)
# channel: 'merchant' | 'peer' | 'atm' | 'deposit'
# ------------------------------------------------------------------
RETAIL = ["Grocery", "Restaurants", "Fuel", "Pharmacy", "Electronics", "Fashion"]
ONLINE = ["E-commerce", "Electronics", "Fashion"]
TELECOM = ["Telecom"]

TXN_TYPES = [
    # name,                    group,        wt,  outb,  channel,    card,  ach,   fx,    decl,  mean,   std,  pool
    ("Card Payment",          "Payment",    22.0, True,  "merchant", True,  False, True,  0.05,  550.0,  225.0, RETAIL),
    ("POS Purchase",          "Payment",    18.0, True,  "merchant", True,  False, False, 0.04,  500.0,  220.0, RETAIL),
    ("Online Payment",        "Payment",    14.0, True,  "merchant", True,  False, True,  0.08,  650.0,  260.0, ONLINE),
    ("Mobile Recharge",       "Payment",     8.0, True,  "merchant", False, False, False, 0.03,   60.0,   30.0, TELECOM),
    ("Bill Payment",          "Payment",     8.0, True,  "merchant", False, False, False, 0.04,  480.0,  200.0, TELECOM),
    ("P2P Transfer Sent",     "Transfer",   10.0, True,  "peer",     False, False, False, 0.04,  600.0,  280.0, None),
    ("P2P Transfer Received", "Transfer",    6.0, False, "peer",     False, False, False, 0.00,  600.0,  280.0, None),
    ("ATM Withdrawal",        "Withdrawal",  7.0, True,  "atm",      False, False, False, 0.03,  700.0,  350.0, None),
    ("ACH Transfer",          "Transfer",    3.0, True,  "ach",      False, True,  False, 0.05,  900.0,  400.0, None),
    ("Wire Transfer",         "Transfer",    1.5, True,  "ach",      False, True,  True,  0.06, 5000.0, 2500.0, None),
    ("Cash Deposit",          "Deposit",     2.5, False, "deposit",  False, False, False, 0.01,  800.0,  400.0, None),
    ("Salary Deposit",        "Deposit",     1.0, False, "deposit",  False, False, False, 0.00, 9000.0, 3500.0, None),
    ("Refund",                "Refund",      2.0, False, "merchant", True,  False, False, 0.00,  400.0,  200.0, RETAIL),
]

DECLINE_REASONS = [
    "Insufficient Funds", "Card Expired", "Incorrect PIN", "Daily Limit Exceeded",
    "Suspected Fraud", "Card Blocked", "Technical Error", "Invalid Merchant",
    "Do Not Honor", "Network Timeout",
]
# How often each reason occurs (relative weights), aligned to DECLINE_REASONS.
DECLINE_WEIGHTS = [34, 8, 9, 11, 6, 5, 8, 4, 10, 5]

# Hour-of-day activity profile (index 0..23) -> relative weight.
HOUR_WEIGHTS = np.array([
    1.0, 0.5, 0.4, 0.3, 0.3, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0,
    7.0, 7.0, 6.0, 5.0, 5.0, 6.0, 7.0, 8.0, 8.0, 7.0, 5.0, 3.0,
])


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def read_csv(name):
    with open(os.path.join(DATA_DIR, name), encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def write_csv(name, header, rows):
    path = os.path.join(DATA_DIR, name)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {len(rows):>9,} rows -> {path}")


# ------------------------------------------------------------------
# 1. dim_time  (every minute of the day: HHMM)
# ------------------------------------------------------------------
def gen_dim_time():
    header = ["time_key", "hour_of_day", "time_bucket", "is_daytime", "hour_label"]
    rows = []
    for h in range(24):
        if 5 <= h <= 11:
            bucket = "Morning"
        elif 12 <= h <= 16:
            bucket = "Afternoon"
        elif 17 <= h <= 20:
            bucket = "Evening"
        else:
            bucket = "Night"
        is_daytime = 1 if 6 <= h <= 18 else 0
        ampm = "AM" if h < 12 else "PM"
        h12 = ((h + 11) % 12) + 1
        for m in range(60):
            time_key = h * 100 + m
            hour_label = f"{h12:02d}:{m:02d} {ampm}"
            rows.append([time_key, h, bucket, is_daytime, hour_label])
    write_csv("dim_time.csv", header, rows)


def gen_dim_transaction_type():
    header = ["transaction_type_key", "transaction_type_name", "transaction_group"]
    rows = [[i + 1, t[0], t[1]] for i, t in enumerate(TXN_TYPES)]
    write_csv("dim_transaction_type.csv", header, rows)


def gen_dim_decline_reason():
    header = ["decline_reason_key", "decline_reason_name"]
    rows = [[i + 1, name] for i, name in enumerate(DECLINE_REASONS)]
    write_csv("dim_decline_reason.csv", header, rows)


# ------------------------------------------------------------------
# 4. fact_transactions
# ------------------------------------------------------------------
def gen_fact_transactions():
    print("  loading dimensions...")
    date_rows = read_csv("dim_date.csv")
    date_keys = np.array([int(r["date_key"]) for r in date_rows], dtype=np.int64)
    date_idx_of = {int(r["date_key"]): i for i, r in enumerate(date_rows)}
    n_dates = len(date_keys)

    acc_rows = read_csv("dim_account.csv")
    acct_key = np.array([int(r["account_key"]) for r in acc_rows], dtype=np.int64)
    signup_idx = np.array([date_idx_of[int(r["signup_date_key"])] for r in acc_rows],
                          dtype=np.int64)
    num_acc = len(acct_key)

    mer_rows = read_csv("dim_merchant.csv")
    pool_by_cat = {}
    for r in mer_rows:
        pool_by_cat.setdefault(r["merchant_category"], []).append(int(r["merchant_key"]))
    # Build a merchant-key pool array per transaction type (None where N/A).
    type_pools = []
    for t in TXN_TYPES:
        cats = t[11]
        if cats is None:
            type_pools.append(None)
        else:
            keys = []
            for c in cats:
                keys.extend(pool_by_cat.get(c, []))
            type_pools.append(np.array(keys, dtype=np.int64))

    N = NUM_TXNS
    print(f"  drawing {N:,} transactions (vectorised)...")

    # --- account assignment: skewed activity (lognormal) so a few power users ---
    act_w = rng.lognormal(mean=0.0, sigma=0.9, size=num_acc)
    act_p = act_w / act_w.sum()
    acct_pos = rng.choice(num_acc, size=N, p=act_p)

    # --- transaction type assignment ---
    type_w = np.array([t[2] for t in TXN_TYPES], dtype=np.float64)
    type_p = type_w / type_w.sum()
    type_pos = rng.choice(len(TXN_TYPES), size=N, p=type_p)

    # Per-type attribute arrays indexed by type_pos
    outbound_arr = np.array([t[3] for t in TXN_TYPES], dtype=bool)[type_pos]
    card_arr = np.array([t[5] for t in TXN_TYPES], dtype=bool)[type_pos]
    ach_arr = np.array([t[6] for t in TXN_TYPES], dtype=bool)[type_pos]
    fxelig_arr = np.array([t[7] for t in TXN_TYPES], dtype=bool)[type_pos]
    declrate_arr = np.array([t[8] for t in TXN_TYPES], dtype=np.float64)[type_pos]
    mean_arr = np.array([t[9] for t in TXN_TYPES], dtype=np.float64)[type_pos]
    std_arr = np.array([t[10] for t in TXN_TYPES], dtype=np.float64)[type_pos]

    # --- date: transaction on/after signup, ramped toward recent (right triangle) ---
    sgn = signup_idx[acct_pos]
    span = (n_dates - 1) - sgn
    u = rng.random(N)
    offset = np.floor(np.sqrt(u) * span).astype(np.int64)   # density rises with recency
    date_idx = sgn + offset
    date_key_arr = date_keys[date_idx]

    # --- time of day ---
    hour_p = HOUR_WEIGHTS / HOUR_WEIGHTS.sum()
    hours = rng.choice(24, size=N, p=hour_p)
    minutes = rng.integers(0, 60, size=N)
    time_key_arr = (hours * 100 + minutes).astype(np.int64)

    # --- amounts: normal core, clamped; ATM rounded to nearest 50 ---
    z = rng.standard_normal(N)
    abs_amt = mean_arr + z * std_arr
    abs_amt = np.maximum(abs_amt, MIN_AMOUNT)
    atm_mask = type_pos == [t[0] for t in TXN_TYPES].index("ATM Withdrawal")
    abs_amt[atm_mask] = np.maximum(np.round(abs_amt[atm_mask] / 50.0) * 50.0, 50.0)
    abs_amt = np.round(abs_amt, 2)

    amount_egp = np.where(outbound_arr, -abs_amt, abs_amt)
    amount_minor = np.rint(amount_egp * 100).astype(np.int64)

    # --- declines (only where rate > 0) ---
    is_declined = rng.random(N) < declrate_arr
    decline_key = np.full(N, -1, dtype=np.int64)
    n_decl = int(is_declined.sum())
    if n_decl:
        dr_p = np.array(DECLINE_WEIGHTS, float) / sum(DECLINE_WEIGHTS)
        decline_key[is_declined] = rng.choice(len(DECLINE_REASONS), size=n_decl, p=dr_p) + 1

    # --- FX (subset of eligible types) ---
    is_fx = fxelig_arr & (rng.random(N) < 0.12)
    fx_amount_minor = np.full(N, -1, dtype=np.int64)
    exch_rate_e6 = np.full(N, -1, dtype=np.int64)
    n_fx = int(is_fx.sum())
    if n_fx:
        cur_names = list(FX_RATES.keys())
        cur_rates = np.array([FX_RATES[c] for c in cur_names], dtype=np.float64)
        pick = rng.integers(0, len(cur_names), size=n_fx)
        rates = cur_rates[pick]
        exch_rate_e6[is_fx] = np.rint(rates * 1_000_000).astype(np.int64)
        fx_amount_minor[is_fx] = np.rint(abs_amt[is_fx] / rates * 100).astype(np.int64)

    # --- merchant & peer assignment (per-type) ---
    merchant_key = np.full(N, -1, dtype=np.int64)
    peer_key = np.full(N, -1, dtype=np.int64)
    for ti, t in enumerate(TXN_TYPES):
        mask = type_pos == ti
        cnt = int(mask.sum())
        if cnt == 0:
            continue
        channel = t[4]
        if channel == "merchant":
            pool = type_pools[ti]
            merchant_key[mask] = pool[rng.integers(0, len(pool), size=cnt)]
        elif channel == "peer":
            a = acct_pos[mask]
            p = rng.integers(0, num_acc, size=cnt)
            coll = p == a
            p[coll] = (p[coll] + 1) % num_acc
            peer_key[mask] = acct_key[p]

    account_key_arr = acct_key[acct_pos]
    ttype_key_arr = (type_pos + 1).astype(np.int64)

    # --- business-key id strings ---
    seq = np.arange(1, N + 1)
    transaction_id = np.char.add("TXN", np.char.zfill(seq.astype(str), 10))
    mc_id = np.full(N, "", dtype=object)
    ach_id = np.full(N, "", dtype=object)
    if card_arr.any():
        idx = np.flatnonzero(card_arr)
        rand12 = rng.integers(0, 10**12, size=idx.size)
        mc_id[idx] = ["MC" + str(v).zfill(12) for v in rand12]
    if ach_arr.any():
        idx = np.flatnonzero(ach_arr)
        rand10 = rng.integers(0, 10**10, size=idx.size)
        ach_id[idx] = ["ACH" + str(v).zfill(10) for v in rand10]

    # --- assemble & write in chunks ---
    print("  writing fact_transactions.csv ...")
    header = [
        "date_key", "time_key", "account_key", "peer_account_key",
        "transaction_type_key", "decline_reason_key", "merchant_key",
        "transaction_id", "mc_transaction_id", "ach_transfer_id",
        "amount_minor", "amount_egp", "abs_amount_egp",
        "fx_amount_minor", "exchange_rate_e6",
        "is_outbound", "is_declined", "is_fx",
    ]

    def blank(v):
        return "" if v == -1 else v

    path = os.path.join(DATA_DIR, "fact_transactions.csv")
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        for start in range(0, N, CHUNK):
            end = min(start + CHUNK, N)
            buf = []
            for i in range(start, end):
                buf.append([
                    int(date_key_arr[i]),
                    int(time_key_arr[i]),
                    int(account_key_arr[i]),
                    blank(int(peer_key[i])),
                    int(ttype_key_arr[i]),
                    blank(int(decline_key[i])),
                    blank(int(merchant_key[i])),
                    transaction_id[i],
                    mc_id[i],
                    ach_id[i],
                    int(amount_minor[i]),
                    f"{amount_egp[i]:.2f}",
                    f"{abs_amt[i]:.2f}",
                    blank(int(fx_amount_minor[i])),
                    blank(int(exch_rate_e6[i])),
                    1 if outbound_arr[i] else 0,
                    1 if is_declined[i] else 0,
                    1 if is_fx[i] else 0,
                ])
            w.writerows(buf)
            print(f"    ...{end:,}/{N:,}")
    print(f"  wrote {N:,} rows -> {path}")

    # quick stats
    in_band = np.mean((abs_amt >= 100) & (abs_amt <= 1000)) * 100
    print("\n  --- amount sanity ---")
    print(f"  mean abs amount : {abs_amt.mean():.2f} EGP   median: {np.median(abs_amt):.2f}")
    print(f"  within 100-1000 : {in_band:.1f}% of all txns")
    print(f"  declined        : {is_declined.mean()*100:.1f}%   fx: {is_fx.mean()*100:.1f}%")
    print(f"  avg txns/account: {N/num_acc:.1f}")


def main():
    print("Generating remaining dimensions + fact_transactions...")
    print(f"Data directory: {DATA_DIR}\n")
    print("[1/4] dim_time")
    gen_dim_time()
    print("[2/4] dim_transaction_type")
    gen_dim_transaction_type()
    print("[3/4] dim_decline_reason")
    gen_dim_decline_reason()
    print("[4/4] fact_transactions")
    gen_fact_transactions()
    print("\nDone. Load order: dim_time, dim_transaction_type, dim_decline_reason, "
          "then fact_transactions.")
    print("Note: omit transaction_sk on load (IDENTITY); use KEEPNULLS so empty "
          "fields become NULL. SET IDENTITY_INSERT ON for dim_transaction_type "
          "and dim_decline_reason.")


if __name__ == "__main__":
    main()
