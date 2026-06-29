"""
FinTech Data Warehouse - Dimension Data Generator
=================================================
Generates CSV files for the following tables (in dependency order):

    1. dim_date        (independent)
    2. dim_location    (independent)
    3. dim_account     (-> dim_location, dim_date)
    4. dim_merchant    (-> dim_location, dim_date)

Design notes
------------
* Surrogate keys (location_key, account_key, merchant_key) are assigned
  EXPLICITLY here so the foreign keys line up across CSVs. When loading into
  SQL Server, enable IDENTITY_INSERT for these tables, e.g.:
      SET IDENTITY_INSERT dim_location ON;  -- BULK INSERT ...  SET IDENTITY_INSERT dim_location OFF;
* dim_date / dim_time use SUPPLIED integer keys (YYYYMMDD / HHMM), not IDENTITY.
* Accounts and merchants are NOT uniformly distributed: sampling is weighted so
  that Cairo + Giza + Alexandria account for ~62% of all rows.
* Account count (40,000) is sized so that ~1,000,000 future fact rows yield an
  average of ~25 transactions per account.

Output: one CSV per table in OUT_DIR (default ./dw_data).
Dependencies: Python 3.8+ standard library only.
"""

import csv
import os
import random
from datetime import date, timedelta

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
SEED = 42
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dw_data")

NUM_ACCOUNTS = 40_000          # ~25 txns/account against a ~1M fact target
NUM_MERCHANTS = 1_200          # distinct merchant locations

DATE_START = date(2023, 1, 1)  # dim_date window (2 years)
DATE_END = date(2024, 12, 31)

random.seed(SEED)

# ------------------------------------------------------------------
# Reference data: Egyptian cities with population/economy weights.
# Cairo + Giza + Alexandria intentionally dominate (~62%).
# (city, governorate, weight)
# ------------------------------------------------------------------
LOCATIONS = [
    ("Cairo",            "Cairo",        30.0),
    ("Giza",             "Giza",         18.0),
    ("Alexandria",       "Alexandria",   14.0),
    ("Shubra El Kheima", "Qalyubia",      4.0),
    ("Mansoura",         "Dakahlia",      3.0),
    ("Tanta",            "Gharbia",       2.5),
    ("Zagazig",          "Sharqia",       2.5),
    ("Port Said",        "Port Said",     2.5),
    ("Asyut",            "Asyut",         2.5),
    ("Minya",            "Minya",         2.5),
    ("Suez",             "Suez",          2.0),
    ("Ismailia",         "Ismailia",      2.0),
    ("Sohag",            "Sohag",         2.0),
    ("Hurghada",         "Red Sea",       2.0),
    ("Luxor",            "Luxor",         1.5),
    ("Aswan",            "Aswan",         1.5),
    ("Sharm El Sheikh",  "South Sinai",   1.5),
    ("Damanhur",         "Beheira",       1.5),
    ("Fayoum",           "Fayoum",        1.5),
]
COUNTRY = "Egypt"

# Real Egyptian merchant brands by category.
# (brand, default_size)   size in {'SME','Mid-Market','Enterprise'}
MERCHANT_BRANDS = {
    "Grocery": [
        ("Carrefour", "Enterprise"), ("Spinneys", "Enterprise"),
        ("Seoudi", "Mid-Market"), ("Metro Market", "Enterprise"),
        ("Kheir Zaman", "Mid-Market"), ("Hyper One", "Enterprise"),
        ("Kazyon", "Enterprise"), ("Awlad Ragab", "Mid-Market"),
        ("Fathalla Market", "Mid-Market"), ("BIM", "Enterprise"),
        ("Lulu Hypermarket", "Enterprise"), ("Gourmet Egypt", "SME"),
    ],
    "Restaurants": [
        ("Mo'men", "Mid-Market"), ("Cook Door", "Mid-Market"),
        ("GAD", "Mid-Market"), ("Koshary Abou Tarek", "SME"),
        ("Felfela", "SME"), ("McDonald's", "Enterprise"),
        ("KFC", "Enterprise"), ("Pizza Hut", "Enterprise"),
        ("Domino's Pizza", "Enterprise"), ("Hardee's", "Enterprise"),
        ("Buffalo Burger", "Mid-Market"), ("Starbucks", "Enterprise"),
    ],
    "Pharmacy": [
        ("El Ezaby Pharmacy", "Enterprise"), ("Seif Pharmacy", "Enterprise"),
        ("Roshdy Pharmacy", "Mid-Market"), ("Ezabawy", "Mid-Market"),
    ],
    "Electronics": [
        ("B.TECH", "Enterprise"), ("2B", "Mid-Market"),
        ("Tradeline", "Mid-Market"), ("Raya Shop", "Mid-Market"),
        ("Compu Me", "SME"),
    ],
    "Fuel": [
        ("Misr Petroleum", "Enterprise"), ("TotalEnergies", "Enterprise"),
        ("Emarat Misr", "Enterprise"), ("Wataniya", "Enterprise"),
        ("ADNOC", "Enterprise"), ("Shell", "Enterprise"),
    ],
    "Telecom": [
        ("Vodafone", "Enterprise"), ("Orange", "Enterprise"),
        ("Etisalat e&", "Enterprise"), ("WE", "Enterprise"),
    ],
    "E-commerce": [
        ("Jumia", "Enterprise"), ("Noon", "Enterprise"),
        ("Talabat", "Enterprise"), ("Amazon.eg", "Enterprise"),
        ("instashop", "Mid-Market"),
    ],
    "Fashion": [
        ("Mobaco", "Mid-Market"), ("Concrete", "Mid-Market"),
        ("Town Team", "Mid-Market"), ("Defacto", "Enterprise"),
        ("LC Waikiki", "Enterprise"), ("Max Fashion", "Enterprise"),
    ],
}
# Relative weight of each category in the merchant mix.
CATEGORY_WEIGHTS = {
    "Grocery": 22, "Restaurants": 24, "Pharmacy": 10, "Electronics": 8,
    "Fuel": 12, "Telecom": 6, "E-commerce": 8, "Fashion": 10,
}

# Account attribute pools (value, weight)
AGE_BANDS = [("18-24", 18), ("25-34", 34), ("35-44", 24),
             ("45-54", 13), ("55-64", 7), ("65+", 4)]
ACQUISITION_CHANNELS = [("Organic", 30), ("Referral", 20), ("Paid Social", 18),
                        ("Google Ads", 12), ("Branch", 10), ("Partner", 6),
                        ("Influencer", 4)]

# --- Correlated attributes ---------------------------------------------------
# customer_tier depends on age_band: older customers skew slightly more to
# Premium/Business. (Aggregate stays near 70/22/8.)
TIER_BY_AGE = {
    "18-24": [("Standard", 85), ("Premium", 13), ("Business", 2)],
    "25-34": [("Standard", 72), ("Premium", 22), ("Business", 6)],
    "35-44": [("Standard", 63), ("Premium", 27), ("Business", 10)],
    "45-54": [("Standard", 60), ("Premium", 27), ("Business", 13)],
    "55-64": [("Standard", 62), ("Premium", 26), ("Business", 12)],
    "65+":   [("Standard", 70), ("Premium", 22), ("Business", 8)],
}
# currency depends on customer_tier: Premium/Business hold far more FX accounts.
CURRENCY_BY_TIER = {
    "Standard": [("EGP", 98.0), ("USD", 1.3), ("EUR", 0.4), ("SAR", 0.2), ("AED", 0.1)],
    "Premium":  [("EGP", 90.0), ("USD", 6.0), ("EUR", 2.0), ("SAR", 1.0), ("AED", 1.0)],
    "Business": [("EGP", 80.0), ("USD", 12.0), ("EUR", 4.0), ("SAR", 2.0), ("AED", 2.0)],
}
# account_status depends on signup recency (f = 0 oldest .. 1 newest):
# older cohorts churn more (Dormant/Closed), recent signups are mostly Active.
def status_by_recency(f):
    if f > 0.80:
        pool = [("Active", 95), ("Suspended", 3), ("Dormant", 1), ("Closed", 1)]
    elif f > 0.50:
        pool = [("Active", 88), ("Dormant", 6), ("Suspended", 4), ("Closed", 2)]
    elif f > 0.25:
        pool = [("Active", 78), ("Dormant", 12), ("Suspended", 6), ("Closed", 4)]
    else:
        pool = [("Active", 68), ("Dormant", 16), ("Suspended", 7), ("Closed", 9)]
    return weighted_choice(pool)

# Account/branch acquisition ramps up over the window (later dates more likely).
# Latest day is ~(1 + GROWTH_SLOPE)x as likely as the earliest day.
GROWTH_SLOPE = 3.0


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def weighted_choice(pairs):
    """pairs: list of (value, weight) -> value"""
    values = [p[0] for p in pairs]
    weights = [p[1] for p in pairs]
    return random.choices(values, weights=weights, k=1)[0]


def date_to_key(d):
    return d.year * 10000 + d.month * 100 + d.day


def growth_weights(n, slope=GROWTH_SLOPE):
    """Linearly increasing weights over n ordered points (acquisition ramp)."""
    if n == 1:
        return [1.0]
    return [1.0 + slope * (i / (n - 1)) for i in range(n)]


def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def write_csv(filename, header, rows):
    path = os.path.join(OUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {len(rows):>7,} rows -> {path}")
    return path


# ------------------------------------------------------------------
# 1. dim_date
# ------------------------------------------------------------------
def gen_dim_date():
    header = ["date_key", "full_date", "day", "month", "quarter", "year", "is_weekend"]
    rows = []
    d = DATE_START
    while d <= DATE_END:
        # Egypt's weekend is Friday & Saturday -> weekday() 4, 5
        is_weekend = 1 if d.weekday() in (4, 5) else 0
        quarter = (d.month - 1) // 3 + 1
        rows.append([date_to_key(d), d.isoformat(), d.day, d.month,
                     quarter, d.year, is_weekend])
        d += timedelta(days=1)
    write_csv("dim_date.csv", header, rows)
    return [r[0] for r in rows]  # list of valid date_keys


# ------------------------------------------------------------------
# 2. dim_location
# ------------------------------------------------------------------
def gen_dim_location():
    header = ["location_key", "city", "governorate", "country"]
    rows = []
    location_keys = []        # (location_key, weight) for downstream sampling
    for i, (city, gov, weight) in enumerate(LOCATIONS, start=1):
        rows.append([i, city, gov, COUNTRY])
        location_keys.append((i, weight))
    write_csv("dim_location.csv", header, rows)
    return location_keys


# ------------------------------------------------------------------
# 3. dim_account
# ------------------------------------------------------------------
def gen_dim_account(location_keys, date_keys):
    header = ["account_key", "account_id", "location_key", "currency",
              "age_band", "acquisition_channel", "signup_date_key",
              "customer_tier", "account_status"]
    loc_values = [lk for lk, _ in location_keys]
    loc_weights = [w for _, w in location_keys]

    # Signup dates follow an acquisition ramp; sample indices so we can also
    # derive recency for account_status.
    n_dates = len(date_keys)
    date_w = growth_weights(n_dates)
    signup_idx = random.choices(range(n_dates), weights=date_w, k=NUM_ACCOUNTS)

    rows = []
    for i in range(1, NUM_ACCOUNTS + 1):
        idx = signup_idx[i - 1]
        recency = idx / (n_dates - 1)            # 0 = oldest, 1 = newest
        location_key = random.choices(loc_values, weights=loc_weights, k=1)[0]
        age_band = weighted_choice(AGE_BANDS)
        tier = weighted_choice(TIER_BY_AGE[age_band])       # tier ~ age
        currency = weighted_choice(CURRENCY_BY_TIER[tier])  # currency ~ tier
        status = status_by_recency(recency)                 # status ~ signup age
        rows.append([
            i,
            f"ACC{i:08d}",
            location_key,
            currency,
            age_band,
            weighted_choice(ACQUISITION_CHANNELS),
            date_keys[idx],
            tier,
            status,
        ])
    write_csv("dim_account.csv", header, rows)


# ------------------------------------------------------------------
# 4. dim_merchant
# ------------------------------------------------------------------
def gen_dim_merchant(location_keys, date_keys):
    header = ["merchant_key", "location_key", "merchant_id", "merchant_name",
              "merchant_category", "merchant_size", "opened_date_key"]
    loc_values = [lk for lk, _ in location_keys]
    loc_weights = [w for _, w in location_keys]
    # Map location_key -> city name for friendly merchant names
    city_by_key = {i: LOCATIONS[i - 1][0] for i, _ in location_keys}

    cat_values = list(CATEGORY_WEIGHTS.keys())
    cat_weights = list(CATEGORY_WEIGHTS.values())

    # Branch open dates follow the same acquisition ramp as accounts.
    n_dates = len(date_keys)
    date_w = growth_weights(n_dates)
    opened_idx = random.choices(range(n_dates), weights=date_w, k=NUM_MERCHANTS)

    # Track branch numbers per (brand, city) for realistic naming
    branch_counter = {}
    rows = []
    for i in range(1, NUM_MERCHANTS + 1):
        category = random.choices(cat_values, weights=cat_weights, k=1)[0]
        brand, size = random.choice(MERCHANT_BRANDS[category])
        location_key = random.choices(loc_values, weights=loc_weights, k=1)[0]
        city = city_by_key[location_key]

        key = (brand, city)
        branch_counter[key] = branch_counter.get(key, 0) + 1
        branch_no = branch_counter[key]
        merchant_name = f"{brand} - {city} #{branch_no}"

        opened_date_key = date_keys[opened_idx[i - 1]]
        rows.append([
            i,
            location_key,
            f"MER{i:06d}",
            merchant_name,
            category,
            size,
            opened_date_key,
        ])
    write_csv("dim_merchant.csv", header, rows)


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main():
    print("Generating FinTech DW dimension data...")
    ensure_out_dir()
    print(f"Output directory: {OUT_DIR}\n")

    print("[1/4] dim_date")
    date_keys = gen_dim_date()

    print("[2/4] dim_location")
    location_keys = gen_dim_location()

    print("[3/4] dim_account")
    gen_dim_account(location_keys, date_keys)

    print("[4/4] dim_merchant")
    gen_dim_merchant(location_keys, date_keys)

    print("\nDone. Load order: dim_date, dim_location, dim_account, dim_merchant.")
    print("Reminder: SET IDENTITY_INSERT ON for dim_location, dim_account, "
          "dim_merchant when bulk-loading (date uses a supplied key).")


if __name__ == "__main__":
    main()
