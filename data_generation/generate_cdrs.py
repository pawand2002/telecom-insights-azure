"""
TelecomInsights — CDR (Call Detail Record) Generator
Generates realistic CDRs for 1000 customers over 90 days
Output: cdrs/cdrs_YYYY_MM_DD.csv  (one file per day)

Run: python generate_cdrs.py
Depends on: generate_customers.py having been run first
"""

import csv
import random
import os
from datetime import datetime, timedelta
from generate_customers import generate_customers

random.seed(42)

# ── config ─────────────────────────────────────────────────────────────
DAYS_TO_GENERATE  = 90          # 3 months of data
START_DATE        = datetime(2024, 10, 1)
OUTPUT_DIR        = "output/cdrs"

# ── reference data ─────────────────────────────────────────────────────
CALL_TYPES        = ["Voice","Data","SMS","Video Call","IDD Voice"]
CALL_TYPE_WEIGHTS = [0.40, 0.35, 0.15, 0.06, 0.04]

NETWORK_TYPES     = ["4G","5G","3G","2G","WiFi Calling"]
NETWORK_WEIGHTS   = [0.45, 0.25, 0.15, 0.05, 0.10]

TERMINATION_CODES = ["NORMAL","BUSY","NO_ANSWER","NETWORK_ERROR","USER_DROPPED"]
TERM_WEIGHTS      = [0.78, 0.08, 0.07, 0.04, 0.03]

ROAMING_COUNTRIES = ["UAE","Saudi Arabia","Kuwait","Bahrain","Oman","UK","India","Pakistan"]

# Call volume patterns by segment
DAILY_CALL_RANGES = {
    "Low Value":   (1,  5),
    "Mid Value":   (3, 12),
    "High Value":  (8, 25),
    "VIP":        (15, 40)
}

# Revenue per minute by call type
REVENUE_PER_MIN = {
    "Voice":       0.12,    # QAR per minute
    "Data":        0.00,    # Charged via data plan
    "SMS":         0.10,    # Per SMS
    "Video Call":  0.18,
    "IDD Voice":   0.45     # International
}

# Data consumption MB per session by plan
DATA_MB_RANGES = {
    "Basic 10":   (5,  50),
    "Smart 25":   (20, 150),
    "Value 50":   (50, 400),
    "Super 100":  (100, 800),
    "Business 200": (200, 1500),
    "Executive 350": (300, 2000),
    "Premium 500":   (400, 3000),
    "Elite 1000":    (500, 5000)
}


def generate_cdr_id(date_str, seq):
    return f"CDR{date_str.replace('-','')}{seq:08d}"


def simulate_call(customer, call_date, seq):
    call_type    = random.choices(CALL_TYPES, weights=CALL_TYPE_WEIGHTS)[0]
    network_type = random.choices(NETWORK_TYPES, weights=NETWORK_WEIGHTS)[0]
    term_code    = random.choices(TERMINATION_CODES, weights=TERM_WEIGHTS)[0]

    # Duration depends on call type
    if call_type == "Voice":
        duration_sec = int(random.expovariate(1/180))  # avg 3 min
        duration_sec = max(5, min(duration_sec, 3600))
    elif call_type == "Video Call":
        duration_sec = int(random.expovariate(1/120))
        duration_sec = max(10, min(duration_sec, 1800))
    elif call_type == "IDD Voice":
        duration_sec = int(random.expovariate(1/240))
        duration_sec = max(10, min(duration_sec, 2400))
    elif call_type == "SMS":
        duration_sec = 0
    else:  # Data session
        duration_sec = int(random.expovariate(1/600))
        duration_sec = max(30, min(duration_sec, 7200))

    # Data consumed
    if call_type == "Data":
        mb_range  = DATA_MB_RANGES.get(customer["plan_name"], (20, 100))
        data_mb   = round(random.uniform(*mb_range), 2)
    else:
        data_mb = 0.0

    # Revenue calculation
    if call_type == "SMS":
        revenue = REVENUE_PER_MIN["SMS"]
    elif call_type == "Data":
        revenue = 0.0  # included in plan
    else:
        duration_min = duration_sec / 60
        revenue = round(duration_min * REVENUE_PER_MIN[call_type], 3)

    # Drop rate higher for low network quality
    if network_type in ["2G","3G"] and random.random() < 0.12:
        term_code = "USER_DROPPED"

    # Roaming flag — 3% of calls
    is_roaming     = 1 if random.random() < 0.03 else 0
    roaming_country= random.choice(ROAMING_COUNTRIES) if is_roaming else ""
    if is_roaming:
        revenue = revenue * 3.5   # roaming surcharge

    # Call start time — weighted towards business hours
    hour_weights = (
        [1]*5 +    # 00-04: very low
        [2]*2 +    # 05-06: waking up
        [5]*4 +    # 07-10: morning
        [8]*4 +    # 11-14: peak
        [7]*3 +    # 15-17: afternoon
        [9]*3 +    # 18-20: evening peak
        [6]*2 +    # 21-22: evening
        [3]*2      # 23: late night
    )
    hour   = random.choices(range(24), weights=hour_weights)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    call_start = call_date.replace(hour=hour, minute=minute, second=second)
    call_end   = call_start + timedelta(seconds=duration_sec)

    return {
        "cdr_id":            generate_cdr_id(call_date.strftime("%Y-%m-%d"), seq),
        "customer_id":       customer["customer_id"],
        "msisdn":            customer["msisdn"],
        "call_type":         call_type,
        "call_start_dttm":   call_start.strftime("%Y-%m-%d %H:%M:%S"),
        "call_end_dttm":     call_end.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds":  duration_sec,
        "network_type":      network_type,
        "data_consumed_mb":  data_mb,
        "termination_code":  term_code,
        "is_roaming":        is_roaming,
        "roaming_country":   roaming_country,
        "revenue_qar":       round(revenue, 3),
        "plan_type":         customer["plan_type"],
        "plan_name":         customer["plan_name"],
        "region":            customer["region"],
        "segment":           customer["segment"],
        "call_date":         call_date.strftime("%Y-%m-%d"),
        "call_hour":         hour,
        "created_at":        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


def generate_cdrs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load customers
    print("Loading customer profiles...")
    customers = generate_customers()
    # Filter active customers only
    active_customers = [c for c in customers if c["is_active"] == 1]
    print(f"Active customers: {len(active_customers)}")

    total_cdrs   = 0
    total_revenue= 0.0

    for day_offset in range(DAYS_TO_GENERATE):
        call_date  = START_DATE + timedelta(days=day_offset)
        date_str   = call_date.strftime("%Y-%m-%d")
        output_file= f"{OUTPUT_DIR}/cdrs_{date_str}.csv"

        # Skip if already generated
        if os.path.exists(output_file):
            print(f"  {date_str} — already exists, skipping")
            continue

        day_cdrs = []
        seq      = 1

        # Weekend has higher personal calls, lower business
        is_weekend = call_date.weekday() >= 4

        for customer in active_customers:
            # Churn risk customers make fewer calls towards end of period
            churn_multiplier = 1.0
            if customer["churn_risk"] == "High" and day_offset > 60:
                churn_multiplier = random.uniform(0.2, 0.6)
            elif customer["churn_risk"] == "Medium" and day_offset > 75:
                churn_multiplier = random.uniform(0.5, 0.8)

            # Some customers inactive on a given day
            if random.random() < 0.08:
                continue

            # Number of CDRs for this customer today
            lo, hi = DAILY_CALL_RANGES[customer["segment"]]
            if is_weekend:
                lo, hi = max(1, lo - 1), hi + 2
            num_cdrs = int(random.uniform(lo, hi) * churn_multiplier)
            num_cdrs = max(0, num_cdrs)

            for _ in range(num_cdrs):
                cdr = simulate_call(customer, call_date, seq)
                day_cdrs.append(cdr)
                seq += 1

        # Write daily file
        if day_cdrs:
            fieldnames = list(day_cdrs[0].keys())
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(day_cdrs)

            day_revenue = sum(c["revenue_qar"] for c in day_cdrs)
            total_cdrs   += len(day_cdrs)
            total_revenue+= day_revenue
            print(f"  {date_str} — {len(day_cdrs):,} CDRs, QAR {day_revenue:,.2f} revenue → {output_file}")
        else:
            print(f"  {date_str} — 0 CDRs")

    print(f"\nGeneration complete!")
    print(f"  Total CDRs:    {total_cdrs:,}")
    print(f"  Total revenue: QAR {total_revenue:,.2f}")
    print(f"  Daily avg:     {total_cdrs // DAYS_TO_GENERATE:,} CDRs/day")
    print(f"\nUpload output/cdrs/ folder to ADLS Bronze/cdrs/")


if __name__ == "__main__":
    generate_cdrs()
