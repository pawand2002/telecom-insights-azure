"""
TelecomInsights — Recharge Events Generator
Generates prepaid recharge (top-up) transactions
Output: recharges/recharges_YYYY_MM.csv  (one file per month)

Run: python generate_recharges.py
"""

import csv
import random
import os
from datetime import datetime, timedelta
from generate_customers import generate_customers

random.seed(123)

OUTPUT_DIR   = "output/recharges"
START_DATE   = datetime(2024, 10, 1)
END_DATE     = datetime(2024, 12, 31)

RECHARGE_CHANNELS  = ["Mobile App","USSD","Web Portal","Retailer","IVR","Bank Transfer"]
CHANNEL_WEIGHTS    = [0.35, 0.25, 0.15, 0.15, 0.05, 0.05]

RECHARGE_AMOUNTS   = [10, 25, 30, 50, 75, 100, 150, 200]
AMOUNT_WEIGHTS     = [0.15, 0.25, 0.10, 0.25, 0.08, 0.10, 0.04, 0.03]

VALIDITY_DAYS = {10: 7, 25: 15, 30: 15, 50: 30, 75: 30, 100: 30, 150: 45, 200: 60}


def generate_recharges():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Loading customers...")
    all_customers = generate_customers()
    # Only prepaid active customers recharge
    prepaid_customers = [c for c in all_customers
                         if c["plan_type"] == "Prepaid" and c["is_active"] == 1]
    print(f"Prepaid active customers: {len(prepaid_customers)}")

    # Group by month
    months = {}
    current = START_DATE
    while current <= END_DATE:
        month_key = current.strftime("%Y_%m")
        if month_key not in months:
            months[month_key] = []
        current += timedelta(days=1)

    total_recharges = 0
    total_amount    = 0.0

    for month_key in months:
        year, month = map(int, month_key.split("_"))
        # Find all days in this month within our range
        month_start = datetime(year, month, 1)
        if month < 12:
            month_end = datetime(year, month + 1, 1) - timedelta(days=1)
        else:
            month_end = datetime(year, 12, 31)

        month_records = []
        seq = 1

        for customer in prepaid_customers:
            # Recharge frequency based on churn risk and segment
            if customer["churn_risk"] == "High":
                avg_recharges = random.uniform(0.8, 2.0)   # churning = recharging less
            elif customer["segment"] == "VIP":
                avg_recharges = random.uniform(4.0, 8.0)
            elif customer["segment"] == "High Value":
                avg_recharges = random.uniform(3.0, 6.0)
            elif customer["segment"] == "Mid Value":
                avg_recharges = random.uniform(2.0, 4.0)
            else:
                avg_recharges = random.uniform(1.0, 3.0)

            num_recharges = max(0, int(random.gauss(avg_recharges, 0.5)))

            for _ in range(num_recharges):
                # Random day in month
                days_in_month = (month_end - month_start).days + 1
                recharge_date = month_start + timedelta(days=random.randint(0, days_in_month - 1))
                recharge_time = recharge_date.replace(
                    hour=random.randint(8, 22),
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )

                amount   = random.choices(RECHARGE_AMOUNTS, weights=AMOUNT_WEIGHTS)[0]
                channel  = random.choices(RECHARGE_CHANNELS, weights=CHANNEL_WEIGHTS)[0]
                validity = VALIDITY_DAYS[amount]
                expiry   = recharge_time + timedelta(days=validity)

                month_records.append({
                    "recharge_id":      f"RCH{month_key}{seq:07d}",
                    "customer_id":      customer["customer_id"],
                    "msisdn":           customer["msisdn"],
                    "recharge_amount":  amount,
                    "recharge_channel": channel,
                    "recharge_dttm":    recharge_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "validity_days":    validity,
                    "expiry_dttm":      expiry.strftime("%Y-%m-%d %H:%M:%S"),
                    "plan_name":        customer["plan_name"],
                    "segment":          customer["segment"],
                    "region":           customer["region"],
                    "churn_risk":       customer["churn_risk"],
                    "recharge_month":   recharge_time.strftime("%Y-%m"),
                    "recharge_date":    recharge_time.strftime("%Y-%m-%d"),
                    "created_at":       datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                seq += 1

        # Write monthly file
        output_file = f"{OUTPUT_DIR}/recharges_{month_key}.csv"
        if month_records:
            fieldnames = list(month_records[0].keys())
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(month_records)

        month_revenue = sum(r["recharge_amount"] for r in month_records)
        total_recharges += len(month_records)
        total_amount    += month_revenue
        print(f"  {month_key} — {len(month_records):,} recharges, QAR {month_revenue:,.2f} → {output_file}")

    print(f"\nRecharge generation complete!")
    print(f"  Total recharges: {total_recharges:,}")
    print(f"  Total amount:    QAR {total_amount:,.2f}")
    print(f"  Avg per month:   {total_recharges // 3:,}")
    print(f"\nUpload output/recharges/ to ADLS Bronze/recharges/")


if __name__ == "__main__":
    generate_recharges()
