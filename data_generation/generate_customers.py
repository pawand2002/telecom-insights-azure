"""
TelecomInsights — Customer Profile Generator
Generates 1000 realistic telecom customer profiles
Output: customers/customers.csv

Run: python generate_customers.py
"""

import csv
import random
import os
from datetime import datetime, timedelta

random.seed(42)

# ── config ─────────────────────────────────────────────────────────────
NUM_CUSTOMERS     = 1000
OUTPUT_DIR        = "output/customers"
OUTPUT_FILE       = f"{OUTPUT_DIR}/customers.csv"

# ── reference data ─────────────────────────────────────────────────────
NATIONALITIES     = ["Qatari","Indian","Pakistani","Filipino","Egyptian",
                     "British","Jordanian","Lebanese","Bangladeshi","Sri Lankan"]
NATIONALITY_WEIGHTS=[0.12,0.28,0.15,0.10,0.06,0.05,0.05,0.04,0.08,0.07]

LANGUAGES         = ["Arabic","English","Hindi","Urdu","Tagalog","Bengali"]
PLAN_TYPES        = ["Prepaid","Postpaid"]
PLAN_NAMES = {
    "Prepaid":  ["Basic 10","Smart 25","Value 50","Super 100"],
    "Postpaid": ["Business 200","Executive 350","Premium 500","Elite 1000"]
}
PLAN_MONTHLY_FEES = {
    "Basic 10":10,"Smart 25":25,"Value 50":50,"Super 100":100,
    "Business 200":200,"Executive 350":350,"Premium 500":500,"Elite 1000":1000
}
SEGMENTS          = ["Low Value","Mid Value","High Value","VIP"]
SEGMENT_WEIGHTS   = [0.35, 0.40, 0.18, 0.07]
REGIONS           = ["Doha","Al Rayyan","Al Wakrah","Al Khor","Mesaieed","Lusail"]
REGION_WEIGHTS    = [0.45, 0.25, 0.10, 0.07, 0.05, 0.08]
DEVICE_TYPES      = ["Smartphone","Feature Phone","Tablet","MiFi/Router"]
DEVICE_WEIGHTS    = [0.72, 0.10, 0.08, 0.10]
CHURN_RISK        = ["Low","Medium","High"]

def random_date(start_year=2018, end_year=2024):
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")

def msisdn():
    # Qatar mobile format: +974 3/5/6/7 XXXXXXX
    prefix = random.choice(["3","5","6","7"])
    return f"+974{prefix}{random.randint(1000000,9999999)}"

def generate_customers():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    customers = []

    for i in range(1, NUM_CUSTOMERS + 1):
        plan_type   = random.choices(PLAN_TYPES, weights=[0.55, 0.45])[0]
        plan_name   = random.choice(PLAN_NAMES[plan_type])
        monthly_fee = PLAN_MONTHLY_FEES[plan_name]
        segment     = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS)[0]
        nationality = random.choices(NATIONALITIES, weights=NATIONALITY_WEIGHTS)[0]
        region      = random.choices(REGIONS, weights=REGION_WEIGHTS)[0]
        device      = random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS)[0]

        # Tenure drives churn risk — new customers churn more
        activation_date = random_date(2018, 2024)
        activation_dt   = datetime.strptime(activation_date, "%Y-%m-%d")
        tenure_months   = max(1, (datetime(2024, 12, 31) - activation_dt).days // 30)

        # Churn risk logic — short tenure + prepaid + low value = higher risk
        if tenure_months < 6 or (plan_type == "Prepaid" and segment == "Low Value"):
            churn_risk = random.choices(CHURN_RISK, weights=[0.20, 0.40, 0.40])[0]
        elif tenure_months > 36 and segment in ["High Value","VIP"]:
            churn_risk = random.choices(CHURN_RISK, weights=[0.75, 0.20, 0.05])[0]
        else:
            churn_risk = random.choices(CHURN_RISK, weights=[0.50, 0.35, 0.15])[0]

        # Age distribution realistic for Qatar expat population
        age = random.choices(
            list(range(18, 70)),
            weights=[max(1, 10 - abs(i - 32)) for i in range(18, 70)]
        )[0]

        # Data allowance based on plan
        data_allowance_gb = {
            "Basic 10": 1, "Smart 25": 5, "Value 50": 15, "Super 100": 30,
            "Business 200": 50, "Executive 350": 100, "Premium 500": 150, "Elite 1000": 300
        }[plan_name]

        customers.append({
            "customer_id":        f"CUST{i:05d}",
            "msisdn":             msisdn(),
            "first_name":         f"Customer{i}",
            "last_name":          f"LastName{i}",
            "age":                age,
            "nationality":        nationality,
            "language":           random.choice(LANGUAGES),
            "region":             region,
            "device_type":        device,
            "plan_type":          plan_type,
            "plan_name":          plan_name,
            "monthly_fee_qar":    monthly_fee,
            "data_allowance_gb":  data_allowance_gb,
            "activation_date":    activation_date,
            "tenure_months":      tenure_months,
            "segment":            segment,
            "churn_risk":         churn_risk,
            "is_active":          1 if random.random() > 0.05 else 0,
            "created_at":         activation_date,
            "updated_at":         datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    # Write CSV
    fieldnames = list(customers[0].keys())
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(customers)

    print(f"Generated {len(customers)} customers → {OUTPUT_FILE}")

    # Print summary stats
    prepaid  = sum(1 for c in customers if c["plan_type"] == "Prepaid")
    postpaid = sum(1 for c in customers if c["plan_type"] == "Postpaid")
    high_risk= sum(1 for c in customers if c["churn_risk"] == "High")
    avg_fee  = sum(c["monthly_fee_qar"] for c in customers) / len(customers)
    print(f"\nSummary:")
    print(f"  Prepaid:     {prepaid} ({prepaid/NUM_CUSTOMERS*100:.0f}%)")
    print(f"  Postpaid:    {postpaid} ({postpaid/NUM_CUSTOMERS*100:.0f}%)")
    print(f"  High churn risk: {high_risk} ({high_risk/NUM_CUSTOMERS*100:.0f}%)")
    print(f"  Avg monthly fee: QAR {avg_fee:.2f}")
    return customers


if __name__ == "__main__":
    generate_customers()
    print("\nDone! Upload output/customers/customers.csv to ADLS Bronze/customers/")
