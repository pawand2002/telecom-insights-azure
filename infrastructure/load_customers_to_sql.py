"""
TelecomInsights — Load Customers into Azure SQL
Reads generated customers.csv and inserts into Azure SQL

Run: python load_customers_to_sql.py
Requires: pip install pyodbc
"""

import csv
import os
import pyodbc
from datetime import datetime

# ── Config — set as environment variables ─────────────────────
SQL_SERVER   = os.environ.get("SQL_SERVER",   "your-server.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "telecom-customers")
SQL_USER     = os.environ.get("SQL_USER",     "sqladmin")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")

CUSTOMER_FILE = "data_generation/output/customers/customers.csv"
BATCH_SIZE    = 100


def get_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str)


def load_customers():
    print("TelecomInsights — Loading Customers to Azure SQL")
    print("=" * 50)

    if not os.path.exists(CUSTOMER_FILE):
        print(f"ERROR: Customer file not found: {CUSTOMER_FILE}")
        print("Run data_generation/run_all.py first")
        return

    # Read customers
    with open(CUSTOMER_FILE, "r", encoding="utf-8") as f:
        reader   = csv.DictReader(f)
        customers= list(reader)
    print(f"Loaded {len(customers)} customers from CSV")

    # Connect
    print(f"Connecting to: {SQL_SERVER}/{SQL_DATABASE}")
    conn   = get_connection()
    cursor = conn.cursor()

    # Truncate existing data
    cursor.execute("TRUNCATE TABLE dbo.customers")
    conn.commit()
    print("Truncated existing customers table")

    # Insert in batches
    insert_sql = """
    INSERT INTO dbo.customers (
        customer_id, msisdn, first_name, last_name, age,
        nationality, language, region, device_type,
        plan_type, plan_name, monthly_fee_qar, data_allowance_gb,
        activation_date, tenure_months, segment, churn_risk,
        is_active, created_at, updated_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    inserted = 0
    batch    = []

    for row in customers:
        batch.append((
            row["customer_id"],
            row["msisdn"],
            row["first_name"],
            row["last_name"],
            int(row["age"]),
            row["nationality"],
            row["language"],
            row["region"],
            row["device_type"],
            row["plan_type"],
            row["plan_name"],
            float(row["monthly_fee_qar"]),
            int(row["data_allowance_gb"]),
            row["activation_date"],
            int(row["tenure_months"]),
            row["segment"],
            row["churn_risk"],
            int(row["is_active"]),
            row["created_at"],
            row["updated_at"]
        ))

        if len(batch) >= BATCH_SIZE:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            inserted += len(batch)
            batch     = []
            print(f"  Inserted {inserted} rows...")

    # Final batch
    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()
        inserted += len(batch)

    print(f"\nDone! Inserted {inserted} customers into Azure SQL")

    # Verify
    cursor.execute("SELECT COUNT(*) FROM dbo.customers")
    count = cursor.fetchone()[0]
    print(f"Verification: {count} rows in dbo.customers")

    cursor.execute("""
        SELECT plan_type, COUNT(*) as cnt, AVG(monthly_fee_qar) as avg_fee
        FROM dbo.customers GROUP BY plan_type
    """)
    print("\nBreakdown by plan type:")
    for row in cursor.fetchall():
        print(f"  {row[0]:10s}: {row[1]} customers, avg fee QAR {row[2]:.2f}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    load_customers()
