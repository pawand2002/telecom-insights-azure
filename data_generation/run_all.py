"""
TelecomInsights — Master Data Generation Runner
Runs all generators in the correct order

Run: python run_all.py
"""

import os
import time

def run_step(step_name, func):
    print(f"\n{'='*60}")
    print(f"  STEP: {step_name}")
    print(f"{'='*60}")
    start = time.time()
    func()
    elapsed = time.time() - start
    print(f"\n  Completed in {elapsed:.1f} seconds")


def check_output():
    print(f"\n{'='*60}")
    print("  OUTPUT SUMMARY")
    print(f"{'='*60}")
    base = "output"
    total_size = 0
    for folder in ["customers","cdrs","recharges"]:
        path = os.path.join(base, folder)
        if os.path.exists(path):
            files = os.listdir(path)
            size  = sum(os.path.getsize(os.path.join(path,f))
                        for f in files if os.path.isfile(os.path.join(path,f)))
            total_size += size
            print(f"  {folder:15s}: {len(files):4d} files  {size/1024/1024:.1f} MB")
    print(f"  {'TOTAL':15s}:       {total_size/1024/1024:.1f} MB")


def adls_upload_instructions():
    print(f"\n{'='*60}")
    print("  NEXT STEP — UPLOAD TO ADLS GEN2")
    print(f"{'='*60}")
    print("""
  1. Open Azure Portal → your Storage Account
  2. Go to Containers → bronze

  3. Upload customers:
     Path: bronze/customers/customers.csv

  4. Upload CDRs (90 files):
     Path: bronze/cdrs/cdrs_YYYY_MM_DD.csv
     (Upload entire cdrs folder)

  5. Upload recharges (3 files):
     Path: bronze/recharges/recharges_YYYY_MM.csv

  6. Azure CLI alternative (faster):
     az storage blob upload-batch \\
         --destination bronze \\
         --source output/ \\
         --account-name YOUR_STORAGE_ACCOUNT \\
         --auth-mode login

  7. Verify structure in portal:
     bronze/
       customers/
         customers.csv
       cdrs/
         cdrs_2024_10_01.csv
         cdrs_2024_10_02.csv
         ... (90 files)
       recharges/
         recharges_2024_10.csv
         recharges_2024_11.csv
         recharges_2024_12.csv
    """)


if __name__ == "__main__":
    print("\nTelecomInsights — Data Generation Suite")
    print("Azure Lakehouse Platform — Telecom Analytics")
    print(f"{'='*60}\n")

    from generate_customers import generate_customers
    from generate_cdrs      import generate_cdrs
    from generate_recharges import generate_recharges

    run_step("Generate Customer Profiles (1,000 customers)",  generate_customers)
    run_step("Generate CDR Records (90 days)",               generate_cdrs)
    run_step("Generate Recharge Events (3 months)",          generate_recharges)

    check_output()
    adls_upload_instructions()

    print("\nAll data generated successfully!")
    print("Run simulate_stream.py separately to start real-time event simulation.")
