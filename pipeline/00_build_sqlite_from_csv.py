"""
Pipeline Step 0 -- Build SQLite from CSV (if SQLite not provided)
==================================================================
Loads the six source tables from CSV files and creates saas_dataset.sqlite.
Run this first if you have CSV files but no SQLite database.

The client may provide either:
  - saas_dataset.sqlite (all six tables) -> skip this step
  - Individual CSV files -> run this script first

Run:
    python pipeline/00_build_sqlite_from_csv.py

Output:
    saas_dataset.sqlite (in project root)
"""

import os
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "saas_dataset.sqlite")

# CSV paths (in project root)
TABLES = [
    ("tenants", "tenants.csv"),
    ("subscriptions", "subscriptions.csv"),
    ("users", "users.csv"),
    ("events", "events.csv"),
    ("crm_companies", "crm_companies.csv"),
    ("crm_activities", "crm_activities.csv"),
]


def main():
    print("=" * 60)
    print("STEP 0 -- Build SQLite from CSV")
    print("=" * 60)

    if os.path.exists(DB_PATH):
        print(f"\n[SKIP] {DB_PATH} already exists.")
        print("       Delete it to rebuild from CSV.")
        return

    missing = [f for _, f in TABLES if not os.path.exists(os.path.join(BASE_DIR, f))]
    if missing:
        raise FileNotFoundError(
            f"Missing CSV files: {missing}. "
            "Ensure tenants.csv, subscriptions.csv, users.csv, events.csv, "
            "crm_companies.csv, and crm_activities.csv are in the project root."
        )

    conn = sqlite3.connect(DB_PATH)

    for table, csv_file in TABLES:
        path = os.path.join(BASE_DIR, csv_file)
        df = pd.read_csv(path)
        df.to_sql(table, conn, index=False, if_exists="replace")
        print(f"  [OK] {table}: {len(df):,} rows from {csv_file}")

    conn.close()
    print(f"\n[DONE] Created {DB_PATH}")
    print("       Run: python pipeline/01_ingest_and_clean.py")


if __name__ == "__main__":
    main()
