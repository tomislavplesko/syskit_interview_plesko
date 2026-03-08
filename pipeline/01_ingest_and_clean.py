"""
Pipeline Step 1 -- Ingest & Clean
=================================
Loads all six source tables from the SQLite database, performs explicit data
quality checks (nothing is silently dropped), applies light normalisations, and
writes clean Parquet files to ../outputs/ for use by subsequent pipeline steps.

Run:
    python pipeline/01_ingest_and_clean.py

Outputs (in /outputs):
    tenants_clean.parquet
    subscriptions_clean.parquet
    users_clean.parquet
    events_clean.parquet
    crm_companies_clean.parquet
    crm_activities_clean.parquet
    data_quality_report.csv
"""

import sqlite3
import os
import sys
import subprocess
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, "saas_dataset.sqlite")
OUT_DIR  = os.path.join(BASE_DIR, "outputs")
os.makedirs(OUT_DIR, exist_ok=True)

DQ_LOG = []   # data quality issue log -- every anomaly is recorded here


def log_dq(table: str, column: str, issue: str, count: int, action: str):
    """Append a data-quality record to the log."""
    DQ_LOG.append(
        {"table": table, "column": column, "issue": issue, "affected_rows": count, "action": action}
    )
    print(f"  [DQ] {table}.{column} -- {issue} ({count} rows) -> {action}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_table(conn: sqlite3.Connection, table: str) -> pd.DataFrame:
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    print(f"\nLoaded {table}: {len(df):,} rows × {len(df.columns)} cols")
    return df


def null_summary(df: pd.DataFrame, table: str):
    for col in df.columns:
        n_null = df[col].isna().sum()
        if n_null > 0:
            log_dq(table, col, "null values", n_null, "retained -- handled downstream")


# ---------------------------------------------------------------------------
# 1. TENANTS
# ---------------------------------------------------------------------------
def clean_tenants(conn):
    df = load_table(conn, "tenants")

    null_summary(df, "tenants")

    # Normalise plan to lowercase
    df["plan"] = df["plan"].str.lower().str.strip()

    # Deduplicate -- keep first occurrence
    dupes = df.duplicated("tenant_id").sum()
    if dupes:
        log_dq("tenants", "tenant_id", "duplicate IDs", dupes, "kept first occurrence")
        df = df.drop_duplicates("tenant_id")

    # employee_size: map to ordered categorical
    size_order = ["1-50", "51-200", "201-1000", "1000+"]
    invalid_size = ~df["employee_size"].isin(size_order)
    if invalid_size.sum():
        log_dq("tenants", "employee_size", "unexpected values", invalid_size.sum(),
               "replaced with NaN")
        df.loc[invalid_size, "employee_size"] = np.nan

    df["employee_size"] = pd.Categorical(df["employee_size"], categories=size_order, ordered=True)

    df.to_parquet(os.path.join(OUT_DIR, "tenants_clean.parquet"), index=False)
    print(f"  [OK] tenants_clean.parquet written ({len(df):,} rows)")
    return df


# ---------------------------------------------------------------------------
# 2. SUBSCRIPTIONS
# ---------------------------------------------------------------------------
def clean_subscriptions(conn):
    df = load_table(conn, "subscriptions")

    null_summary(df, "subscriptions")

    # Parse dates
    for col in ["contract_start_date", "renewal_date", "churn_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # churned flag -- ensure boolean
    df["churned"] = df["churned"].astype(bool)

    # Consistency check: churned=True but no churn_date
    inconsistent = df["churned"] & df["churn_date"].isna()
    if inconsistent.sum():
        log_dq("subscriptions", "churn_date",
               "churned=True but churn_date is NULL", inconsistent.sum(),
               "set churn_date = renewal_date as proxy")
        df.loc[inconsistent, "churn_date"] = df.loc[inconsistent, "renewal_date"]

    # Consistency check: churned=False but churn_date filled
    wrong_flag = (~df["churned"]) & df["churn_date"].notna()
    if wrong_flag.sum():
        log_dq("subscriptions", "churned",
               "churned=False but churn_date is populated", wrong_flag.sum(),
               "set churned=True to match churn_date")
        df.loc[wrong_flag, "churned"] = True

    # ARR: flag non-positive values
    bad_arr = df["arr"] <= 0
    if bad_arr.sum():
        log_dq("subscriptions", "arr", "ARR <= 0", bad_arr.sum(), "retained -- flagged")
        df["arr_suspect"] = bad_arr
    else:
        df["arr_suspect"] = False

    df.to_parquet(os.path.join(OUT_DIR, "subscriptions_clean.parquet"), index=False)
    print(f"  [OK] subscriptions_clean.parquet written ({len(df):,} rows)")
    return df


# ---------------------------------------------------------------------------
# 3. USERS
# ---------------------------------------------------------------------------
def clean_users(conn):
    df = load_table(conn, "users")

    null_summary(df, "users")

    # Parse dates
    df["registered_at"] = pd.to_datetime(df["registered_at"], errors="coerce")
    df["last_seen_at"]   = pd.to_datetime(df["last_seen_at"],  errors="coerce")

    # is_active: ensure boolean
    df["is_active"] = df["is_active"].astype(bool)

    # Sanity: last_seen_at before registered_at
    bad_dates = df["last_seen_at"] < df["registered_at"]
    if bad_dates.sum():
        log_dq("users", "last_seen_at",
               "last_seen_at < registered_at", bad_dates.sum(),
               "retained -- flagged as date_order_suspect")
        df["date_order_suspect"] = bad_dates
    else:
        df["date_order_suspect"] = False

    # Normalise role
    df["role"] = df["role"].str.lower().str.strip()
    valid_roles = {"admin", "member", "read-only"}
    bad_roles = ~df["role"].isin(valid_roles)
    if bad_roles.sum():
        log_dq("users", "role", "unexpected role values", bad_roles.sum(),
               "replaced with 'member'")
        df.loc[bad_roles, "role"] = "member"

    df.to_parquet(os.path.join(OUT_DIR, "users_clean.parquet"), index=False)
    print(f"  [OK] users_clean.parquet written ({len(df):,} rows)")
    return df


# ---------------------------------------------------------------------------
# 4. EVENTS
# ---------------------------------------------------------------------------
def clean_events(conn):
    df = load_table(conn, "events")

    null_summary(df, "events")

    # Parse event_time
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    bad_ts = df["event_time"].isna()
    if bad_ts.sum():
        log_dq("events", "event_time", "unparseable timestamp", bad_ts.sum(),
               "rows dropped -- unusable without timestamp")
        df = df[~bad_ts]

    # event_count: must be >= 1
    bad_count = df["event_count"] < 1
    if bad_count.sum():
        log_dq("events", "event_count", "event_count < 1", bad_count.sum(),
               "set to 1 -- minimum meaningful count")
        df.loc[bad_count, "event_count"] = 1

    # Extract date
    df["event_date"] = df["event_time"].dt.date

    # Flag known high-value events
    HIGH_VALUE = {
        "policy_created",
        "risky_workspace_resolved",
        "sensitivity_label_applied",
        "license_recommendation_applied",
    }
    df["is_high_value"] = df["event_name"].isin(HIGH_VALUE)

    df.to_parquet(os.path.join(OUT_DIR, "events_clean.parquet"), index=False)
    print(f"  [OK] events_clean.parquet written ({len(df):,} rows)")
    return df


# ---------------------------------------------------------------------------
# 5. CRM COMPANIES
# ---------------------------------------------------------------------------
def clean_crm_companies(conn):
    df = load_table(conn, "crm_companies")

    null_summary(df, "crm_companies")

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["pov_started"] = df["pov_started"].astype(bool)
    df["lifecycle_stage"] = df["lifecycle_stage"].str.lower().str.strip()
    df["acquisition_source"] = df["acquisition_source"].str.lower().str.strip()

    df.to_parquet(os.path.join(OUT_DIR, "crm_companies_clean.parquet"), index=False)
    print(f"  [OK] crm_companies_clean.parquet written ({len(df):,} rows)")
    return df


# ---------------------------------------------------------------------------
# 6. CRM ACTIVITIES
# ---------------------------------------------------------------------------
def clean_crm_activities(conn):
    df = load_table(conn, "crm_activities")

    null_summary(df, "crm_activities")

    df["activity_date"] = pd.to_datetime(df["activity_date"], errors="coerce")
    df["activity_type"]  = df["activity_type"].str.lower().str.strip()
    df["outcome"]        = df["outcome"].str.lower().str.strip()

    # days_to_renewal can be negative (activity after renewal) -- that is valid per README
    neg_dtr = (df["days_to_renewal"] < 0).sum()
    if neg_dtr:
        log_dq("crm_activities", "days_to_renewal",
               "negative values (activity after renewal date)", neg_dtr,
               "retained -- expected per data spec")

    df.to_parquet(os.path.join(OUT_DIR, "crm_activities_clean.parquet"), index=False)
    print(f"  [OK] crm_activities_clean.parquet written ({len(df):,} rows)")
    return df


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("STEP 1 -- Ingest & Clean")
    print("=" * 60)

    # Build SQLite from CSV if it doesn't exist
    if not os.path.exists(DB_PATH):
        print("\n[INFO] saas_dataset.sqlite not found. Building from CSV files...")
        import subprocess
        subprocess.run(
            [sys.executable, os.path.join(BASE_DIR, "pipeline", "00_build_sqlite_from_csv.py")],
            check=True,
            cwd=BASE_DIR,
        )
        print()

    conn = sqlite3.connect(DB_PATH)

    clean_tenants(conn)
    clean_subscriptions(conn)
    clean_users(conn)
    clean_events(conn)
    clean_crm_companies(conn)
    clean_crm_activities(conn)

    conn.close()

    # Write DQ report
    dq_df = pd.DataFrame(DQ_LOG)
    if dq_df.empty:
        dq_df = pd.DataFrame(columns=["table", "column", "issue", "affected_rows", "action"])
    dq_path = os.path.join(OUT_DIR, "data_quality_report.csv")
    dq_df.to_csv(dq_path, index=False)
    print(f"\n[OK] Data quality report written -> {dq_path}")
    print(f"  Total issues logged: {len(dq_df)}")

    print("\n[DONE] Step 1 complete -- all clean files in /outputs")


if __name__ == "__main__":
    main()

