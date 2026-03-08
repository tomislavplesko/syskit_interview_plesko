"""
Pipeline Step 2 -- Build Analytical Layer
==========================================
Reads cleaned Parquet files from /outputs and produces:

  mart_tenant_health.parquet   -- One row per tenant; the master analytical table.
  mart_weekly_activity.parquet -- Weekly event volume per tenant (last 12 weeks).
  mart_channel_performance.parquet -- Acquisition channel quality metrics.
  mart_renewal_pipeline.parquet    -- Tenants with renewals in next 90 days.
  mart_trial_funnel.parquet        -- Trial-to-paid funnel proxy and drop-off points.

Health Score Definition (0-100):
  Component                             Max pts  Weight rationale
  -----------------------------------------------------------------
  Active user ratio (30-day)              25     Breadth of adoption
  Normalised recent event volume (30d)    20     Recency of engagement
  High-value event share                  20     Depth / value realisation
  Usage trend (4w vs prior 4w)            20     Direction of travel
  Days since last CS touch                15     CS coverage risk
  -----------------------------------------------------------------
  Total                                  100

Tiers:
  80-100  Healthy
  60-79   Neutral
  40-59   At Risk
  0-39    Red Alert  (or Churned if churned=True)

Run:
    python pipeline/02_build_analytical_layer.py
"""

import os
import warnings
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR  = os.path.join(BASE_DIR, "outputs")

# Snapshot date -- the "today" for this analysis
SNAPSHOT_DATE = pd.Timestamp("2024-06-30")
ANALYSIS_WINDOW_DAYS = 84   # 12 weeks


# ---------------------------------------------------------------------------
# Load clean data
# ---------------------------------------------------------------------------
def load_clean():
    tenants     = pd.read_parquet(os.path.join(OUT_DIR, "tenants_clean.parquet"))
    subs        = pd.read_parquet(os.path.join(OUT_DIR, "subscriptions_clean.parquet"))
    users       = pd.read_parquet(os.path.join(OUT_DIR, "users_clean.parquet"))
    events      = pd.read_parquet(os.path.join(OUT_DIR, "events_clean.parquet"))
    crm_co      = pd.read_parquet(os.path.join(OUT_DIR, "crm_companies_clean.parquet"))
    crm_act     = pd.read_parquet(os.path.join(OUT_DIR, "crm_activities_clean.parquet"))
    return tenants, subs, users, events, crm_co, crm_act


# ---------------------------------------------------------------------------
# User-level aggregates
# ---------------------------------------------------------------------------
def build_user_metrics(users: pd.DataFrame) -> pd.DataFrame:
    agg = (
        users.groupby("tenant_id")
        .agg(
            total_users=("user_id", "count"),
            active_users=("is_active", "sum"),
            admin_count=("role", lambda s: (s == "admin").sum()),
        )
        .reset_index()
    )
    agg["active_user_pct"] = agg["active_users"] / agg["total_users"].clip(lower=1)
    return agg


# ---------------------------------------------------------------------------
# Event-level aggregates
# ---------------------------------------------------------------------------
def build_event_metrics(events: pd.DataFrame) -> pd.DataFrame:
    df = events.copy()
    df["event_time"] = pd.to_datetime(df["event_time"])

    # Windows
    w30_start = SNAPSHOT_DATE - pd.Timedelta(days=30)
    w60_start = SNAPSHOT_DATE - pd.Timedelta(days=60)
    w90_start = SNAPSHOT_DATE - pd.Timedelta(days=90)
    w28_start = SNAPSHOT_DATE - pd.Timedelta(days=28)
    w56_start = SNAPSHOT_DATE - pd.Timedelta(days=56)

    def window_events(start, end=SNAPSHOT_DATE):
        mask = (df["event_time"] >= start) & (df["event_time"] <= end)
        return df[mask]

    def agg_events(subset, suffix):
        grp = (
            subset.groupby("tenant_id")
            .agg(
                **{f"total_events_{suffix}": ("event_count", "sum"),
                   f"hv_events_{suffix}": ("is_high_value",
                                           lambda x: (x * subset.loc[x.index, "event_count"]).sum()),
                   f"active_days_{suffix}": ("event_date", "nunique"),
                   f"active_users_{suffix}": ("user_id", "nunique"),
                   }
            )
            .reset_index()
        )
        return grp

    e30 = agg_events(window_events(w30_start), "30d")
    e60 = agg_events(window_events(w60_start), "60d")
    e90 = agg_events(window_events(w90_start), "90d")

    # Trend: last 4w vs prior 4w
    e_recent = agg_events(window_events(w28_start), "recent_4w")
    e_prior  = agg_events(window_events(w56_start, w28_start), "prior_4w")

    # Last event date per tenant
    last_event = (
        df.groupby("tenant_id")["event_time"].max()
        .reset_index()
        .rename(columns={"event_time": "last_event_date"})
    )

    # Merge
    result = e30
    for other in [e60, e90, e_recent, e_prior, last_event]:
        result = result.merge(other, on="tenant_id", how="left")

    result = result.fillna(0)

    # Trend ratio: recent 4w vs prior 4w (events)
    result["usage_trend_ratio"] = (
        result["total_events_recent_4w"] /
        result["total_events_prior_4w"].replace(0, np.nan)
    ).fillna(0)

    # High-value share in last 30d
    result["hv_share_30d"] = (
        result["hv_events_30d"] /
        result["total_events_30d"].replace(0, np.nan)
    ).fillna(0)

    return result


# ---------------------------------------------------------------------------
# CRM activity aggregates
# ---------------------------------------------------------------------------
def build_crm_metrics(crm_act: pd.DataFrame) -> pd.DataFrame:
    df = crm_act.copy()
    df["activity_date"] = pd.to_datetime(df["activity_date"])

    last_touch = (
        df.groupby("tenant_id")["activity_date"].max()
        .reset_index()
        .rename(columns={"activity_date": "last_cs_touch_date"})
    )

    # Activity counts 90d
    w90_start = SNAPSHOT_DATE - pd.Timedelta(days=90)
    recent = df[df["activity_date"] >= w90_start]
    act90 = (
        recent.groupby("tenant_id")
        .agg(
            cs_touches_90d=("activity_id", "count"),
            negative_outcomes_90d=("outcome", lambda x: (x == "negative").sum()),
            no_response_90d=("outcome", lambda x: (x == "no_response").sum()),
        )
        .reset_index()
    )

    result = last_touch.merge(act90, on="tenant_id", how="left")
    result["cs_touches_90d"] = result["cs_touches_90d"].fillna(0)
    result["days_since_cs_touch"] = (
        SNAPSHOT_DATE - result["last_cs_touch_date"]
    ).dt.days.fillna(999)   # 999 = never touched

    return result


# ---------------------------------------------------------------------------
# Health Score
# ---------------------------------------------------------------------------
def compute_health_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    df must have columns from the merged mart.
    Returns df with health_score (0-100) and health_tier columns added.
    """

    # --- Component 1: Active user ratio (25 pts) ---
    # Normalise: 100% active = 25 pts, 0% = 0 pts
    df["score_active_users"] = df["active_user_pct"].clip(0, 1) * 25

    # --- Component 2: Recent event volume vs 90d baseline (20 pts) ---
    # Ratio of 30d_events vs (90d_events/3). Capped at 2x = full score.
    baseline_daily = df["total_events_90d"] / 90
    recent_daily   = df["total_events_30d"] / 30
    ratio = (recent_daily / baseline_daily.replace(0, np.nan)).fillna(0).clip(0, 2)
    df["score_event_volume"] = (ratio / 2) * 20

    # --- Component 3: High-value event share (20 pts) ---
    # 40%+ HV share = full score; linear below
    df["score_hv_share"] = (df["hv_share_30d"] / 0.40).clip(0, 1) * 20

    # --- Component 4: Usage trend (20 pts) ---
    # trend_ratio >= 1.2  -> 20 pts (growing)
    # trend_ratio 0.8-1.2 -> 10 pts (stable)
    # trend_ratio < 0.8   -> 0 pts  (declining)
    conditions = [
        df["usage_trend_ratio"] >= 1.2,
        (df["usage_trend_ratio"] >= 0.8) & (df["usage_trend_ratio"] < 1.2),
    ]
    choices = [20, 10]
    df["score_trend"] = np.select(conditions, choices, default=0).astype(float)

    # --- Component 5: Days since last CS touch (15 pts) ---
    # 0-30 days = 15 pts; 31-60 = 10; 61-90 = 5; 90+ = 0
    conditions = [
        df["days_since_cs_touch"] <= 30,
        (df["days_since_cs_touch"] > 30) & (df["days_since_cs_touch"] <= 60),
        (df["days_since_cs_touch"] > 60) & (df["days_since_cs_touch"] <= 90),
    ]
    choices = [15, 10, 5]
    df["score_cs_touch"] = np.select(conditions, choices, default=0).astype(float)

    # --- Total ---
    score_cols = [
        "score_active_users", "score_event_volume",
        "score_hv_share", "score_trend", "score_cs_touch"
    ]
    df["health_score"] = df[score_cols].sum(axis=1).round(1)

    # Churned tenants -> always "Churned" tier
    def assign_tier(row):
        if row["churned"]:
            return "Churned"
        s = row["health_score"]
        if s >= 80:
            return "Healthy"
        elif s >= 60:
            return "Neutral"
        elif s >= 40:
            return "At Risk"
        else:
            return "Red Alert"

    df["health_tier"] = df.apply(assign_tier, axis=1)
    return df


# ---------------------------------------------------------------------------
# Expansion Candidates
# ---------------------------------------------------------------------------
def flag_expansion_candidates(df: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """
    Expansion candidate = starter/business tenant with:
      - health_tier Healthy or Neutral
      - growing pp_sync usage  OR  growing active user count
      - plan != enterprise
    """
    pp_events = events[events["event_name"].isin(["pp_sync_completed", "pp_sync_started"])]
    w60_start = SNAPSHOT_DATE - pd.Timedelta(days=60)
    w30_start = SNAPSHOT_DATE - pd.Timedelta(days=30)

    pp_recent = (
        pp_events[pp_events["event_time"] >= w30_start]
        .groupby("tenant_id")["event_count"].sum()
        .rename("pp_events_30d")
        .reset_index()
    )
    pp_prior = (
        pp_events[
            (pp_events["event_time"] >= w60_start) &
            (pp_events["event_time"] < w30_start)
        ]
        .groupby("tenant_id")["event_count"].sum()
        .rename("pp_events_prior_30d")
        .reset_index()
    )

    pp_trend = pp_recent.merge(pp_prior, on="tenant_id", how="outer").fillna(0)
    pp_trend["pp_growing"] = pp_trend["pp_events_30d"] > pp_trend["pp_events_prior_30d"]

    df = df.merge(pp_trend[["tenant_id", "pp_growing"]], on="tenant_id", how="left")
    df["pp_growing"] = df["pp_growing"].fillna(False)

    df["expansion_candidate"] = (
        (df["plan"] != "enterprise") &
        (df["health_tier"].isin(["Healthy", "Neutral"])) &
        (
            df["pp_growing"] |
            (df["active_users"] >= 4)   # multi-user engagement signal
        ) &
        (~df["churned"])
    )
    return df


# ---------------------------------------------------------------------------
# CS Blind Spots (churned with no CRM in last 60 days before churn)
# ---------------------------------------------------------------------------
def flag_cs_blind_spots(df: pd.DataFrame, crm_act: pd.DataFrame,
                        subs: pd.DataFrame) -> pd.DataFrame:
    churned = subs[subs["churned"] & subs["churn_date"].notna()][["tenant_id", "churn_date"]]

    # For each churned tenant, check if there was any CRM activity
    # in the 60 days BEFORE the churn date
    crm_act["activity_date"] = pd.to_datetime(crm_act["activity_date"])
    crm_act_indexed = crm_act.set_index("tenant_id")

    blind_spot_ids = []
    for _, row in churned.iterrows():
        tid = row["tenant_id"]
        churn_dt = pd.to_datetime(row["churn_date"])
        window_start = churn_dt - pd.Timedelta(days=60)
        if tid in crm_act_indexed.index:
            activities = crm_act_indexed.loc[[tid]]
            recent = activities[
                (activities["activity_date"] >= window_start) &
                (activities["activity_date"] <= churn_dt)
            ]
            if len(recent) == 0:
                blind_spot_ids.append(tid)
        else:
            blind_spot_ids.append(tid)  # No CRM record at all

    df["cs_blind_spot"] = df["tenant_id"].isin(blind_spot_ids)
    return df


# ---------------------------------------------------------------------------
# Weekly Activity Mart (last 12 weeks)
# ---------------------------------------------------------------------------
def build_weekly_mart(events: pd.DataFrame) -> pd.DataFrame:
    df = events.copy()
    df["event_time"] = pd.to_datetime(df["event_time"])
    start = SNAPSHOT_DATE - pd.Timedelta(weeks=12)
    df = df[df["event_time"] >= start]

    # Week start (Monday)
    df["week_start"] = df["event_time"].dt.to_period("W-SUN").apply(lambda r: r.start_time)

    weekly = (
        df.groupby(["tenant_id", "week_start"])
        .agg(
            event_count=("event_count", "sum"),
            hv_event_count=("is_high_value",
                            lambda x: (x * df.loc[x.index, "event_count"]).sum()),
            active_users=("user_id", "nunique"),
            active_days=("event_date", "nunique"),
        )
        .reset_index()
    )
    weekly["week_start"] = pd.to_datetime(weekly["week_start"])
    weekly.to_parquet(os.path.join(OUT_DIR, "mart_weekly_activity.parquet"), index=False)
    print(f"  [OK] mart_weekly_activity.parquet written ({len(weekly):,} rows)")
    return weekly


# ---------------------------------------------------------------------------
# Channel Performance Mart
# ---------------------------------------------------------------------------
def build_channel_mart(tenant_health: pd.DataFrame) -> pd.DataFrame:
    channel = (
        tenant_health.groupby("acquisition_source")
        .agg(
            total_tenants=("tenant_id", "count"),
            churned_tenants=("churned", "sum"),
            total_arr=("arr", "sum"),
            avg_arr=("arr", "mean"),
            avg_health_score=("health_score", "mean"),
            expansion_candidates=("expansion_candidate", "sum"),
            healthy_count=("health_tier", lambda x: (x == "Healthy").sum()),
            at_risk_count=("health_tier", lambda x: (x == "At Risk").sum()),
        )
        .reset_index()
    )
    channel["churn_rate"] = channel["churned_tenants"] / channel["total_tenants"]
    channel["pct_healthy"] = channel["healthy_count"] / channel["total_tenants"]
    channel.to_parquet(os.path.join(OUT_DIR, "mart_channel_performance.parquet"), index=False)
    print(f"  [OK] mart_channel_performance.parquet written ({len(channel):,} rows)")
    return channel


# ---------------------------------------------------------------------------
# Renewal Pipeline Mart (next 90 days)
# ---------------------------------------------------------------------------
def build_renewal_mart(tenant_health: pd.DataFrame) -> pd.DataFrame:
    df = tenant_health[~tenant_health["churned"]].copy()
    df["renewal_date"] = pd.to_datetime(df["renewal_date"])
    cutoff = SNAPSHOT_DATE + pd.Timedelta(days=90)
    upcoming = df[df["renewal_date"] <= cutoff].copy()
    upcoming["days_to_renewal"] = (upcoming["renewal_date"] - SNAPSHOT_DATE).dt.days
    cols = [
        "tenant_id", "company_name", "plan", "region", "csm_assigned",
        "arr", "renewal_date", "days_to_renewal",
        "health_score", "health_tier",
        "total_events_30d", "active_user_pct",
        "last_cs_touch_date", "days_since_cs_touch",
        "expansion_candidate",
    ]
    upcoming = upcoming[cols].sort_values("days_to_renewal")
    upcoming.to_parquet(os.path.join(OUT_DIR, "mart_renewal_pipeline.parquet"), index=False)
    print(f"  [OK] mart_renewal_pipeline.parquet written ({len(upcoming):,} rows)")
    return upcoming


# ---------------------------------------------------------------------------
# Trial-to-Paid Funnel Mart (proxy)
# ---------------------------------------------------------------------------
def build_trial_funnel_mart(tenant_health: pd.DataFrame) -> pd.DataFrame:
    """
    The dataset does not contain explicit lead/trial/paid transition timestamps.
    We therefore use a transparent proxy funnel focused on trial-sourced tenants:

      Stage 1: Trial-Sourced Accounts
      Stage 2: POV Started
      Stage 3: Converted to Paying (has ARR > 0)
      Stage 4: Retained Paying (not churned)
      Stage 5: Healthy Retained (not churned and Healthy/Neutral)

    This answers "where are we losing most potential customers" using the best
    available fields while documenting the assumption explicitly.
    """
    df = tenant_health.copy()
    trial = df[df["acquisition_source"] == "trial"].copy()

    stage_counts = []
    stage_counts.append({"stage_order": 1, "stage": "Trial-Sourced Accounts", "tenants": int(len(trial))})
    stage_counts.append({"stage_order": 2, "stage": "POV Started", "tenants": int((trial["pov_started"] == True).sum())})
    stage_counts.append({"stage_order": 3, "stage": "Converted to Paying (ARR > 0)", "tenants": int((trial["arr"] > 0).sum())})
    stage_counts.append({"stage_order": 4, "stage": "Retained Paying (Not Churned)", "tenants": int((~trial["churned"]).sum())})
    stage_counts.append({
        "stage_order": 5,
        "stage": "Healthy Retained (Healthy/Neutral)",
        "tenants": int((~trial["churned"] & trial["health_tier"].isin(["Healthy", "Neutral"])).sum()),
    })

    funnel = pd.DataFrame(stage_counts).sort_values("stage_order").reset_index(drop=True)
    funnel["conversion_from_prev"] = (funnel["tenants"] / funnel["tenants"].shift(1)).fillna(1.0)
    funnel["drop_from_prev"] = 1 - funnel["conversion_from_prev"]
    base = max(funnel.loc[0, "tenants"], 1)
    funnel["conversion_from_start"] = funnel["tenants"] / base

    # Additional diagnostics: by region and by plan (still trial-sourced only)
    diag = (
        trial.groupby(["region", "plan"])
        .agg(
            trial_accounts=("tenant_id", "count"),
            pov_started=("pov_started", "sum"),
            retained_paid=("churned", lambda x: (~x).sum()),
            healthy_retained=("health_tier", lambda x: ((x == "Healthy") | (x == "Neutral")).sum()),
            avg_health_score=("health_score", "mean"),
            avg_arr=("arr", "mean"),
        )
        .reset_index()
    )
    diag["retained_paid_rate"] = diag["retained_paid"] / diag["trial_accounts"].clip(lower=1)
    diag["healthy_retained_rate"] = diag["healthy_retained"] / diag["trial_accounts"].clip(lower=1)

    funnel.to_parquet(os.path.join(OUT_DIR, "mart_trial_funnel.parquet"), index=False)
    diag.to_parquet(os.path.join(OUT_DIR, "mart_trial_funnel_diagnostics.parquet"), index=False)
    print(f"  [OK] mart_trial_funnel.parquet written ({len(funnel):,} rows)")
    print(f"  [OK] mart_trial_funnel_diagnostics.parquet written ({len(diag):,} rows)")
    return funnel


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("STEP 2 -- Build Analytical Layer")
    print("=" * 60)

    tenants, subs, users, events, crm_co, crm_act = load_clean()

    print("\nBuilding user metrics...")
    user_metrics = build_user_metrics(users)

    print("Building event metrics...")
    event_metrics = build_event_metrics(events)

    print("Building CRM metrics...")
    crm_metrics = build_crm_metrics(crm_act)

    print("\nAssembling master mart...")
    # Base: tenants + subscriptions (1:1)
    mart = tenants.merge(
        subs[["tenant_id", "plan", "arr", "contract_start_date",
              "renewal_date", "churned", "churn_date"]],
        on="tenant_id", how="left", suffixes=("", "_sub")
    )

    # Drop duplicate plan column from subs (tenants already has it)
    if "plan_sub" in mart.columns:
        mart = mart.drop(columns=["plan_sub"])

    # CRM company
    mart = mart.merge(
        crm_co[["tenant_id", "lifecycle_stage", "acquisition_source", "pov_started"]],
        on="tenant_id", how="left"
    )

    # User metrics
    mart = mart.merge(user_metrics, on="tenant_id", how="left")

    # Event metrics
    mart = mart.merge(event_metrics, on="tenant_id", how="left")
    for col in event_metrics.columns:
        if col != "tenant_id" and col in mart.columns:
            mart[col] = mart[col].fillna(0)

    # CRM metrics
    mart = mart.merge(crm_metrics, on="tenant_id", how="left")
    mart["days_since_cs_touch"] = mart["days_since_cs_touch"].fillna(999)
    mart["cs_touches_90d"] = mart["cs_touches_90d"].fillna(0)

    # Fill nulls for tenants with zero users / zero events
    mart["total_users"]      = mart["total_users"].fillna(0)
    mart["active_users"]     = mart["active_users"].fillna(0)
    mart["active_user_pct"]  = mart["active_user_pct"].fillna(0)

    # Health score
    print("Computing health scores...")
    mart = compute_health_score(mart)

    # Expansion candidates
    print("Flagging expansion candidates...")
    mart = flag_expansion_candidates(mart, events)

    # CS blind spots
    print("Flagging CS blind spots...")
    mart = flag_cs_blind_spots(mart, crm_act, subs)

    # Final sort
    mart = mart.sort_values("health_score", ascending=False).reset_index(drop=True)

    mart.to_parquet(os.path.join(OUT_DIR, "mart_tenant_health.parquet"), index=False)
    print(f"  [OK] mart_tenant_health.parquet written ({len(mart):,} rows, {len(mart.columns)} cols)")

    # --- Secondary marts ---
    print("\nBuilding secondary marts...")
    build_weekly_mart(events)
    build_channel_mart(mart)
    build_renewal_mart(mart)
    trial_funnel = build_trial_funnel_mart(mart)

    # --- Summary stats ---
    print("\n" + "=" * 60)
    print("ANALYTICAL LAYER SUMMARY")
    print("=" * 60)
    total = len(mart)
    churned = mart["churned"].sum()
    print(f"Total tenants         : {total}")
    print(f"Churned               : {churned} ({churned/total:.1%})")
    print(f"Active ARR            : ${mart[~mart['churned']]['arr'].sum():,.0f}")
    print(f"\nHealth tier breakdown:")
    print(mart["health_tier"].value_counts().to_string())
    print(f"\nExpansion candidates  : {mart['expansion_candidate'].sum()}")
    print(f"CS blind spots        : {mart['cs_blind_spot'].sum()}")
    biggest_drop_idx = trial_funnel["drop_from_prev"].iloc[1:].idxmax()
    biggest_drop_stage = trial_funnel.loc[biggest_drop_idx, "stage"]
    biggest_drop_val = trial_funnel.loc[biggest_drop_idx, "drop_from_prev"]
    print(f"Trial funnel biggest drop: {biggest_drop_stage} ({biggest_drop_val:.1%} drop vs previous stage)")
    print(f"\nAvg health score      : {mart[~mart['churned']]['health_score'].mean():.1f}")
    print("\n[DONE] Step 2 complete")


if __name__ == "__main__":
    main()

