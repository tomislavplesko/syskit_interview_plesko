"""
Syskit Customer Intelligence Dashboard
========================================
A Streamlit dashboard for Customer Success, Sales, and Leadership.

Run locally:
    streamlit run dashboard/app.py

Deploy to Streamlit Community Cloud:
    Push to GitHub -> connect at share.streamlit.io -> set main file = dashboard/app.py

The dashboard auto-runs the pipeline if output files are not found.
"""

import os
import sys
import warnings
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR  = os.path.join(BASE_DIR, "outputs")
sys.path.insert(0, BASE_DIR)

SNAPSHOT_DATE = pd.Timestamp("2024-06-30")

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Syskit Customer Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS - High contrast, readable, professional
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* Main content - dark text on light background */
    .stApp {
        background: #f1f5f9 !important;
        font-family: 'Inter', -apple-system, sans-serif !important;
    }
    [data-testid="stAppViewContainer"] p,
    [data-testid="stAppViewContainer"] span,
    [data-testid="stAppViewContainer"] div,
    .stMarkdown p, .stMarkdown span, .stMarkdown li {
        color: #1e293b !important;
    }
    /* Captions and subtitles - ensure visible */
    [data-testid="stCaptionContainer"] p,
    [data-testid="stCaptionContainer"] {
        color: #334155 !important;
        font-size: 1rem !important;
    }
    /* Slider labels - dark text (critical for visibility) */
    [data-testid="stSlider"] label,
    [data-testid="stSlider"] p,
    [data-testid="stSlider"] span,
    .stSlider label,
    div[data-testid="stSlider"] * {
        color: #1e293b !important;
    }
    /* Block container - main content area */
    [data-testid="block-container"] p,
    [data-testid="block-container"] span,
    [data-testid="block-container"] label {
        color: #1e293b !important;
    }
    
    /* Sidebar - white/cream text on dark slate (high contrast) */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%) !important;
    }
    [data-testid="stSidebar"] .stMarkdown,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] [data-testid="stMarkdown"],
    [data-testid="stSidebar"] * {
        color: #f8fafc !important;
    }
    
    /* KPI cards - white bg, dark text */
    div[data-testid="stMetric"] {
        background: #ffffff !important;
        padding: 1.2rem;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        border: 1px solid #e2e8f0;
    }
    div[data-testid="stMetric"] label,
    div[data-testid="stMetric"] [data-testid="stMetricValue"],
    div[data-testid="stMetric"] div {
        color: #0f172a !important;
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #0f172a !important;
        font-weight: 600 !important;
    }
    
    /* Dataframes */
    .stDataFrame { border-radius: 8px; overflow: hidden; }
    
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Colour palette (brand-safe blues + semantic colours)
# ---------------------------------------------------------------------------
COLOURS = {
    "Healthy":    "#16A34A",
    "Neutral":    "#2563EB",
    "At Risk":    "#D97706",
    "Red Alert":  "#DC2626",
    "Churned":    "#6B7280",
    "primary":    "#2563EB",
    "background": "#F8FAFC",
}

# Plotly config for interactive charts (zoom, pan, hover, download)
PLOTLY_CONFIG = {
    "displayModeBar": True,
    "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    "scrollZoom": True,
}

# Plotly template for consistent, clean charts
CHART_TEMPLATE = dict(
    layout=dict(
        paper_bgcolor="rgba(255,255,255,0.9)",
        plot_bgcolor="rgba(255,255,255,0.5)",
        font=dict(family="DM Sans, sans-serif", size=12),
        margin=dict(t=50, b=50, l=50, r=50),
        hovermode="closest",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="rgba(0,0,0,0.1)",
            borderwidth=1,
        ),
        xaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.06)", zeroline=False),
        yaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.06)", zeroline=False),
    )
)


# ---------------------------------------------------------------------------
# Data loading (cached)
# ---------------------------------------------------------------------------
def run_pipeline_if_needed():
    """Run pipeline steps 0, 1, and 2 if outputs don't exist."""
    mart_path = os.path.join(OUT_DIR, "mart_tenant_health.parquet")
    db_path = os.path.join(BASE_DIR, "saas_dataset.sqlite")
    if not os.path.exists(mart_path):
        with st.spinner("First run: building analytical layer (this takes ~30 seconds)…"):
            import subprocess
            # Step 0: Build SQLite from CSV if needed
            if not os.path.exists(db_path):
                subprocess.run(
                    [sys.executable, os.path.join(BASE_DIR, "pipeline", "00_build_sqlite_from_csv.py")],
                    check=True, cwd=BASE_DIR
                )
            subprocess.run(
                [sys.executable, os.path.join(BASE_DIR, "pipeline", "01_ingest_and_clean.py")],
                check=True, cwd=BASE_DIR
            )
            subprocess.run(
                [sys.executable, os.path.join(BASE_DIR, "pipeline", "02_build_analytical_layer.py")],
                check=True, cwd=BASE_DIR
            )


@st.cache_data(ttl=3600)
def load_data():
    run_pipeline_if_needed()

    mart    = pd.read_parquet(os.path.join(OUT_DIR, "mart_tenant_health.parquet"))
    weekly  = pd.read_parquet(os.path.join(OUT_DIR, "mart_weekly_activity.parquet"))
    channel = pd.read_parquet(os.path.join(OUT_DIR, "mart_channel_performance.parquet"))
    renewal = pd.read_parquet(os.path.join(OUT_DIR, "mart_renewal_pipeline.parquet"))
    trial_funnel = pd.read_parquet(os.path.join(OUT_DIR, "mart_trial_funnel.parquet"))
    trial_diag = pd.read_parquet(os.path.join(OUT_DIR, "mart_trial_funnel_diagnostics.parquet"))

    # Churn predictions (optional -- run model if not found)
    pred_path = os.path.join(OUT_DIR, "churn_predictions.parquet")
    if os.path.exists(pred_path):
        preds = pd.read_parquet(pred_path)
    else:
        preds = None

    return mart, weekly, channel, renewal, trial_funnel, trial_diag, preds


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
def render_sidebar(mart):
    st.sidebar.markdown("## 📊 Syskit Analytics")
    st.sidebar.markdown("---")

    pages = [
        "🏠 Executive Overview",
        "🩺 Customer Health",
        "⚠️ At-Risk & Renewals",
        "📈 Usage Trends",
        "🧪 Trial-to-Paid Funnel",
        "📣 Marketing & Channels",
        "🤖 Churn Prediction",
    ]
    page = st.sidebar.radio("Navigate", pages)

    st.sidebar.markdown("---")
    st.sidebar.markdown("**🔍 Filters**")
    with st.sidebar.expander("Plan", expanded=True):
        all_plans = sorted(mart["plan"].dropna().unique().tolist())
        sel_plans = st.multiselect("Select plans", all_plans, default=all_plans, key="plan_filter")
    with st.sidebar.expander("Region", expanded=True):
        all_regions = sorted(mart["region"].dropna().unique().tolist())
        sel_regions = st.multiselect("Select regions", all_regions, default=all_regions, key="region_filter")
    with st.sidebar.expander("CSM", expanded=False):
        all_csms = sorted(mart["csm_assigned"].dropna().unique().tolist())
        sel_csms = st.multiselect("Select CSMs", all_csms, default=all_csms, key="csm_filter")
    with st.sidebar.expander("Health Tier", expanded=False):
        all_tiers = ["Healthy", "Neutral", "At Risk", "Red Alert", "Churned"]
        sel_tiers = st.multiselect("Select tiers", all_tiers, default=all_tiers, key="tier_filter")

    st.sidebar.markdown("---")
    st.sidebar.caption("Built for Syskit — Data Scientist interview task")

    return page, sel_plans, sel_regions, sel_csms, sel_tiers


def apply_filters(mart, sel_plans, sel_regions, sel_csms, sel_tiers):
    df = mart.copy()
    if sel_plans:
        df = df[df["plan"].isin(sel_plans)]
    if sel_regions:
        df = df[df["region"].isin(sel_regions)]
    if sel_csms:
        df = df[df["csm_assigned"].isin(sel_csms)]
    if sel_tiers:
        df = df[df["health_tier"].isin(sel_tiers)]
    return df


# ---------------------------------------------------------------------------
# Helper: KPI metric card
# ---------------------------------------------------------------------------
def kpi_row(metrics: list):
    cols = st.columns(len(metrics))
    for col, (label, value, delta, delta_colour) in zip(cols, metrics):
        col.metric(label, value, delta=delta,
                   delta_color=delta_colour if delta_colour else "normal")


# ---------------------------------------------------------------------------
# PAGE 1 -- Executive Overview
# ---------------------------------------------------------------------------
def page_executive(mart, channel):
    st.title("📊 Executive Overview")
    st.caption("Health of the full customer base at a glance")

    active = mart[~mart["churned"]]
    churned = mart[mart["churned"]]

    total_arr  = active["arr"].sum()
    churn_arr  = churned["arr"].sum()
    churn_rate = len(churned) / len(mart)
    at_risk_arr = mart[mart["health_tier"] == "At Risk"]["arr"].sum()
    red_arr     = mart[mart["health_tier"] == "Red Alert"]["arr"].sum()

    kpi_row([
        ("Active Tenants",   f"{len(active):,}",       None, None),
        ("Active ARR",        f"${total_arr/1e6:.2f}M",  None, None),
        ("Churn Rate",        f"{churn_rate:.1%}",        None, "inverse"),
        ("ARR At Risk",       f"${(at_risk_arr+red_arr)/1e6:.2f}M", None, "inverse"),
    ])

    st.markdown("---")
    col1, col2 = st.columns([1.2, 1])

    # --- Health tier donut ---
    with col1:
        tier_counts = mart["health_tier"].value_counts().reset_index()
        tier_counts.columns = ["tier", "count"]
        tier_order = ["Healthy", "Neutral", "At Risk", "Red Alert", "Churned"]
        tier_counts["tier"] = pd.Categorical(tier_counts["tier"],
                                              categories=tier_order, ordered=True)
        tier_counts = tier_counts.sort_values("tier")
        colour_seq = [COLOURS[t] for t in tier_counts["tier"]]

        fig = px.pie(
            tier_counts, names="tier", values="count",
            hole=0.55, color="tier",
            color_discrete_map=COLOURS,
            title="Tenant Health Distribution",
        )
        fig.update_traces(textposition="outside", textinfo="percent+label")
        fig.update_layout(showlegend=False, margin=dict(t=50, b=10))
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # --- ARR by health tier bar ---
    with col2:
        arr_tier = (
            mart.groupby("health_tier")["arr"].sum().reset_index()
        )
        arr_tier["health_tier"] = pd.Categorical(
            arr_tier["health_tier"], categories=tier_order, ordered=True
        )
        arr_tier = arr_tier.sort_values("health_tier")
        fig2 = px.bar(
            arr_tier, x="health_tier", y="arr",
            color="health_tier", color_discrete_map=COLOURS,
            title="ARR by Health Tier",
            labels={"arr": "ARR (USD)", "health_tier": "Tier"},
            text_auto=".2s",
        )
        fig2.update_layout(showlegend=False, margin=dict(t=50, b=10))
        st.plotly_chart(fig2, use_container_width=True, config=PLOTLY_CONFIG)

    st.markdown("---")

    # --- ARR by plan and region ---
    col3, col4 = st.columns(2)

    with col3:
        plan_arr = (
            active.groupby("plan")["arr"].sum().reset_index()
                   .sort_values("arr", ascending=False)
        )
        fig3 = px.bar(
            plan_arr, x="plan", y="arr",
            title="ARR by Plan",
            color="plan",
            color_discrete_sequence=px.colors.qualitative.Set2,
            labels={"arr": "ARR (USD)", "plan": "Plan"},
            text_auto=".2s",
        )
        fig3.update_layout(showlegend=False)
        st.plotly_chart(fig3, use_container_width=True, config=PLOTLY_CONFIG)

    with col4:
        region_arr = (
            active.groupby("region")["arr"].sum().reset_index()
                   .sort_values("arr", ascending=False)
        )
        fig4 = px.bar(
            region_arr, x="region", y="arr",
            title="ARR by Region",
            color="region",
            color_discrete_sequence=px.colors.qualitative.Pastel,
            labels={"arr": "ARR (USD)", "region": "Region"},
            text_auto=".2s",
        )
        fig4.update_layout(showlegend=False)
        st.plotly_chart(fig4, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Key call-outs ---
    st.markdown("---")
    st.subheader("🔑 Key Findings")

    blind_spots = mart["cs_blind_spot"].sum()
    expansion   = mart["expansion_candidate"].sum()

    c1, c2, c3 = st.columns(3)
    c1.info(f"**{blind_spots} churned tenants** had **zero CS contact** in their final 60 days -- these were invisible until they left.")
    c2.warning(f"**{expansion} tenants** show expansion signals. Proactive outreach could increase ARR without new logos.")
    churn_tier = mart[mart["health_tier"].isin(["At Risk", "Red Alert"]) & ~mart["churned"]]
    c3.error(f"**{len(churn_tier)} active tenants** are At Risk or Red Alert -- representing **${churn_tier['arr'].sum():,.0f}** in ARR.")


# ---------------------------------------------------------------------------
# PAGE 2 -- Customer Health
# ---------------------------------------------------------------------------
def page_health(mart):
    st.title("🩺 Customer Health")
    st.markdown(
        '<p style="color:#334155; font-size:1rem;">Health score = 0-100 across 5 dimensions. Hover over any bubble for details.</p>',
        unsafe_allow_html=True
    )

    # Interactive health score filter
    st.markdown('<p style="color:#1e293b; font-weight:500; margin-bottom:0.5rem;">🎚️ Filter by health score range</p>', unsafe_allow_html=True)
    score_min, score_max = st.slider(
        "Score range",
        min_value=0, max_value=100, value=(0, 100),
        help="Narrow the view to tenants within this score range",
        label_visibility="collapsed"
    )
    active = mart[~mart["churned"]]
    active = active[(active["health_score"] >= score_min) & (active["health_score"] <= score_max)]

    # Scatter: health score vs ARR, coloured by tier, sized by active users
    fig = px.scatter(
        active,
        x="health_score",
        y="arr",
        color="health_tier",
        color_discrete_map=COLOURS,
        size="active_users",
        size_max=30,
        hover_data=["company_name", "plan", "region", "csm_assigned",
                    "renewal_date", "days_since_cs_touch"],
        title="Health Score vs ARR (bubble size = active users)",
        labels={"health_score": "Health Score (0-100)", "arr": "ARR (USD)"},
    )
    fig.add_vline(x=40, line_dash="dash", line_color="red", opacity=0.5,
                  annotation_text="Red Alert threshold")
    fig.add_vline(x=60, line_dash="dash", line_color="orange", opacity=0.5,
                  annotation_text="At Risk threshold")
    fig.add_vline(x=80, line_dash="dash", line_color="green", opacity=0.5,
                  annotation_text="Healthy threshold")
    fig.update_layout(margin=dict(t=50))
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    st.markdown("---")

    # Score component breakdown for bottom 20 active tenants
    st.subheader("Health Score Components -- Bottom 20 Active Tenants")
    score_cols = ["score_active_users", "score_event_volume",
                  "score_hv_share", "score_trend", "score_cs_touch"]
    col_labels = {
        "score_active_users":  "Active Users (25)",
        "score_event_volume":  "Event Volume (20)",
        "score_hv_share":      "HV Events (20)",
        "score_trend":         "Usage Trend (20)",
        "score_cs_touch":      "CS Touch (15)",
    }

    bottom20 = (
        active.sort_values("health_score")
              .head(20)[["company_name", "health_score"] + score_cols]
    )
    bottom20 = bottom20.rename(columns=col_labels)

    fig2 = px.bar(
        bottom20.melt(id_vars=["company_name", "health_score"],
                      value_vars=list(col_labels.values()),
                      var_name="Component", value_name="Score"),
        x="Score", y="company_name",
        color="Component",
        orientation="h",
        title="Where are the lowest-scoring tenants losing points?",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig2.update_layout(yaxis={"categoryorder": "total ascending"},
                       margin=dict(t=50))
    st.plotly_chart(fig2, use_container_width=True, config=PLOTLY_CONFIG)

    # Detail table
    st.subheader("All Tenants -- Health Scorecard")
    display_cols = ["company_name", "plan", "region", "csm_assigned",
                    "health_tier", "health_score", "arr",
                    "active_user_pct", "total_events_30d",
                    "days_since_cs_touch", "renewal_date"]
    disp = active[display_cols].copy()
    disp["active_user_pct"] = (disp["active_user_pct"] * 100).round(1)
    disp["health_score"] = disp["health_score"].round(1)
    disp["arr"] = disp["arr"].round(0).astype(int)

    def colour_tier(val):
        c = COLOURS.get(val, "")
        return f"color: {c}; font-weight: bold"

    st.dataframe(
        disp.style.applymap(colour_tier, subset=["health_tier"]),
        use_container_width=True,
        height=450,
    )


# ---------------------------------------------------------------------------
# PAGE 3 -- At-Risk & Renewals
# ---------------------------------------------------------------------------
def page_renewals(mart, renewal):
    st.title("⚠️ At-Risk & Upcoming Renewals")
    st.caption("Accounts that need Customer Success attention now")

    active = mart[~mart["churned"]]

    # At-risk cohort
    at_risk = active[active["health_tier"].isin(["At Risk", "Red Alert"])].sort_values(
        "health_score"
    )

    col1, col2, col3 = st.columns(3)
    col1.metric("At-Risk / Red Alert Tenants", len(at_risk))
    col2.metric("ARR at Risk", f"${at_risk['arr'].sum():,.0f}")
    col3.metric("Renewals in 90 Days", len(renewal))

    st.markdown("---")

    st.subheader("🚨 At-Risk Tenants -- Prioritised by Score")
    at_risk_display = at_risk[[
        "company_name", "plan", "region", "csm_assigned",
        "health_tier", "health_score", "arr",
        "renewal_date", "days_since_cs_touch",
        "total_events_30d", "active_user_pct", "cs_blind_spot"
    ]].copy()
    at_risk_display["active_user_pct"] = (at_risk_display["active_user_pct"] * 100).round(1)
    at_risk_display["arr"] = at_risk_display["arr"].round(0).astype(int)
    at_risk_display["health_score"] = at_risk_display["health_score"].round(1)

    def highlight_blind_spot(row):
        if row["cs_blind_spot"]:
            return ["background-color: #FEF3C7"] * len(row)
        return [""] * len(row)

    st.dataframe(
        at_risk_display.style.apply(highlight_blind_spot, axis=1),
        use_container_width=True,
        height=400,
    )
    st.caption("🟡 Yellow rows = CS blind spots (no CRM contact in last 60 days)")

    st.markdown("---")

    st.subheader("📅 Renewal Pipeline (Next 90 Days)")

    if len(renewal) == 0:
        st.info("No renewals in the next 90 days.")
    else:
        # Colour renewals by health tier
        fig = px.scatter(
            renewal,
            x="days_to_renewal",
            y="arr",
            color="health_tier",
            color_discrete_map=COLOURS,
            size="health_score",
            size_max=25,
            hover_data=["company_name", "plan", "csm_assigned",
                        "days_since_cs_touch", "health_score"],
            title="Upcoming Renewals -- Health vs Days to Renewal",
            labels={
                "days_to_renewal": "Days to Renewal",
                "arr": "ARR (USD)",
                "health_tier": "Health Tier",
            },
        )
        fig.add_vline(x=30, line_dash="dash", line_color="red", opacity=0.4,
                      annotation_text="30 days")
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

        renewal_display = renewal[[
            "company_name", "plan", "csm_assigned", "arr",
            "days_to_renewal", "renewal_date",
            "health_tier", "health_score",
            "total_events_30d", "days_since_cs_touch",
        ]].copy()
        renewal_display["arr"] = renewal_display["arr"].round(0).astype(int)
        renewal_display["health_score"] = renewal_display["health_score"].round(1)
        st.dataframe(renewal_display, use_container_width=True, height=350)

    st.markdown("---")

    # CS Blind Spots detail
    st.subheader("👻 CS Blind Spots -- Churned with No CS Contact in Final 60 Days")
    blind = mart[mart["cs_blind_spot"]][[
        "company_name", "plan", "region", "arr",
        "churn_date", "total_events_30d", "total_events_90d"
    ]].copy()
    blind["arr"] = blind["arr"].round(0).astype(int)
    if len(blind) > 0:
        st.dataframe(blind, use_container_width=True, height=300)
        st.caption(
            f"These {len(blind)} tenants churned silently. "
            f"Total ARR lost without any CS intervention: **${blind['arr'].sum():,.0f}**"
        )
    else:
        st.success("No CS blind spots detected.")


# ---------------------------------------------------------------------------
# PAGE 4 -- Usage Trends
# ---------------------------------------------------------------------------
def page_trends(mart, weekly):
    st.title("📈 Usage Trends (Last 12 Weeks)")
    st.caption("Weekly product activity across the tenant base")

    # Aggregate weekly across all tenants (or filtered)
    weekly["week_start"] = pd.to_datetime(weekly["week_start"])

    # Filter to tenants in current mart filter
    tenant_ids = mart["tenant_id"].unique()
    w = weekly[weekly["tenant_id"].isin(tenant_ids)]

    agg = (
        w.groupby("week_start")
         .agg(
             total_events=("event_count", "sum"),
             hv_events=("hv_event_count", "sum"),
             active_users=("active_users", "sum"),
             active_tenants=("tenant_id", "nunique"),
         )
         .reset_index()
         .sort_values("week_start")
    )
    agg["hv_pct"] = agg["hv_events"] / agg["total_events"].replace(0, np.nan)

    # Line chart: total events & HV events
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(
            x=agg["week_start"], y=agg["total_events"],
            name="Total Events", line=dict(color=COLOURS["primary"], width=2.5)
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=agg["week_start"], y=agg["hv_events"],
            name="High-Value Events",
            line=dict(color=COLOURS["Healthy"], width=2, dash="dot")
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Bar(
            x=agg["week_start"], y=agg["active_tenants"],
            name="Active Tenants (WAT)",
            marker_color="rgba(37,99,235,0.15)",
            yaxis="y2",
        ),
        secondary_y=True,
    )
    fig.update_layout(
        title="Weekly Product Activity -- All Tenants",
        xaxis_title="Week",
        legend=dict(orientation="h", y=1.1),
        margin=dict(t=70),
    )
    fig.update_yaxes(title_text="Event Count", secondary_y=False)
    fig.update_yaxes(title_text="Active Tenants", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    st.markdown("---")

    # Plan breakdown weekly
    col1, col2 = st.columns(2)

    with col1:
        plan_map = mart[["tenant_id", "plan"]].drop_duplicates()
        w_plan = w.merge(plan_map, on="tenant_id", how="left")
        plan_weekly = (
            w_plan.groupby(["week_start", "plan"])["event_count"].sum()
                  .reset_index()
        )
        fig2 = px.line(
            plan_weekly, x="week_start", y="event_count",
            color="plan",
            color_discrete_sequence=px.colors.qualitative.Set2,
            title="Weekly Events by Plan",
            labels={"event_count": "Events", "week_start": "Week"},
        )
        st.plotly_chart(fig2, use_container_width=True, config=PLOTLY_CONFIG)

    with col2:
        # Trend heatmap: health tier × week
        tier_map = mart[["tenant_id", "health_tier"]].drop_duplicates()
        w_tier = w.merge(tier_map, on="tenant_id", how="left")
        tier_weekly = (
            w_tier.groupby(["week_start", "health_tier"])["event_count"].sum()
                  .reset_index()
        )
        tier_order = ["Healthy", "Neutral", "At Risk", "Red Alert"]
        tier_weekly["health_tier"] = pd.Categorical(
            tier_weekly["health_tier"], categories=tier_order, ordered=True
        )
        fig3 = px.line(
            tier_weekly.sort_values(["health_tier", "week_start"]),
            x="week_start", y="event_count",
            color="health_tier",
            color_discrete_map=COLOURS,
            title="Weekly Events by Health Tier",
            labels={"event_count": "Events", "week_start": "Week"},
        )
        st.plotly_chart(fig3, use_container_width=True, config=PLOTLY_CONFIG)

    # Top declining tenants
    st.subheader("📉 Most Declining Active Tenants (Usage Trend Ratio < 0.6)")
    active = mart[~mart["churned"]]
    declining = (
        active[active["usage_trend_ratio"] < 0.6]
              .sort_values("usage_trend_ratio")[
            ["company_name", "plan", "region", "csm_assigned",
             "health_tier", "health_score", "arr",
             "usage_trend_ratio", "total_events_recent_4w", "total_events_prior_4w"]
        ]
    ).copy()
    declining["usage_trend_ratio"] = declining["usage_trend_ratio"].round(3)
    declining["arr"] = declining["arr"].round(0).astype(int)
    if len(declining) > 0:
        st.dataframe(declining, use_container_width=True, height=350)
    else:
        st.success("No tenants with severe usage decline detected.")


# ---------------------------------------------------------------------------
# PAGE 5 -- Marketing & Channels
# ---------------------------------------------------------------------------
def page_marketing(mart, channel):
    st.title("📣 Marketing & Channel Performance")
    st.caption("Which acquisition channels bring customers who actually stay and grow?")

    # --- KPIs ---
    col1, col2, col3 = st.columns(3)
    best_channel = channel.loc[channel["pct_healthy"].idxmax(), "acquisition_source"]
    lowest_churn = channel.loc[channel["churn_rate"].idxmin(), "acquisition_source"]
    highest_arr  = channel.loc[channel["avg_arr"].idxmax(), "acquisition_source"]
    col1.metric("Best Retention Channel", best_channel.title())
    col2.metric("Lowest Churn Rate", lowest_churn.title())
    col3.metric("Highest Avg ARR", highest_arr.title())

    st.markdown("---")

    # --- Main visual: 4-axis channel comparison ---
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=[
            "Churn Rate by Channel",
            "Avg ARR by Channel",
            "% Healthy Tenants by Channel",
            "Total Tenants by Channel",
        ]
    )

    ch = channel.sort_values("churn_rate")
    colour_seq = px.colors.qualitative.Pastel

    fig.add_trace(
        go.Bar(x=ch["acquisition_source"], y=ch["churn_rate"],
               name="Churn Rate",
               marker_color=[COLOURS["Red Alert"] if v > 0.25 else COLOURS["Neutral"]
                             for v in ch["churn_rate"]],
               text=[f"{v:.0%}" for v in ch["churn_rate"]],
               textposition="outside"),
        row=1, col=1
    )

    ch2 = channel.sort_values("avg_arr", ascending=False)
    fig.add_trace(
        go.Bar(x=ch2["acquisition_source"], y=ch2["avg_arr"],
               name="Avg ARR",
               marker_color=COLOURS["primary"],
               text=[f"${v:,.0f}" for v in ch2["avg_arr"]],
               textposition="outside"),
        row=1, col=2
    )

    ch3 = channel.sort_values("pct_healthy", ascending=False)
    fig.add_trace(
        go.Bar(x=ch3["acquisition_source"], y=ch3["pct_healthy"],
               name="% Healthy",
               marker_color=COLOURS["Healthy"],
               text=[f"{v:.0%}" for v in ch3["pct_healthy"]],
               textposition="outside"),
        row=2, col=1
    )

    fig.add_trace(
        go.Bar(x=channel["acquisition_source"], y=channel["total_tenants"],
               name="Total Tenants",
               marker_color="steelblue",
               text=channel["total_tenants"].astype(str),
               textposition="outside"),
        row=2, col=2
    )

    fig.update_layout(
        height=650,
        showlegend=False,
        title_text="Channel Quality Scorecard",
        title_font_size=16,
    )
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    st.markdown("---")

    # --- Scatter: avg ARR vs pct healthy (bubble = total tenants) ---
    fig2 = px.scatter(
        channel,
        x="pct_healthy",
        y="avg_arr",
        size="total_tenants",
        color="churn_rate",
        color_continuous_scale="RdYlGn_r",
        text="acquisition_source",
        title="Channel Quality Matrix: % Healthy vs Avg ARR (size = volume, colour = churn rate)",
        labels={
            "pct_healthy": "% Healthy Tenants",
            "avg_arr": "Avg ARR (USD)",
            "churn_rate": "Churn Rate",
        },
    )
    fig2.update_traces(textposition="top center", textfont_size=11)
    fig2.update_layout(margin=dict(t=70))
    st.plotly_chart(fig2, use_container_width=True, config=PLOTLY_CONFIG)

    st.markdown("---")

    # --- Recommendation ---
    st.subheader("💡 Budget Recommendation")
    best = channel.sort_values("pct_healthy", ascending=False).iloc[0]
    worst = channel.sort_values("churn_rate", ascending=False).iloc[0]

    st.success(
        f"**Increase investment in '{best['acquisition_source'].title()}'** -- "
        f"highest % of healthy tenants ({best['pct_healthy']:.0%}) "
        f"and avg ARR of ${best['avg_arr']:,.0f}."
    )
    st.error(
        f"**Review '{worst['acquisition_source'].title()}'** channel -- "
        f"churn rate of {worst['churn_rate']:.0%} is the highest across channels. "
        f"Investigate whether targeting, onboarding, or product-market fit is the root cause."
    )

    # Full channel table
    st.subheader("Channel Detail Table")
    display = channel[[
        "acquisition_source", "total_tenants", "churned_tenants",
        "churn_rate", "avg_arr", "total_arr",
        "pct_healthy", "expansion_candidates"
    ]].copy()
    display["churn_rate"] = (display["churn_rate"] * 100).round(1).astype(str) + "%"
    display["pct_healthy"] = (display["pct_healthy"] * 100).round(1).astype(str) + "%"
    display["avg_arr"] = display["avg_arr"].round(0).astype(int)
    display["total_arr"] = display["total_arr"].round(0).astype(int)
    st.dataframe(display, use_container_width=True)


# ---------------------------------------------------------------------------
# PAGE 6 -- Trial-to-Paid Funnel (proxy)
# ---------------------------------------------------------------------------
def page_trial_funnel(trial_funnel, trial_diag):
    st.title("🧪 Trial-to-Paid Funnel")
    st.caption(
        "Dataset note: no explicit lead->trial->paid timestamps exist. "
        "This uses a transparent proxy funnel for trial-sourced accounts."
    )

    col1, col2, col3 = st.columns(3)
    start_count = int(trial_funnel.loc[trial_funnel["stage_order"] == 1, "tenants"].iloc[0])
    retained_count = int(trial_funnel.loc[trial_funnel["stage"] == "Retained Paying (Not Churned)", "tenants"].iloc[0])
    healthy_count = int(trial_funnel.loc[trial_funnel["stage"] == "Healthy Retained (Healthy/Neutral)", "tenants"].iloc[0])
    col1.metric("Trial-Sourced Accounts", f"{start_count:,}")
    col2.metric("Retained Paying", f"{retained_count:,}", delta=f"{(retained_count / max(start_count, 1)):.1%}")
    col3.metric("Healthy Retained", f"{healthy_count:,}", delta=f"{(healthy_count / max(start_count, 1)):.1%}")

    st.markdown("---")

    chart_df = trial_funnel.copy()
    chart_df["drop_pct"] = (chart_df["drop_from_prev"] * 100).round(1)
    fig = go.Figure()
    fig.add_trace(go.Funnel(
        y=chart_df["stage"],
        x=chart_df["tenants"],
        text=[f"{v:,}" for v in chart_df["tenants"]],
        textposition="inside",
        opacity=0.9,
        marker={"color": ["#2563EB", "#0EA5E9", "#14B8A6", "#16A34A", "#22C55E"]},
    ))
    fig.update_layout(title="Trial Funnel Conversion (Proxy)", margin=dict(t=60, b=20))
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    st.subheader("Biggest Drop-Off Point")
    drop_df = chart_df[chart_df["stage_order"] > 1].sort_values("drop_from_prev", ascending=False)
    top_drop = drop_df.iloc[0]
    st.warning(
        f"Largest drop is at **{top_drop['stage']}**: "
        f"**{top_drop['drop_pct']:.1f}%** drop from previous stage."
    )

    st.markdown("---")

    c1, c2 = st.columns(2)
    with c1:
        by_plan = trial_diag.groupby("plan", as_index=False).agg(
            trial_accounts=("trial_accounts", "sum"),
            retained_paid=("retained_paid", "sum"),
            healthy_retained=("healthy_retained", "sum"),
        )
        by_plan["retained_rate"] = by_plan["retained_paid"] / by_plan["trial_accounts"].clip(lower=1)
        by_plan["healthy_rate"] = by_plan["healthy_retained"] / by_plan["trial_accounts"].clip(lower=1)
        fig_plan = px.bar(
            by_plan.melt(
                id_vars=["plan"],
                value_vars=["retained_rate", "healthy_rate"],
                var_name="metric",
                value_name="rate",
            ),
            x="plan",
            y="rate",
            color="metric",
            barmode="group",
            text_auto=".1%",
            title="Trial Outcomes by Plan",
            labels={"rate": "Rate", "plan": "Plan"},
            color_discrete_sequence=["#2563EB", "#16A34A"],
        )
        st.plotly_chart(fig_plan, use_container_width=True, config=PLOTLY_CONFIG)

    with c2:
        by_region = trial_diag.groupby("region", as_index=False).agg(
            trial_accounts=("trial_accounts", "sum"),
            retained_paid=("retained_paid", "sum"),
            healthy_retained=("healthy_retained", "sum"),
        )
        by_region["retained_rate"] = by_region["retained_paid"] / by_region["trial_accounts"].clip(lower=1)
        by_region = by_region.sort_values("retained_rate", ascending=False)
        fig_region = px.bar(
            by_region,
            x="region",
            y="retained_rate",
            text_auto=".1%",
            title="Retained Paid Rate by Region (Trial-Sourced)",
            labels={"retained_rate": "Retained Paid Rate", "region": "Region"},
            color="retained_rate",
            color_continuous_scale="Blues",
        )
        fig_region.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_region, use_container_width=True, config=PLOTLY_CONFIG)

    st.subheader("Action Recommendation")
    st.info(
        "Focus Sales + CS handoff on the stage with the largest drop above. "
        "Specifically, require a structured onboarding checklist for every trial "
        "that starts POV, then trigger a CSM touch within 14 days if usage trend is flat."
    )

    st.dataframe(
        chart_df[["stage_order", "stage", "tenants", "conversion_from_prev", "drop_from_prev", "conversion_from_start"]],
        use_container_width=True,
    )


# ---------------------------------------------------------------------------
# PAGE 7 -- Churn Prediction
# ---------------------------------------------------------------------------
def page_churn_prediction(mart, preds):
    st.title("🤖 Churn Risk Prediction")
    st.markdown(
        '<p style="color:#334155; font-size:1rem; margin-bottom:1rem;">'
        'Machine learning model (XGBoost) trained to predict churn probability per tenant. '
        'Adjust the threshold below to change who gets flagged as high-risk.</p>',
        unsafe_allow_html=True
    )

    if preds is None:
        st.warning(
            "Churn predictions not found. Run `python ml/churn_model.py` to generate them, "
            "then refresh this page."
        )
        return

    st.markdown('<p style="color:#1e293b; font-weight:500; margin-bottom:0.5rem;">🎚️ Risk threshold (P(churn) above this = high-risk)</p>', unsafe_allow_html=True)
    threshold_pct = st.slider(
        "Threshold (%)",
        min_value=5, max_value=95, value=35, step=5,
        help="Lower = more tenants flagged. Higher = fewer, more confident flags.",
        label_visibility="collapsed"
    )
    threshold = threshold_pct / 100.0
    preds = preds.copy()
    preds["churn_flag"] = (preds["churn_probability"] >= threshold).astype(int)

    active_preds = preds[~preds["churned"]].copy()

    col1, col2, col3 = st.columns(3)
    col1.metric("High-Risk Active Tenants", int(active_preds["churn_flag"].sum()),
                help="Active tenants above threshold (proactive save opportunities)")
    at_risk_arr = active_preds[active_preds["churn_flag"] == 1]["arr"].sum()
    col2.metric("ARR in High-Risk Accounts", f"${at_risk_arr:,.0f}")
    avg_prob = active_preds["churn_probability"].mean()
    col3.metric("Avg Churn Probability", f"{avg_prob:.1%}")

    st.markdown("---")

    # Risk distribution histogram
    col_a, col_b = st.columns(2)

    with col_a:
        fig = px.histogram(
            active_preds, x="churn_probability",
            nbins=30, color="churn_flag",
            color_discrete_map={0: COLOURS["Healthy"], 1: COLOURS["Red Alert"]},
            title="Distribution of Churn Probabilities",
            labels={"churn_probability": "P(Churn)", "churn_flag": "Flagged"},
            barmode="stack",
        )
        fig.add_vline(x=threshold, line_dash="dash", line_color="red",
                      annotation_text=f"Threshold ({threshold:.0%})")
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    with col_b:
        # Scatter: churn prob vs ARR
        fig2 = px.scatter(
            active_preds,
            x="churn_probability",
            y="arr",
            color="health_tier",
            color_discrete_map=COLOURS,
            hover_data=["company_name", "plan"],
            title="Churn Probability vs ARR",
            labels={"churn_probability": "P(Churn)", "arr": "ARR (USD)"},
        )
        fig2.add_vline(x=threshold, line_dash="dash", line_color="red", opacity=0.5)
        st.plotly_chart(fig2, use_container_width=True, config=PLOTLY_CONFIG)

    # Feature importance
    fi_path = os.path.join(OUT_DIR, "feature_importance.csv")
    if os.path.exists(fi_path):
        fi_df = pd.read_csv(fi_path).head(15)
        fig3 = px.bar(
            fi_df.sort_values("importance"),
            x="importance", y="feature",
            orientation="h",
            title="Top 15 Most Predictive Features",
            labels={"importance": "Feature Importance", "feature": ""},
            color="importance",
            color_continuous_scale="Blues",
        )
        fig3.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig3, use_container_width=True, config=PLOTLY_CONFIG)

    # Model evaluation images
    eval_img = os.path.join(OUT_DIR, "churn_evaluation_curves.png")
    if os.path.exists(eval_img):
        st.markdown("---")
        st.subheader("Model Evaluation Curves")
        st.image(eval_img, use_container_width=True)

    metrics_path = os.path.join(OUT_DIR, "churn_model_metrics.csv")
    if os.path.exists(metrics_path):
        m = pd.read_csv(metrics_path).iloc[0]
        st.markdown("---")
        st.subheader("Model Metrics Summary")
        mc1, mc2, mc3 = st.columns(3)
        mc1.metric("CV ROC-AUC", f"{m['cv_roc_auc_mean']:.3f} ± {m['cv_roc_auc_std']:.3f}")
        mc2.metric("CV Avg Precision", f"{m['cv_avg_precision_mean']:.3f}")
        mc3.metric("Decision Threshold", f"{m['decision_threshold']:.0%}")

    st.markdown("---")

    # High-risk table: show ALL flagged tenants (active + churned) so we always have data
    mart_cols = ["tenant_id", "region", "csm_assigned", "days_since_cs_touch", "usage_trend_ratio"]
    mart_cols = [c for c in mart_cols if c in mart.columns]
    all_high_risk = preds[preds["churn_flag"] == 1].merge(
        mart[mart_cols],
        on="tenant_id", how="left"
    ).sort_values("churn_probability", ascending=False)
    display_cols = ["company_name", "plan", "csm_assigned", "churned",
        "arr", "churn_probability", "health_tier", "health_score",
        "days_since_cs_touch", "usage_trend_ratio"]
    if "region" in all_high_risk.columns:
        display_cols.insert(3, "region")
    all_high_risk = all_high_risk[[c for c in display_cols if c in all_high_risk.columns]].copy()
    all_high_risk["churn_probability"] = (all_high_risk["churn_probability"] * 100).round(1).astype(str) + "%"
    all_high_risk["arr"] = all_high_risk["arr"].round(0).astype(int)
    all_high_risk["status"] = all_high_risk["churned"].map({True: "Churned", False: "Active"})
    # Reorder: status first, then key columns
    display_order = ["status", "company_name", "plan", "region", "csm_assigned", "arr",
                     "churn_probability", "health_tier", "health_score",
                     "days_since_cs_touch", "usage_trend_ratio"]
    all_high_risk = all_high_risk[[c for c in display_order if c in all_high_risk.columns]]

    st.subheader("🔴 High-Risk Tenants — Sorted by Churn Probability")
    st.markdown(
        f'<p style="color:#334155; margin-bottom:1rem;">'
        f'<strong>{len(all_high_risk)} tenants</strong> flagged above {threshold_pct}% threshold. '
        'Churned = already left (model validation). Active = save opportunities.</p>',
        unsafe_allow_html=True
    )
    st.dataframe(all_high_risk, use_container_width=True, height=400)


# ---------------------------------------------------------------------------
# MAIN APP
# ---------------------------------------------------------------------------
def main():
    mart, weekly, channel, renewal, trial_funnel, trial_diag, preds = load_data()

    page, sel_plans, sel_regions, sel_csms, sel_tiers = render_sidebar(mart)

    # Apply global filters
    mart_f = apply_filters(mart, sel_plans, sel_regions, sel_csms, sel_tiers)
    if len(mart_f) == 0:
        st.warning("No tenants match the selected filters.")
        return

    if page == "🏠 Executive Overview":
        page_executive(mart_f, channel)
    elif page == "🩺 Customer Health":
        page_health(mart_f)
    elif page == "⚠️ At-Risk & Renewals":
        page_renewals(mart_f, renewal)
    elif page == "📈 Usage Trends":
        page_trends(mart_f, weekly)
    elif page == "🧪 Trial-to-Paid Funnel":
        page_trial_funnel(trial_funnel, trial_diag)
    elif page == "📣 Marketing & Channels":
        page_marketing(mart_f, channel)
    elif page == "🤖 Churn Prediction":
        page_churn_prediction(mart_f, preds)


if __name__ == "__main__":
    main()

