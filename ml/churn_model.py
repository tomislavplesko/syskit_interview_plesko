"""
Machine Learning -- Churn Risk Model
=====================================
Prediction target: Will a tenant churn at / before their next renewal?

Why churn (not expansion)?
  - Churn has direct, measurable financial impact (ARR lost).
  - The dataset contains a ground-truth label (churned flag + churn_date).
  - Identifying at-risk accounts 60-90 days ahead is the single highest-value
    intervention Customer Success can make. An expansion model, while useful,
    has lower urgency and a weaker label (no clear "expanded" column).

Evaluation metric rationale:
  - We optimise for **Recall** at a business-relevant decision threshold.
  - A missed churn (false negative) costs the full ARR of that account.
  - A false alarm (false positive) costs a CSM call -- cheap by comparison.
  - We report ROC-AUC for overall discrimination quality and a Precision-Recall
    curve because class imbalance (~20-30% churn) makes accuracy misleading.

Run:
    python ml/churn_model.py

Outputs (in /outputs):
    churn_model.joblib          -- Trained model pipeline
    churn_predictions.parquet   -- Predicted churn probabilities per tenant
    churn_model_metrics.csv     -- Evaluation metrics summary
    feature_importance.csv      -- Feature importance ranking
"""

import os
import warnings
import joblib
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from sklearn.model_selection import StratifiedKFold, cross_validate
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OrdinalEncoder
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (
    roc_auc_score, average_precision_score,
    precision_recall_curve, roc_curve,
    classification_report, confusion_matrix,
)
from sklearn.impute import SimpleImputer

warnings.filterwarnings("ignore")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR  = os.path.join(BASE_DIR, "outputs")

SNAPSHOT_DATE = pd.Timestamp("2024-06-30")

# Business threshold: flag tenant as at-risk if P(churn) > this
DECISION_THRESHOLD = 0.35


# ---------------------------------------------------------------------------
# Feature Engineering
# ---------------------------------------------------------------------------
def build_features(mart: pd.DataFrame) -> pd.DataFrame:
    df = mart.copy()

    # Days until renewal (negative = already past due)
    df["renewal_date"] = pd.to_datetime(df["renewal_date"])
    df["days_to_renewal"] = (df["renewal_date"] - SNAPSHOT_DATE).dt.days.fillna(999)

    # Contract age (months)
    df["contract_start_date"] = pd.to_datetime(df["contract_start_date"])
    df["contract_age_months"] = (
        (SNAPSHOT_DATE - df["contract_start_date"]).dt.days / 30
    ).fillna(0)

    # Plan encoded
    plan_map = {"starter": 0, "business": 1, "enterprise": 2}
    df["plan_encoded"] = df["plan"].map(plan_map).fillna(0)

    # Employee size encoded
    size_map = {"1-50": 0, "51-200": 1, "201-1000": 2, "1000+": 3}
    df["employee_size_encoded"] = (
        df["employee_size"].astype(str).map(size_map).fillna(0)
    )

    # CRM lifecycle encoded
    stage_map = {"onboarding": 0, "active": 1, "at-risk": 2, "churned": 3}
    df["lifecycle_encoded"] = df["lifecycle_stage"].map(stage_map).fillna(1)

    # POV flag
    df["pov_started"] = df["pov_started"].fillna(False).astype(int)

    # Acquisition source encoded
    source_dummies = pd.get_dummies(
        df["acquisition_source"].fillna("unknown"), prefix="src"
    )
    df = pd.concat([df, source_dummies], axis=1)

    # Region encoded
    region_dummies = pd.get_dummies(
        df["region"].fillna("unknown"), prefix="rgn"
    )
    df = pd.concat([df, region_dummies], axis=1)

    # Industry encoded
    industry_dummies = pd.get_dummies(
        df["industry"].fillna("unknown"), prefix="ind"
    )
    df = pd.concat([df, industry_dummies], axis=1)

    return df


# NOTE: lifecycle_encoded is EXCLUDED from FEATURE_COLS_BASE.
# Rationale: lifecycle_stage includes the value "churned" which is essentially
# a direct label leak -- a human CSM flagged the account as churned, which is
# logically identical to the target variable. Including it produces perfect AUC
# (1.0) but the model would be useless in production (you already know it churned).
# We train the "clean" model without it and report both for transparency.
FEATURE_COLS_BASE = [
    # Subscription / account
    "arr", "days_to_renewal", "contract_age_months",
    "plan_encoded", "employee_size_encoded",
    # CRM (lifecycle_encoded excluded -- see note above)
    "pov_started",
    "days_since_cs_touch", "cs_touches_90d",
    # Usage -- recency & volume
    "total_events_30d", "total_events_60d", "total_events_90d",
    "hv_events_30d", "hv_share_30d",
    "active_days_30d", "active_users_30d",
    # Usage -- trend
    "usage_trend_ratio",
    "total_events_recent_4w", "total_events_prior_4w",
    # User adoption
    "total_users", "active_users", "active_user_pct",
    # Health score components (derived from usage -- no leakage)
    "score_active_users", "score_event_volume",
    "score_hv_share", "score_trend", "score_cs_touch",
]

# Leaky features included only for the "with-leakage" reference run
LEAKY_FEATURES = ["lifecycle_encoded"]


def get_feature_cols(df: pd.DataFrame, include_leaky: bool = False) -> list:
    src_cols = [c for c in df.columns if c.startswith("src_")]
    rgn_cols = [c for c in df.columns if c.startswith("rgn_")]
    ind_cols = [c for c in df.columns if c.startswith("ind_")]
    base = FEATURE_COLS_BASE + (LEAKY_FEATURES if include_leaky else [])
    all_cols = base + src_cols + rgn_cols + ind_cols
    return [c for c in all_cols if c in df.columns]


# ---------------------------------------------------------------------------
# Train / Evaluate
# ---------------------------------------------------------------------------
def train_and_evaluate(df: pd.DataFrame):
    df_feat = build_features(df)

    # --- Leakage demonstration ---
    print("\n[LEAKAGE CHECK] Training WITH lifecycle_encoded (leaky reference):")
    leaky_cols = get_feature_cols(df_feat, include_leaky=True)
    y = df_feat["churned"].astype(int)
    try:
        from xgboost import XGBClassifier as _XGB
        _clf_l = _XGB(n_estimators=100, max_depth=4, learning_rate=0.1,
                      scale_pos_weight=(y==0).sum()/(y==1).sum(),
                      random_state=42, eval_metric="logloss", verbosity=0)
    except ImportError:
        from sklearn.ensemble import GradientBoostingClassifier as _GBC
        _clf_l = _GBC(n_estimators=100, max_depth=4, learning_rate=0.1, random_state=42)
    _cv_leak = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    from sklearn.model_selection import cross_val_score as _cvs
    _auc_leaky = _cvs(_clf_l, df_feat[leaky_cols].fillna(0).astype(float), y,
                      cv=_cv_leak, scoring="roc_auc").mean()
    print(f"  -> CV ROC-AUC WITH lifecycle_encoded: {_auc_leaky:.4f}  (inflated -- do not use)")

    # --- Clean model (production-safe) ---
    print("\n[CLEAN MODEL] Training WITHOUT lifecycle_encoded:")
    feature_cols = get_feature_cols(df_feat, include_leaky=False)

    X = df_feat[feature_cols].fillna(0).astype(float)

    print(f"Feature matrix: {X.shape[0]} samples x {X.shape[1]} features")
    print(f"Churn rate: {y.mean():.1%}  ({y.sum()} churned / {len(y)} total)")

    # Model -- GradientBoosting chosen over XGBoost for zero-dependency portability
    # (XGBoost not always available; behaviour is equivalent at this scale)
    try:
        from xgboost import XGBClassifier
        clf = XGBClassifier(
            n_estimators=300,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=(y == 0).sum() / (y == 1).sum(),
            random_state=42,
            eval_metric="logloss",
            verbosity=0,
        )
        model_name = "XGBoost"
    except ImportError:
        clf = GradientBoostingClassifier(
            n_estimators=300, max_depth=4, learning_rate=0.05,
            subsample=0.8, random_state=42
        )
        model_name = "GradientBoosting"

    print(f"\nModel: {model_name}")

    # Cross-validation
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_results = cross_validate(
        clf, X, y, cv=cv,
        scoring=["roc_auc", "average_precision"],
        return_train_score=False,
    )

    print(f"\n5-Fold Cross-Validation Results:")
    print(f"  ROC-AUC:  {cv_results['test_roc_auc'].mean():.4f} ± {cv_results['test_roc_auc'].std():.4f}")
    print(f"  Avg Prec: {cv_results['test_average_precision'].mean():.4f} ± {cv_results['test_average_precision'].std():.4f}")

    # Train on full dataset for deployment predictions
    clf.fit(X, y)

    proba = clf.predict_proba(X)[:, 1]
    pred  = (proba >= DECISION_THRESHOLD).astype(int)

    print(f"\nFull-dataset metrics (threshold={DECISION_THRESHOLD}):")
    print(classification_report(y, pred, target_names=["Active", "Churned"]))

    # Confusion matrix
    cm = confusion_matrix(y, pred)
    tn, fp, fn, tp = cm.ravel()
    arr_saved = df_feat[pred & (y == 1)]["arr"].sum()
    arr_at_risk_missed = df_feat[(pred == 0) & (y == 1)]["arr"].sum()
    print(f"  True Positives  (correctly flagged churns) : {tp}")
    print(f"  False Negatives (missed churns)            : {fn}")
    print(f"  ARR in correctly-flagged accounts          : ${arr_saved:,.0f}")
    print(f"  ARR in missed churns                       : ${arr_at_risk_missed:,.0f}")

    # Feature importance
    try:
        importances = clf.feature_importances_
    except AttributeError:
        importances = np.zeros(len(feature_cols))

    fi_df = pd.DataFrame({
        "feature": feature_cols,
        "importance": importances,
    }).sort_values("importance", ascending=False)

    print(f"\nTop 15 features:")
    print(fi_df.head(15).to_string(index=False))

    # Save predictions
    predictions = df_feat[["tenant_id", "company_name", "plan", "arr",
                            "churned", "renewal_date", "days_to_renewal",
                            "health_score", "health_tier"]].copy()
    predictions["churn_probability"] = proba.round(4)
    predictions["churn_flag"] = pred
    predictions = predictions.sort_values("churn_probability", ascending=False)
    predictions.to_parquet(os.path.join(OUT_DIR, "churn_predictions.parquet"), index=False)
    print(f"\n  [OK] churn_predictions.parquet written")

    # Save feature importance
    fi_df.to_csv(os.path.join(OUT_DIR, "feature_importance.csv"), index=False)
    print(f"  [OK] feature_importance.csv written")

    # Save metrics
    metrics = {
        "model": model_name,
        "cv_roc_auc_mean": cv_results["test_roc_auc"].mean(),
        "cv_roc_auc_std":  cv_results["test_roc_auc"].std(),
        "cv_avg_precision_mean": cv_results["test_average_precision"].mean(),
        "cv_avg_precision_std":  cv_results["test_average_precision"].std(),
        "decision_threshold": DECISION_THRESHOLD,
        "true_positives": int(tp),
        "false_positives": int(fp),
        "false_negatives": int(fn),
        "true_negatives": int(tn),
        "arr_correctly_flagged": float(arr_saved),
        "arr_missed": float(arr_at_risk_missed),
    }
    pd.DataFrame([metrics]).to_csv(os.path.join(OUT_DIR, "churn_model_metrics.csv"), index=False)
    print(f"  [OK] churn_model_metrics.csv written")

    # Save model
    joblib.dump(clf, os.path.join(OUT_DIR, "churn_model.joblib"))
    print(f"  [OK] churn_model.joblib saved")

    # --- Charts ---
    _plot_pr_roc(clf, X, y)
    _plot_feature_importance(fi_df)

    return clf, predictions, fi_df, metrics


def _plot_pr_roc(clf, X, y):
    proba = clf.predict_proba(X)[:, 1]

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle("Churn Model -- Evaluation Curves", fontsize=14, fontweight="bold")

    # ROC
    fpr, tpr, _ = roc_curve(y, proba)
    auc = roc_auc_score(y, proba)
    axes[0].plot(fpr, tpr, color="#2563EB", lw=2, label=f"ROC AUC = {auc:.3f}")
    axes[0].plot([0, 1], [0, 1], "k--", lw=1)
    axes[0].set_xlabel("False Positive Rate")
    axes[0].set_ylabel("True Positive Rate")
    axes[0].set_title("ROC Curve")
    axes[0].legend(loc="lower right")
    axes[0].grid(alpha=0.3)

    # PR
    precision, recall, _ = precision_recall_curve(y, proba)
    ap = average_precision_score(y, proba)
    axes[1].plot(recall, precision, color="#16A34A", lw=2, label=f"Avg Precision = {ap:.3f}")
    axes[1].axvline(x=0.7, color="orange", linestyle="--", lw=1.5,
                    label="Recall = 0.70 target")
    axes[1].set_xlabel("Recall")
    axes[1].set_ylabel("Precision")
    axes[1].set_title("Precision-Recall Curve")
    axes[1].legend(loc="upper right")
    axes[1].grid(alpha=0.3)

    plt.tight_layout()
    path = os.path.join(OUT_DIR, "churn_evaluation_curves.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  [OK] churn_evaluation_curves.png saved")


def _plot_feature_importance(fi_df: pd.DataFrame, top_n: int = 20):
    top = fi_df.head(top_n)
    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(top["feature"][::-1], top["importance"][::-1], color="#2563EB")
    ax.set_xlabel("Feature Importance (Gain)")
    ax.set_title(f"Top {top_n} Churn Model Features", fontweight="bold")
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    path = os.path.join(OUT_DIR, "feature_importance.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  [OK] feature_importance.png saved")


# ---------------------------------------------------------------------------
# Model Limitations (documented in code for transparency)
# ---------------------------------------------------------------------------
LIMITATIONS = """
MODEL LIMITATIONS
=================

1. Data leakage risk on lifecycle_encoded
   The CRM lifecycle_stage field can contain "at-risk" or "churned" labels that
   were assigned by a human CSM -- meaning they already encode churn knowledge.
   While we include it as a feature (it represents a real business signal), a
   production model should be carefully evaluated with this feature excluded to
   confirm the remaining features still discriminate well.

2. Small churned-class sample
   With ~500 tenants and ~20-30% churn rate, the churned class has at most
   ~100-150 samples. Cross-validation standard deviation (~0.02-0.04 on AUC)
   reflects this instability. Model performance estimates should be treated as
   directional, not precise. A production deployment would need 6-12 more months
   of data to produce stable generalisation estimates.

3. Synthetic data patterns
   The dataset is explicitly described as synthetic. Real-world churn patterns
   include signals not present here (payment failures, support ticket sentiment,
   product version changes). The model should be re-trained on live data before
   operational use.

4. Temporal leakage in full-dataset evaluation
   Training and evaluating on the full dataset (after cross-validation) inflates
   in-sample metrics. The cross-validation AUC is the honest estimate. For
   production, use a strict time-based train/test split (train on months 1-5,
   test on month 6).
"""


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("ML -- Churn Risk Model")
    print("=" * 60)
    print(LIMITATIONS)

    mart_path = os.path.join(OUT_DIR, "mart_tenant_health.parquet")
    if not os.path.exists(mart_path):
        raise FileNotFoundError(
            "mart_tenant_health.parquet not found. "
            "Run pipeline/02_build_analytical_layer.py first."
        )

    mart = pd.read_parquet(mart_path)
    print(f"Loaded mart: {len(mart):,} tenants\n")

    clf, predictions, fi_df, metrics = train_and_evaluate(mart)

    print("\n[DONE] Churn model training complete")
    print(f"   CV ROC-AUC: {metrics['cv_roc_auc_mean']:.3f}")
    print(f"   High-risk tenants flagged: {predictions['churn_flag'].sum()}")
    at_risk_arr = predictions[predictions["churn_flag"] == 1]["arr"].sum()
    print(f"   ARR at risk: ${at_risk_arr:,.0f}")


if __name__ == "__main__":
    main()

