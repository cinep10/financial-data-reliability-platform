#!/usr/bin/env python3
import argparse
import json
import os
from datetime import datetime, UTC

import joblib
import numpy as np
import pandas as pd
import pymysql
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, confusion_matrix
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

MODEL_NAME = "ml_risk_model"


def feature_columns(df_cols):
    candidates = [
        "daily_active_users",
        "page_view_count",
        "avg_session_duration",
        "avg_session_duration_sec",
        "avg_session_dura",
        "new_user_ratio",
        "auth_attempt_count",
        "auth_success_count",
        "auth_fail_count",
        "auth_success_rate",
        "auth_fail_rate",
        "otp_request_count",
        "risk_login_count",
        "loan_view_count",
        "loan_apply_start_count",
        "loan_apply_submit_count",
        "card_apply_start_count",
        "card_apply_submit_count",
        "card_apply_submit_rate",
        "collector_event_count",
        "raw_event_count",
        "estimated_missing_rate",
        "validation_fail_count",
        "validation_warn_count",
        "drift_alert_count",
        "drift_warn_count",
        "ml_feature_alert_count",
        "ml_feature_warn_count",
        "scenario_active_flag",
        "campaign_flag",
    ]
    return [c for c in candidates if c in df_cols]


def fetch_df(args):
    query = "SELECT * FROM ml_feature_vector_day WHERE profile_id=%s"
    params = [args.profile_id]

    if not args.train_all_history:
        query += " AND dt BETWEEN %s AND %s"
        params.extend([args.dt_from, args.dt_to])

    if args.train_mode == "baseline_only":
        query += " AND COALESCE(scenario_active_flag,0)=0"
    elif args.train_mode == "curated_history":
        query += """
          AND (
            COALESCE(scenario_active_flag,0)=0
            OR COALESCE(scenario_name,'') IN (
              'funnel_break',
              'auth_failure',
              'weather_drop',
              'mixed_incident',
              'campaign_spike'
            )
          )
        """

    query += " ORDER BY dt"

    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return pd.DataFrame(list(rows))
    finally:
        conn.close()


def coerce_feature_frame(df, feats):
    X = df[feats].copy()
    for c in feats:
        X[c] = pd.to_numeric(X[c], errors="coerce").fillna(0.0)
    return X


def save_fallback_bundle(args, feats, y, label_definition, reason):
    unique_classes = sorted(set(y.tolist()))
    bundle = {
        "model_name": MODEL_NAME,
        "model_version": args.model_version,
        "feature_columns": feats,
        "pipeline": None,
        "trained_at": datetime.now(UTC).isoformat(),
        "mode": "rule_fallback",
        "single_class_label": int(unique_classes[0]),
        "reason": reason,
    }
    joblib.dump(bundle, args.model_path)

    imp_df = pd.DataFrame({
        "feature_name": feats,
        "coefficient": 0.0,
        "abs_coefficient": 0.0,
        "importance_mean": 0.0,
        "importance_std": 0.0,
        "importance_rank": range(1, len(feats) + 1),
    })
    imp_df.to_csv(args.importance_csv, index=False)

    report = {
        "model_name": MODEL_NAME,
        "model_version": args.model_version,
        "mode": "rule_fallback",
        "reason": reason,
        "single_class_label": int(unique_classes[0]),
        "feature_columns": feats,
        "label_definition": label_definition,
        "train_all_history": args.train_all_history,
        "train_mode": args.train_mode,
        "train_start_dt": args.dt_from,
        "train_end_dt": args.dt_to,
        "row_count": int(len(y)),
        "class_distribution": {str(k): int(v) for k, v in y.value_counts().to_dict().items()},
    }
    with open(args.report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)


ap = argparse.ArgumentParser()
ap.add_argument("--host", default=os.getenv("DB_HOST", "127.0.0.1"))
ap.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", "3306")))
ap.add_argument("--user", default=os.getenv("DB_USER", "nethru"))
ap.add_argument("--password", default=os.getenv("DB_PASSWORD", "nethru1234"))
ap.add_argument("--db", default=os.getenv("DB_NAME", "weblog"))
ap.add_argument("--profile-id", required=True)
ap.add_argument("--dt-from", required=True)
ap.add_argument("--dt-to", required=True)
ap.add_argument("--model-path", default="ml_risk_model_safe.joblib")
ap.add_argument("--report-path", default="ml_risk_model_report_safe.json")
ap.add_argument("--importance-csv", default="ml_feature_importance_safe.csv")
ap.add_argument("--model-version", default="ml_risk_safe_v6")
ap.add_argument("--train-all-history", action="store_true")
ap.add_argument(
    "--train-mode",
    choices=["all_history", "baseline_only", "curated_history"],
    default=os.getenv("TRAIN_MODE", "curated_history"),
)
args = ap.parse_args()

df = fetch_df(args)
if df.empty:
    raise SystemExit("No rows found in ml_feature_vector_day")

feats = feature_columns(df.columns)
if not feats:
    raise SystemExit("No usable feature columns found")

X = coerce_feature_frame(df, feats)

if "target_risk_label" not in df.columns:
    raise SystemExit("target_risk_label column is required")

y = pd.to_numeric(df["target_risk_label"], errors="coerce").fillna(0).astype(int)
label_definition = "scenario-aware label from ml_feature_vector_day.target_risk_label"

class_counts = y.value_counts().sort_index()
unique_classes = sorted(set(y.tolist()))

if len(unique_classes) < 2:
    save_fallback_bundle(args, feats, y, label_definition, "single class only")
    print(f"[OK] ml_risk_model_train_v6 fallback completed: single_class={unique_classes[0]}, rows={len(df)}")
    raise SystemExit(0)

if len(df) < 15:
    save_fallback_bundle(args, feats, y, label_definition, "insufficient rows for stable supervised train")
    print(f"[OK] ml_risk_model_train_v6 fallback completed: reason=insufficient_rows, rows={len(df)}")
    raise SystemExit(0)

split_idx = max(10, int(len(df) * 0.7))
split_idx = min(split_idx, len(df) - 1)

X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

train_classes = sorted(set(y_train.tolist()))
if len(train_classes) < 2:
    save_fallback_bundle(args, feats, y, label_definition, "train split single class only")
    print(f"[OK] ml_risk_model_train_v6 fallback completed: reason=train_split_single_class, rows={len(df)}")
    raise SystemExit(0)

pipe = Pipeline([
    ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
    ("scaler", StandardScaler()),
    ("model", LogisticRegression(max_iter=3000, class_weight="balanced", random_state=42)),
])

pipe.fit(X_train, y_train)

y_pred = pipe.predict(X_test)
acc = accuracy_score(y_test, y_pred)
precision, recall, f1, _ = precision_recall_fscore_support(
    y_test,
    y_pred,
    average="weighted",
    zero_division=0,
)
cm = confusion_matrix(y_test, y_pred, labels=[0, 1, 2])

bundle = {
    "model_name": MODEL_NAME,
    "model_version": args.model_version,
    "feature_columns": feats,
    "pipeline": pipe,
    "trained_at": datetime.now(UTC).isoformat(),
    "mode": "supervised",
    "train_all_history": args.train_all_history,
    "train_mode": args.train_mode,
}
joblib.dump(bundle, args.model_path)

model = pipe.named_steps["model"]
coef = model.coef_
abs_mean = np.mean(np.abs(coef), axis=0)

imp_df = pd.DataFrame({
    "feature_name": feats,
    "coefficient": np.mean(coef, axis=0),
    "abs_coefficient": abs_mean,
    "importance_mean": abs_mean,
    "importance_std": np.std(np.abs(coef), axis=0),
}).sort_values("importance_mean", ascending=False).reset_index(drop=True)

imp_df["importance_rank"] = imp_df.index + 1
imp_df.to_csv(args.importance_csv, index=False)

report = {
    "model_name": MODEL_NAME,
    "model_version": args.model_version,
    "mode": "supervised",
    "feature_columns": feats,
    "label_definition": label_definition,
    "train_all_history": args.train_all_history,
    "train_mode": args.train_mode,
    "train_start_dt": args.dt_from,
    "train_end_dt": args.dt_to,
    "row_count": int(len(df)),
    "train_row_count": int(len(X_train)),
    "test_row_count": int(len(X_test)),
    "class_distribution": {str(k): int(v) for k, v in class_counts.to_dict().items()},
    "metrics": {
        "accuracy": float(acc),
        "precision_weighted": float(precision),
        "recall_weighted": float(recall),
        "f1_weighted": float(f1),
        "confusion_matrix_labels": [0, 1, 2],
        "confusion_matrix": cm.tolist(),
    },
}

with open(args.report_path, "w", encoding="utf-8") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)

print(
    f"[OK] ml_risk_model_train_v6 supervised completed: "
    f"rows={len(df)}, train_rows={len(X_train)}, test_rows={len(X_test)}, "
    f"features={len(feats)}, train_mode={args.train_mode}"
)
