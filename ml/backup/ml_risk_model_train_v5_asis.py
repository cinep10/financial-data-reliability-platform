#!/usr/bin/env python3
import argparse
import json
import os
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
import pymysql
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_recall_fscore_support
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


MODEL_NAME = "ml_risk_model"
FEATURE_COLUMNS = [
    "daily_active_users",
    "page_view_count",
    "avg_session_duration",
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
]


def fetch_df(args) -> pd.DataFrame:
    conn = pymysql.connect(
        host=args.host, port=args.port, user=args.user, password=args.password,
        database=args.db, cursorclass=pymysql.cursors.DictCursor
    )
    try:
        sql = """
        SELECT *
        FROM ml_feature_vector_day
        WHERE profile_id=%s
          AND dt BETWEEN %s AND %s
        ORDER BY dt
        """
        return pd.read_sql(sql, conn, params=[args.profile_id, args.dt_from, args.dt_to])
    finally:
        conn.close()


def map_status_to_label(status: str) -> int:
    status = (status or "").lower()
    if status == "alert":
        return 2
    if status == "warning":
        return 1
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.getenv("DB_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", "3306")))
    ap.add_argument("--user", default=os.getenv("DB_USER", "nethru"))
    ap.add_argument("--password", default=os.getenv("DB_PASSWORD", "nethru1234"))
    ap.add_argument("--db", default=os.getenv("DB_NAME", "weblog"))
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--model-path", default="ml_risk_model_v2.joblib")
    ap.add_argument("--report-path", default="ml_risk_model_report_v2.json")
    ap.add_argument("--importance-csv", default="ml_feature_importance_v2.csv")
    ap.add_argument("--model-version", default="ml_risk_v2")
    args = ap.parse_args()

    df = fetch_df(args)
    if df.empty:
        raise SystemExit("No rows found in ml_feature_vector_day")

    missing_cols = [c for c in FEATURE_COLUMNS if c not in df.columns]
    for c in missing_cols:
        df[c] = 0.0

    X = df[FEATURE_COLUMNS].apply(pd.to_numeric, errors="coerce")

    if "target_risk_status" in df.columns:
        y = df["target_risk_status"].map(map_status_to_label).fillna(0).astype(int)
        label_definition = "target_risk_status mapped to normal=0, warning=1, alert=2"
    else:
        score = pd.to_numeric(df.get("target_risk_score", 0), errors="coerce").fillna(0)
        y = pd.cut(score, bins=[-1, 0.40, 0.70, 999999], labels=[0, 1, 2]).astype(int)
        label_definition = "target_risk_score mapped with thresholds 0.40 / 0.70"

    split_idx = max(1, int(len(df) * 0.7))
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
        ("scaler", StandardScaler()),
        ("model", LogisticRegression(max_iter=2000, multi_class="auto", class_weight="balanced", random_state=42)),
    ])
    pipe.fit(X_train, y_train)

    metrics = {}
    if len(X_test) > 0:
        y_pred = pipe.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        precision, recall, f1, _ = precision_recall_fscore_support(
            y_test, y_pred, average="weighted", zero_division=0
        )
        metrics = {
            "accuracy": float(acc),
            "precision_weighted": float(precision),
            "recall_weighted": float(recall),
            "f1_weighted": float(f1),
        }

    bundle = {
        "model_name": MODEL_NAME,
        "model_version": args.model_version,
        "feature_columns": FEATURE_COLUMNS,
        "label_mapping": {0: "normal", 1: "warning", 2: "alert"},
        "pipeline": pipe,
        "trained_at": datetime.utcnow().isoformat() + "Z",
    }
    joblib.dump(bundle, args.model_path)

    model = pipe.named_steps["model"]
    coef = model.coef_
    abs_mean = np.mean(np.abs(coef), axis=0)
    imp_df = pd.DataFrame({
        "feature_name": FEATURE_COLUMNS,
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
        "train_start_dt": args.dt_from,
        "train_end_dt": args.dt_to,
        "feature_columns": FEATURE_COLUMNS,
        "missing_columns_filled_with_zero": missing_cols,
        "label_definition": label_definition,
        "row_count": int(len(df)),
        "train_row_count": int(len(X_train)),
        "test_row_count": int(len(X_test)),
        "metrics": metrics,
        "artifacts": {
            "model_path": args.model_path,
            "report_path": args.report_path,
            "importance_csv": args.importance_csv,
        },
    }
    with open(args.report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"[OK] model trained: version={args.model_version}, rows={len(df)}")


if __name__ == "__main__":
    main()
