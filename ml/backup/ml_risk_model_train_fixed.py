from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pymysql
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# Preferred features. Missing columns will be auto-created as 0.
FEATURE_COLS = [
    "daily_active_users",
    "page_view_count",
    "avg_session_duration_sec",
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
    "total_signal_count",
]


def connect_mysql(host: str, port: int, user: str, password: str, db: str):
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db,
        charset="utf8mb4",
    )


def load_df(conn, profile_id: str, dt_from: str, dt_to: str) -> pd.DataFrame:
    sql = """
    SELECT *
    FROM ml_feature_vector_day
    WHERE profile_id = %s
      AND dt BETWEEN %s AND %s
    ORDER BY dt
    """
    return pd.read_sql(sql, conn, params=[profile_id, dt_from, dt_to])


def make_binary_label(df: pd.DataFrame, threshold: float) -> pd.Series:
    if "target_risk_status" in df.columns and df["target_risk_status"].notna().any():
        return (df["target_risk_status"].astype(str).str.lower() == "alert").astype(int)
    if "target_risk_score" in df.columns and df["target_risk_score"].notna().any():
        return (pd.to_numeric(df["target_risk_score"], errors="coerce").fillna(0) >= threshold).astype(int)
    raise ValueError("No target_risk_status or target_risk_score available for label generation")


def prepare_feature_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str], list[str]]:
    available = []
    missing = []
    work = df.copy()

    for col in FEATURE_COLS:
        if col in work.columns:
            available.append(col)
        else:
            work[col] = 0
            available.append(col)
            missing.append(col)

    work[available] = work[available].apply(pd.to_numeric, errors="coerce")
    work[available] = work[available].fillna(0)
    return work, available, missing


def json_default(obj):
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if pd.isna(obj):
        return None
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def main() -> None:
    ap = argparse.ArgumentParser(description="Train binary ML risk classifier with explainability output")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--alert-threshold", type=float, default=1000.0)
    ap.add_argument("--model-out", default="ml_risk_model.joblib")
    ap.add_argument("--report-out", default="ml_risk_model_report.json")
    ap.add_argument("--importance-out", default="ml_feature_importance.csv")
    args = ap.parse_args()

    conn = connect_mysql(args.host, args.port, args.user, args.password, args.db)
    try:
        df = load_df(conn, args.profile_id, args.dt_from, args.dt_to)
    finally:
        conn.close()

    if df.empty:
        raise SystemExit("No training rows found in ml_feature_vector_day")

    df["label"] = make_binary_label(df, args.alert_threshold)
    df, available_features, missing_features = prepare_feature_frame(df)

    # Guardrail: need at least 2 classes
    label_counts = df["label"].value_counts(dropna=False).to_dict()
    if len(label_counts) < 2:
        raise SystemExit(f"Training labels have only one class: {label_counts}. Tune thresholds or widen the period first.")

    split_idx = max(int(len(df) * 0.8), 1)
    train_df = df.iloc[:split_idx].copy()
    test_df = df.iloc[split_idx:].copy() if len(df) > 1 else df.iloc[:0].copy()

    X_train = train_df[available_features]
    y_train = train_df["label"]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", Pipeline(steps=[
                ("imputer", SimpleImputer(strategy="constant", fill_value=0)),
                ("scaler", StandardScaler()),
            ]), available_features)
        ]
    )

    model = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("clf", LogisticRegression(max_iter=2000, class_weight="balanced", solver="lbfgs")),
    ])
    model.fit(X_train, y_train)

    classes = list(model.named_steps["clf"].classes_)
    coef = model.named_steps["clf"].coef_[0]
    coef_df = pd.DataFrame({
        "feature_name": available_features,
        "coefficient": coef,
        "abs_coefficient": [abs(x) for x in coef],
    }).sort_values("abs_coefficient", ascending=False)

    report = {
        "profile_id": args.profile_id,
        "trained_at": datetime.now().isoformat(),
        "train_rows": int(len(train_df)),
        "test_rows": int(len(test_df)),
        "feature_cols_requested": FEATURE_COLS,
        "feature_cols_used": available_features,
        "feature_cols_missing_filled_as_zero": missing_features,
        "label_rule": f"alert if target_risk_status='alert' else fallback target_risk_score >= {args.alert_threshold}",
        "train_label_distribution": train_df["label"].value_counts(dropna=False).to_dict(),
        "test_label_distribution": test_df["label"].value_counts(dropna=False).to_dict() if not test_df.empty else {},
        "classes": classes,
        "top_coefficients": coef_df.head(15).to_dict(orient="records"),
    }

    if not test_df.empty:
        X_test = test_df[available_features]
        y_test = test_df["label"]
        y_pred = model.predict(X_test)
        report["accuracy"] = float(accuracy_score(y_test, y_pred))
        report["classification_report"] = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
        report["confusion_matrix"] = confusion_matrix(y_test, y_pred, labels=classes).tolist()

        try:
            pi = permutation_importance(model, X_test, y_test, n_repeats=10, random_state=42)
            perm_df = pd.DataFrame({
                "feature_name": available_features,
                "importance_mean": pi.importances_mean,
                "importance_std": pi.importances_std,
            }).sort_values("importance_mean", ascending=False)
        except Exception:
            perm_df = pd.DataFrame(columns=["feature_name", "importance_mean", "importance_std"])

        report["top_permutation_importance"] = perm_df.head(15).to_dict(orient="records")
    else:
        perm_df = pd.DataFrame(columns=["feature_name", "importance_mean", "importance_std"])

    joblib.dump(model, args.model_out)

    Path(args.report_out).write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=json_default),
        encoding="utf-8",
    )

    if perm_df.empty:
        out_df = coef_df[["feature_name", "coefficient", "abs_coefficient"]].copy()
        out_df["importance_mean"] = None
        out_df["importance_std"] = None
    else:
        out_df = coef_df.merge(perm_df, on="feature_name", how="left")

    out_df.to_csv(args.importance_out, index=False)

    print(
        f"[OK] model trained: rows={len(df)} "
        f"model_out={args.model_out} "
        f"report_out={args.report_out} "
        f"importance_out={args.importance_out}"
    )

    if missing_features:
        print(f"[INFO] missing feature columns were filled with 0: {', '.join(missing_features)}")


if __name__ == "__main__":
    main()
