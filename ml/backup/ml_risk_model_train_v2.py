from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import joblib
import pandas as pd
import pymysql
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

FEATURE_SCHEMA_VERSION = "v2"
FEATURE_COLS = [
    "daily_active_users","page_view_count","avg_session_duration_sec","new_user_ratio",
    "auth_attempt_count","auth_success_count","auth_fail_count","auth_success_rate","auth_fail_rate",
    "otp_request_count","risk_login_count","loan_view_count","loan_apply_start_count",
    "loan_apply_submit_count","loan_funnel_conversion","card_apply_start_count",
    "card_apply_submit_count","card_apply_submit_rate","card_funnel_conversion",
    "submit_capture_rate","success_outcome_capture_rate","collector_event_count","raw_event_count",
    "estimated_missing_rate","mapping_coverage","validation_fail_count","validation_warn_count",
    "drift_alert_count","drift_warn_count","anomaly_alert_count","anomaly_warn_count",
    "ml_feature_alert_count","ml_feature_warn_count","total_signal_count",
]


def connect_mysql(host: str, port: int, user: str, password: str, db: str):
    return pymysql.connect(host=host, port=port, user=user, password=password, database=db, charset="utf8mb4")


def load_df(conn, profile_id: str, dt_from: str, dt_to: str) -> pd.DataFrame:
    sql = """
    SELECT *
    FROM ml_feature_vector_day
    WHERE profile_id = %s AND dt BETWEEN %s AND %s
    ORDER BY dt
    """
    return pd.read_sql(sql, conn, params=[profile_id, dt_from, dt_to])


def make_binary_label(df: pd.DataFrame, threshold: float) -> pd.Series:
    if "target_risk_label" in df.columns and df["target_risk_label"].notna().any():
        return pd.to_numeric(df["target_risk_label"], errors="coerce").fillna(0).astype(int)
    if "target_risk_grade" in df.columns and df["target_risk_grade"].notna().any():
        return (df["target_risk_grade"].astype(str).str.lower() == "high").astype(int)
    if "target_risk_score" in df.columns and df["target_risk_score"].notna().any():
        return (pd.to_numeric(df["target_risk_score"], errors="coerce").fillna(0) >= threshold).astype(int)
    raise ValueError("No target label columns available")


def main() -> None:
    ap = argparse.ArgumentParser(description="Train standardized ML risk classifier v2")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--risk-threshold", type=float, default=0.70)
    ap.add_argument("--model-version", default="ml_risk_v2")
    ap.add_argument("--model-out", default="ml_risk_model_v2.joblib")
    ap.add_argument("--report-out", default="ml_risk_model_report_v2.json")
    ap.add_argument("--importance-out", default="ml_feature_importance_v2.csv")
    args = ap.parse_args()

    conn = connect_mysql(args.host, args.port, args.user, args.password, args.db)
    try:
        df = load_df(conn, args.profile_id, args.dt_from, args.dt_to)
    finally:
        conn.close()

    if df.empty:
        raise SystemExit("No training rows found in ml_feature_vector_day")

    feature_cols_requested = FEATURE_COLS.copy()
    feature_cols_used = [c for c in FEATURE_COLS if c in df.columns]
    missing_cols = [c for c in FEATURE_COLS if c not in df.columns]

    for c in missing_cols:
        df[c] = 0
    feature_cols_used = FEATURE_COLS.copy()

    df[feature_cols_used] = df[feature_cols_used].apply(pd.to_numeric, errors="coerce")
    y = make_binary_label(df, args.risk_threshold)
    X = df[feature_cols_used].copy()

    stratify = y if y.nunique() > 1 and y.value_counts().min() >= 2 else None
    test_size = 0.2 if len(df) >= 10 else 0.33
    if len(df) >= 4:
        X_train, X_test, y_train, y_test, train_idx, test_idx = train_test_split(
            X, y, df.index, test_size=test_size, shuffle=True, random_state=42, stratify=stratify
        )
    else:
        X_train, y_train = X, y
        X_test, y_test = X.iloc[0:0], y.iloc[0:0]
        train_idx, test_idx = df.index, df.index[0:0]

    preprocessor = ColumnTransformer([
        ("num", Pipeline([
            ("imputer", SimpleImputer(strategy="constant", fill_value=0)),
            ("scaler", StandardScaler()),
        ]), feature_cols_used)
    ])

    model = Pipeline([
        ("preprocessor", preprocessor),
        ("clf", LogisticRegression(max_iter=2000, class_weight="balanced", solver="lbfgs")),
    ])
    model.fit(X_train, y_train)

    classes = list(model.named_steps["clf"].classes_)
    coef = model.named_steps["clf"].coef_[0]
    coef_df = pd.DataFrame({
        "feature_name": feature_cols_used,
        "coefficient": coef,
        "abs_coefficient": [abs(x) for x in coef],
    }).sort_values("abs_coefficient", ascending=False)

    report = {
        "profile_id": args.profile_id,
        "model_version": args.model_version,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "trained_at": datetime.now().isoformat(),
        "train_start_dt": args.dt_from,
        "train_end_dt": args.dt_to,
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "feature_cols_requested": feature_cols_requested,
        "feature_cols_used": feature_cols_used,
        "feature_cols_missing_filled_as_zero": missing_cols,
        "label_rule": f"target_risk_label if available else target_risk_grade='high' else target_risk_score >= {args.risk_threshold}",
        "train_label_distribution": pd.Series(y_train).value_counts(dropna=False).to_dict(),
        "test_label_distribution": pd.Series(y_test).value_counts(dropna=False).to_dict() if len(y_test) else {},
        "classes": classes,
        "top_coefficients": coef_df.head(15).to_dict(orient="records"),
    }

    if len(X_test):
        y_pred = model.predict(X_test)
        report["accuracy"] = float(accuracy_score(y_test, y_pred))
        report["classification_report"] = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
        report["confusion_matrix"] = confusion_matrix(y_test, y_pred, labels=classes).tolist()
        try:
            pi = permutation_importance(model, X_test, y_test, n_repeats=10, random_state=42)
            perm_df = pd.DataFrame({
                "feature_name": feature_cols_used,
                "importance_mean": pi.importances_mean,
                "importance_std": pi.importances_std,
            }).sort_values("importance_mean", ascending=False)
        except Exception:
            perm_df = pd.DataFrame(columns=["feature_name","importance_mean","importance_std"])
        report["top_permutation_importance"] = perm_df.head(15).to_dict(orient="records")
    else:
        perm_df = pd.DataFrame(columns=["feature_name","importance_mean","importance_std"])

    artifact = {
        "model": model,
        "feature_columns": feature_cols_used,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "model_version": args.model_version,
        "trained_at": report["trained_at"],
        "label_rule": report["label_rule"],
    }
    joblib.dump(artifact, args.model_out)
    Path(args.report_out).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    out_df = coef_df.merge(perm_df, on="feature_name", how="left") if not perm_df.empty else coef_df.assign(importance_mean=None, importance_std=None)
    out_df["model_version"] = args.model_version
    out_df["feature_schema_version"] = FEATURE_SCHEMA_VERSION
    out_df.to_csv(args.importance_out, index=False)
    print(f"[OK] model trained: rows={len(df)} model_out={args.model_out} report_out={args.report_out} importance_out={args.importance_out}")


if __name__ == "__main__":
    main()
