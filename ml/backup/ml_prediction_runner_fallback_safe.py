#!/usr/bin/env python3
import argparse
import os

import joblib
import pandas as pd
import pymysql


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


def label_to_status(label: int) -> str:
    return {0: "normal", 1: "warning", 2: "alert"}.get(int(label), "normal")


def safe_float(val, fallback=0.0):
    if val is None:
        return fallback
    s = str(val).strip()
    if s in ("", "None", "nan", "NaN", "NULL", "null", "target_risk_score"):
        return fallback
    try:
        return float(s)
    except Exception:
        return fallback


def fallback_predict(row):
    score = safe_float(row.get("target_risk_score"), 0.0)
    if score >= 0.70:
        return 2, "alert", 0.02, 0.08, 0.90, "fallback: target_risk_score high"
    if score >= 0.40:
        return 1, "warning", 0.15, 0.70, 0.15, "fallback: target_risk_score medium"
    return 0, "normal", 0.90, 0.08, 0.02, "fallback: target_risk_score low"


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
    ap.add_argument("--model-path", default="ml_risk_model_safe.joblib")
    ap.add_argument("--model-version", default="ml_risk_safe_v1")
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    bundle = joblib.load(args.model_path)
    mode = bundle.get("mode", "supervised")
    pipe = bundle.get("pipeline")
    feature_columns = bundle.get("feature_columns", [])
    model_name = bundle.get("model_name", "ml_risk_model")

    df = fetch_df(args)
    if df.empty:
        raise SystemExit("No rows found in ml_feature_vector_day")

    conn = pymysql.connect(
        host=args.host, port=args.port, user=args.user, password=args.password,
        database=args.db, autocommit=False, cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with conn.cursor() as cur:
            if args.truncate:
                cur.execute(
                    "DELETE FROM ml_prediction_result WHERE profile_id=%s AND dt BETWEEN %s AND %s AND model_version=%s",
                    (args.profile_id, args.dt_from, args.dt_to, args.model_version),
                )

            insert_sql = """
            REPLACE INTO ml_prediction_result (
                profile_id, dt, model_name, model_version,
                predicted_label, predicted_risk_status,
                prob_normal, prob_warning, prob_alert,
                actual_risk_status, actual_risk_score,
                run_id, note
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s
            )
            """

            if mode == "supervised" and pipe is not None:
                X = df[feature_columns].copy()
                for c in feature_columns:
                    X[c] = pd.to_numeric(X[c], errors="coerce").fillna(0.0)

                probs = pipe.predict_proba(X)
                preds = pipe.predict(X)

                for i, row in df.iterrows():
                    p = probs[i]
                    prob_normal = float(p[0]) if len(p) > 0 else 0.0
                    prob_warning = float(p[1]) if len(p) > 1 else 0.0
                    prob_alert = float(p[2]) if len(p) > 2 else 0.0
                    pred_label = int(preds[i])
                    pred_status = label_to_status(pred_label)
                    note = "supervised prediction"
                    run_id = f"mlpred_{row['profile_id']}_{str(row['dt']).replace('-', '')}_{args.model_version}"

                    cur.execute(
                        insert_sql,
                        (
                            row["profile_id"], row["dt"], model_name, args.model_version,
                            pred_label, pred_status,
                            prob_normal, prob_warning, prob_alert,
                            row.get("target_risk_status"), safe_float(row.get("target_risk_score"), 0.0),
                            run_id, note,
                        ),
                    )
            else:
                for _, row in df.iterrows():
                    pred_label, pred_status, prob_normal, prob_warning, prob_alert, note = fallback_predict(row)
                    run_id = f"mlpred_{row['profile_id']}_{str(row['dt']).replace('-', '')}_{args.model_version}"

                    cur.execute(
                        insert_sql,
                        (
                            row["profile_id"], row["dt"], model_name, args.model_version,
                            pred_label, pred_status,
                            prob_normal, prob_warning, prob_alert,
                            row.get("target_risk_status"), safe_float(row.get("target_risk_score"), 0.0),
                            run_id, note,
                        ),
                    )

            conn.commit()
            print(f"[OK] prediction completed: mode={mode}, rows={len(df)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
