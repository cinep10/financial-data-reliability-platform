#!/usr/bin/env python3
import argparse
import os

import joblib
import numpy as np
import pandas as pd
import pymysql


def label_to_status(label: int) -> str:
    return {0: "normal", 1: "warning", 2: "alert"}.get(int(label), "normal")


def fetch_df(args):
    conn = pymysql.connect(
        host=args.host, port=args.port, user=args.user, password=args.password,
        database=args.db, cursorclass=pymysql.cursors.DictCursor
    )
    try:
        return pd.read_sql(
            "SELECT * FROM ml_feature_vector_day WHERE profile_id=%s AND dt BETWEEN %s AND %s ORDER BY dt",
            conn,
            params=[args.profile_id, args.dt_from, args.dt_to],
        )
    finally:
        conn.close()


def make_note(model, feature_names, row_scaled):
    coef = model.coef_
    class_idx = int(np.argmax(model.predict_proba(row_scaled)[0]))
    contrib = row_scaled[0] * coef[class_idx]
    top_idx = np.argsort(np.abs(contrib))[::-1][:3]
    reasons = [f"{feature_names[i]}={contrib[i]:.4f}" for i in top_idx]
    return "top_reasons: " + ", ".join(reasons)


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
    pipe = bundle["pipeline"]
    feats = bundle["feature_columns"]
    model_name = bundle.get("model_name", "ml_risk_model")

    df = fetch_df(args)
    if df.empty:
        raise SystemExit("No rows found in ml_feature_vector_day")

    missing = [c for c in feats if c not in df.columns]
    if missing:
        raise ValueError(f"Missing feature columns: {missing}")

    X = df[feats].apply(pd.to_numeric, errors="coerce")
    probs = pipe.predict_proba(X)
    preds = pipe.predict(X)

    imputer = pipe.named_steps["imputer"]
    scaler = pipe.named_steps["scaler"]
    model = pipe.named_steps["model"]
    X_scaled = scaler.transform(imputer.transform(X))

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
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            for i, row in df.iterrows():
                p = probs[i]
                prob_normal = float(p[0]) if len(p) > 0 else 0.0
                prob_warning = float(p[1]) if len(p) > 1 else 0.0
                prob_alert = float(p[2]) if len(p) > 2 else 0.0
                pred_label = int(preds[i])
                pred_status = label_to_status(pred_label)
                run_id = f"mlpred_{row['profile_id']}_{str(row['dt']).replace('-', '')}_{args.model_version}"
                note = make_note(model, feats, X_scaled[i:i+1])

                cur.execute(
                    insert_sql,
                    (
                        row["profile_id"], row["dt"], model_name, args.model_version,
                        pred_label, pred_status,
                        prob_normal, prob_warning, prob_alert,
                        row.get("target_risk_status"), float(row.get("target_risk_score") or 0),
                        run_id, note,
                    ),
                )

            conn.commit()
            print(f"[OK] ml_prediction_runner_safe completed: rows={len(df)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
