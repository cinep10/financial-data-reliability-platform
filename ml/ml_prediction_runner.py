#!/usr/bin/env python3
import argparse
import os
from datetime import datetime

import joblib
import pandas as pd
import pymysql


def safe_float(v, fallback=0.0):
    try:
        if v is None:
            return fallback
        return float(v)
    except Exception:
        return fallback


def normalize_dt(v):
    try:
        x = pd.to_datetime(v, errors="coerce")
        if pd.isna(x):
            return None
        return x.strftime("%Y-%m-%d")
    except Exception:
        return None


def feature_frame(df, feats):
    X = df[feats].copy()
    for c in feats:
        X[c] = pd.to_numeric(X[c], errors="coerce").fillna(0.0)
    return X


def fallback_predict(row):
    score = safe_float(row.get("target_risk_score"), 0.0)

    # builder/train과 맞춘 fallback 기준
    if score >= 0.80:
        return 2, "alert", 0.05, 0.15, 0.80, "fallback: calibrated high score"
    if score >= 0.45:
        return 1, "warning", 0.20, 0.60, 0.20, "fallback: calibrated medium score"
    return 0, "normal", 0.85, 0.12, 0.03, "fallback: calibrated low score"


def calibrated_status(prob_normal, prob_warning, prob_alert):
    """
    현재 프로젝트 분포 기준 calibrated threshold
    - alert는 보수적으로
    - warning을 충분히 살려줌
    """
    if prob_alert >= 0.80:
        return 2, "alert"

    if prob_alert >= 0.45 or prob_warning >= 0.45:
        return 1, "warning"

    return 0, "normal"


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
    ap.add_argument("--model-version", default="ml_risk_safe_v6")
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    bundle = joblib.load(args.model_path)
    mode = bundle.get("mode", "rule_fallback")
    pipe = bundle.get("pipeline")
    feats = bundle.get("feature_columns", [])
    model_name = bundle.get("model_name", "ml_risk_model")

    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM ml_feature_vector_day
                WHERE profile_id=%s
                  AND dt BETWEEN %s AND %s
                ORDER BY dt
                """,
                (args.profile_id, args.dt_from, args.dt_to),
            )
            rows = cur.fetchall()

        df = pd.DataFrame(list(rows))
        if df.empty:
            raise SystemExit("No rows found in ml_feature_vector_day")

        print(f"[DEBUG] fetched rows: {len(df)}")
        if "dt" in df.columns:
            print(f"[DEBUG] fetched dt values: {df['dt'].tolist()[:10]}")

        df["dt_norm"] = df["dt"].apply(normalize_dt)
        bad_dt = df["dt_norm"].isna().sum()
        if bad_dt > 0:
            print(f"[WARN] dropped rows with invalid dt: {bad_dt}")
            df = df[df["dt_norm"].notna()].copy()

        if df.empty:
            raise SystemExit("No valid dt rows after normalization")

        with conn.cursor() as cur:
            if args.truncate:
                cur.execute(
                    """
                    DELETE FROM ml_prediction_result
                    WHERE profile_id=%s
                      AND dt BETWEEN %s AND %s
                      AND model_version=%s
                    """,
                    (args.profile_id, args.dt_from, args.dt_to, args.model_version),
                )

            insert_sql = """
                REPLACE INTO ml_prediction_result
                (
                    profile_id,
                    dt,
                    model_name,
                    model_version,
                    predicted_label,
                    predicted_risk_status,
                    prob_normal,
                    prob_warning,
                    prob_alert,
                    actual_risk_status,
                    actual_risk_score,
                    run_id,
                    note
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """

            inserted = 0

            if mode == "supervised" and pipe is not None and feats:
                X = feature_frame(df, feats)
                probs = pipe.predict_proba(X)

                class_order = list(pipe.named_steps["model"].classes_) if hasattr(pipe.named_steps["model"], "classes_") else [0, 1, 2]

                for i, row in df.iterrows():
                    dt = row["dt_norm"]
                    if not dt:
                        continue

                    prob_map = {0: 0.0, 1: 0.0, 2: 0.0}
                    for cls_idx, cls_val in enumerate(class_order):
                        prob_map[int(cls_val)] = float(probs[i][cls_idx])

                    prob_normal = prob_map[0]
                    prob_warning = prob_map[1]
                    prob_alert = prob_map[2]

                    pred_label, pred_status = calibrated_status(
                        prob_normal=prob_normal,
                        prob_warning=prob_warning,
                        prob_alert=prob_alert,
                    )

                    note = "supervised prediction with calibrated thresholds"

                    cur.execute(
                        insert_sql,
                        (
                            row["profile_id"],
                            dt,
                            model_name,
                            args.model_version,
                            pred_label,
                            pred_status,
                            prob_normal,
                            prob_warning,
                            prob_alert,
                            row.get("target_risk_status"),
                            safe_float(row.get("target_risk_score"), 0.0),
                            f"mlpred_{row['profile_id']}_{dt.replace('-', '')}_{args.model_version}",
                            note,
                        ),
                    )
                    inserted += 1

                print(f"[INFO] supervised rows inserted: {inserted}")

            else:
                for _, row in df.iterrows():
                    dt = row["dt_norm"]
                    if not dt:
                        continue

                    pred_label, pred_status, prob_normal, prob_warning, prob_alert, note = fallback_predict(row)

                    cur.execute(
                        insert_sql,
                        (
                            row["profile_id"],
                            dt,
                            model_name,
                            args.model_version,
                            pred_label,
                            pred_status,
                            prob_normal,
                            prob_warning,
                            prob_alert,
                            row.get("target_risk_status"),
                            safe_float(row.get("target_risk_score"), 0.0),
                            f"mlpred_{row['profile_id']}_{dt.replace('-', '')}_{args.model_version}",
                            note,
                        ),
                    )
                    inserted += 1

                print(f"[INFO] fallback rows inserted: {inserted}")

        conn.commit()
        print(f"[OK] prediction completed: mode={mode}, rows={len(df)}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
