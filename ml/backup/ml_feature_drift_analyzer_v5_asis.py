#!/usr/bin/env python3
import argparse
import os
from decimal import Decimal

import pandas as pd
import pymysql

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


def d(val):
    try:
        return Decimal(str(val or 0))
    except Exception:
        return Decimal("0")


def severity_from_score(score: float):
    if score >= 3.0:
        return "high", "alert"
    if score >= 2.0:
        return "medium", "warn"
    return "low", "normal"


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
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    conn = pymysql.connect(
        host=args.host, port=args.port, user=args.user, password=args.password,
        database=args.db, autocommit=False, cursorclass=pymysql.cursors.DictCursor
    )
    try:
        df = pd.read_sql(
            """
            SELECT *
            FROM ml_feature_vector_day
            WHERE profile_id=%s
              AND dt BETWEEN %s AND %s
            ORDER BY dt
            """,
            conn,
            params=[args.profile_id, args.dt_from, args.dt_to],
        )
        if df.empty:
            raise SystemExit("No rows found in ml_feature_vector_day")

        pred_map = {}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT profile_id, dt, predicted_risk_status
                FROM ml_prediction_result
                WHERE profile_id=%s AND dt BETWEEN %s AND %s
                """,
                (args.profile_id, args.dt_from, args.dt_to),
            )
            for r in cur.fetchall():
                pred_map[(str(r["profile_id"]), str(r["dt"]))] = r["predicted_risk_status"]

        with conn.cursor() as cur:
            if args.truncate:
                cur.execute(
                    "DELETE FROM ml_feature_drift_result WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                    (args.profile_id, args.dt_from, args.dt_to),
                )

            insert_sql = """
            INSERT INTO ml_feature_drift_result (
                profile_id, dt, hh, feature_name, feature_group,
                baseline_value, observed_value, baseline_sd,
                drift_score, drift_method, drift_status, severity,
                run_id, note
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s
            )
            """

            for idx, row in df.iterrows():
                history = df.iloc[max(0, idx - 7):idx]
                if history.empty:
                    continue

                for feat in FEATURE_COLUMNS:
                    if feat not in df.columns:
                        continue
                    hist = pd.to_numeric(history[feat], errors="coerce").dropna()
                    obs = pd.to_numeric(pd.Series([row.get(feat)]), errors="coerce").iloc[0]
                    if hist.empty or pd.isna(obs):
                        continue

                    baseline = float(hist.mean())
                    baseline_sd = float(hist.std(ddof=0)) if len(hist) > 1 else 0.0
                    if baseline_sd <= 1e-9:
                        score = 0.0 if float(obs) == baseline else 9.99
                    else:
                        score = abs(float(obs) - baseline) / baseline_sd

                    severity, drift_status = severity_from_score(score)
                    run_id = f"mlfd_{row['profile_id']}_{str(row['dt']).replace('-', '')}"
                    pred = pred_map.get((str(row["profile_id"]), str(row["dt"])), "")
                    note = f"predicted_risk_status={pred}" if pred else None
                    feature_group = "rate" if ("rate" in feat or "ratio" in feat) else "count"

                    cur.execute(
                        insert_sql,
                        (
                            row["profile_id"], row["dt"], None, feat, feature_group,
                            baseline, float(obs), baseline_sd,
                            score, "zscore", drift_status, severity,
                            run_id, note,
                        ),
                    )

            conn.commit()
            print("[OK] ml feature drift completed")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
