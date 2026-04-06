#!/usr/bin/env python3
import argparse
import os
from decimal import Decimal

import pymysql


def d(val):
    try:
        return Decimal(str(val or 0))
    except Exception:
        return Decimal("0")


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
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )

    # Avoid literal % patterns in SQL because pymysql uses Python-style % substitution.
    feature_sql = """
    SELECT
        m.profile_id,
        m.dt,

        MAX(CASE WHEN m.metric_name='daily_active_users' THEN m.metric_value END) AS daily_active_users,
        MAX(CASE WHEN m.metric_name='page_view_count' THEN m.metric_value END) AS page_view_count,
        MAX(CASE WHEN m.metric_name='avg_session_duration_sec' THEN m.metric_value END) AS avg_session_duration,
        MAX(CASE WHEN m.metric_name='new_user_ratio' THEN m.metric_value END) AS new_user_ratio,

        MAX(CASE WHEN m.metric_name='auth_attempt_count' THEN m.metric_value END) AS auth_attempt_count,
        MAX(CASE WHEN m.metric_name='auth_success_count' THEN m.metric_value END) AS auth_success_count,
        MAX(CASE WHEN m.metric_name='auth_fail_count' THEN m.metric_value END) AS auth_fail_count,
        MAX(CASE WHEN m.metric_name='auth_success_rate' THEN m.metric_value END) AS auth_success_rate,
        MAX(CASE WHEN m.metric_name='auth_fail_rate' THEN m.metric_value END) AS auth_fail_rate,
        MAX(CASE WHEN m.metric_name='otp_request_count' THEN m.metric_value END) AS otp_request_count,
        MAX(CASE WHEN m.metric_name='risk_login_count' THEN m.metric_value END) AS risk_login_count,

        MAX(CASE WHEN m.metric_name='loan_view_count' THEN m.metric_value END) AS loan_view_count,
        MAX(CASE WHEN m.metric_name='loan_apply_start_count' THEN m.metric_value END) AS loan_apply_start_count,
        MAX(CASE WHEN m.metric_name='loan_apply_submit_count' THEN m.metric_value END) AS loan_apply_submit_count,

        MAX(CASE WHEN m.metric_name='card_apply_start_count' THEN m.metric_value END) AS card_apply_start_count,
        MAX(CASE WHEN m.metric_name='card_apply_submit_count' THEN m.metric_value END) AS card_apply_submit_count,
        MAX(CASE WHEN m.metric_name='card_apply_submit_rate' THEN m.metric_value END) AS card_apply_submit_rate,

        MAX(CASE WHEN m.metric_name='collector_event_count' THEN m.metric_value END) AS collector_event_count,
        MAX(CASE WHEN m.metric_name='raw_event_count' THEN m.metric_value END) AS raw_event_count,
        MAX(CASE WHEN m.metric_name='estimated_missing_rate' THEN m.metric_value END) AS estimated_missing_rate,

        COALESCE(MAX(v.fail_count), 0) AS validation_fail_count,
        COALESCE(MAX(v.warn_count), 0) AS validation_warn_count,

        COALESCE(MAX(ds.alert_cnt), 0) AS drift_alert_count,
        COALESCE(MAX(ds.warn_cnt), 0) AS drift_warn_count,

        0 AS ml_feature_alert_count,
        0 AS ml_feature_warn_count,

        COALESCE(
            MAX(
                CASE
                    WHEN r.risk_grade='high' THEN 'alert'
                    WHEN r.risk_grade='medium' THEN 'warning'
                    ELSE 'normal'
                END
            ),
            'normal'
        ) AS target_risk_status,

        COALESCE(MAX(r.final_risk_score), 0) AS target_risk_score

    FROM metric_value_day m
    LEFT JOIN (
        SELECT
            profile_id,
            dt,
            MAX(fail_count) AS fail_count,
            MAX(warn_count) AS warn_count
        FROM validation_summary_day
        GROUP BY profile_id, dt
    ) v
      ON m.profile_id = v.profile_id
     AND m.dt = v.dt
    LEFT JOIN (
        SELECT
            profile_id,
            dt,
            SUM(CASE WHEN drift_status='alert' THEN 1 ELSE 0 END) AS alert_cnt,
            SUM(CASE WHEN drift_status='warn' THEN 1 ELSE 0 END) AS warn_cnt
        FROM metric_drift_result_r
        GROUP BY profile_id, dt
    ) ds
      ON m.profile_id = ds.profile_id
     AND m.dt = ds.dt
    LEFT JOIN data_risk_score_day_v3 r
      ON m.profile_id = r.profile_id
     AND m.dt = r.dt
     AND r.metric_nm = 'ALL'
    WHERE m.profile_id = %s
      AND m.dt BETWEEN %s AND %s
    GROUP BY m.profile_id, m.dt
    ORDER BY m.dt
    """

    insert_sql = """
    REPLACE INTO ml_feature_vector_day (
        profile_id,
        dt,
        daily_active_users,
        page_view_count,
        avg_session_duration,
        new_user_ratio,
        auth_attempt_count,
        auth_success_count,
        auth_fail_count,
        auth_success_rate,
        auth_fail_rate,
        otp_request_count,
        risk_login_count,
        loan_view_count,
        loan_apply_start_count,
        loan_apply_submit_count,
        card_apply_start_count,
        card_apply_submit_count,
        card_apply_submit_rate,
        collector_event_count,
        raw_event_count,
        estimated_missing_rate,
        validation_fail_count,
        validation_warn_count,
        drift_alert_count,
        drift_warn_count,
        ml_feature_alert_count,
        ml_feature_warn_count,
        target_risk_status,
        target_risk_score,
        run_id,
        note
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """

    try:
        with conn.cursor() as cur:
            if args.truncate:
                cur.execute(
                    "DELETE FROM ml_feature_vector_day WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                    (args.profile_id, args.dt_from, args.dt_to),
                )

            cur.execute(feature_sql, (args.profile_id, args.dt_from, args.dt_to))
            rows = cur.fetchall()

            for row in rows:
                dt_text = str(row["dt"]).replace("-", "")
                run_id = f"mlfv_{row['profile_id']}_{dt_text}"
                note = "as-is schema aligned feature vector"

                vals = (
                    row["profile_id"],
                    row["dt"],
                    float(d(row.get("daily_active_users"))),
                    float(d(row.get("page_view_count"))),
                    float(d(row.get("avg_session_duration"))),
                    float(d(row.get("new_user_ratio"))),
                    float(d(row.get("auth_attempt_count"))),
                    float(d(row.get("auth_success_count"))),
                    float(d(row.get("auth_fail_count"))),
                    float(d(row.get("auth_success_rate"))),
                    float(d(row.get("auth_fail_rate"))),
                    float(d(row.get("otp_request_count"))),
                    float(d(row.get("risk_login_count"))),
                    float(d(row.get("loan_view_count"))),
                    float(d(row.get("loan_apply_start_count"))),
                    float(d(row.get("loan_apply_submit_count"))),
                    float(d(row.get("card_apply_start_count"))),
                    float(d(row.get("card_apply_submit_count"))),
                    float(d(row.get("card_apply_submit_rate"))),
                    float(d(row.get("collector_event_count"))),
                    float(d(row.get("raw_event_count"))),
                    float(d(row.get("estimated_missing_rate"))),
                    int(row.get("validation_fail_count") or 0),
                    int(row.get("validation_warn_count") or 0),
                    int(row.get("drift_alert_count") or 0),
                    int(row.get("drift_warn_count") or 0),
                    int(row.get("ml_feature_alert_count") or 0),
                    int(row.get("ml_feature_warn_count") or 0),
                    row.get("target_risk_status") or "normal",
                    float(d(row.get("target_risk_score"))),
                    run_id,
                    note,
                )
                cur.execute(insert_sql, vals)

            conn.commit()
            print(f"[OK] ml feature vector completed: profile_id={args.profile_id}, rows={len(rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
