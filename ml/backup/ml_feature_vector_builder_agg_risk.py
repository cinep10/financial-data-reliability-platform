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


def get_columns(cur, table_name):
    cur.execute(f"DESC {table_name}")
    return [r["Field"] for r in cur.fetchall()]


def choose(columns, *names):
    for n in names:
        if n in columns:
            return n
    return None


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

    try:
        with conn.cursor() as cur:
            mlfv_cols = get_columns(cur, "ml_feature_vector_day")
            risk_cols = get_columns(cur, "data_risk_score_day_v3")

            avg_session_col = choose(mlfv_cols, "avg_session_duration", "avg_session_duration_sec", "avg_session_dura")
            if avg_session_col is None:
                raise RuntimeError("No avg session duration column found in ml_feature_vector_day")

            risk_score_col = choose(risk_cols, "final_risk_score", "risk_score")
            risk_grade_col = choose(risk_cols, "risk_grade", "risk_status")
            metric_nm_col = choose(risk_cols, "metric_nm")

            # IMPORTANT:
            # Do NOT assume metric_nm='ALL' exists.
            # Aggregate daily risk across all metric_nm rows.
            if risk_score_col is None or risk_grade_col is None:
                raise RuntimeError("data_risk_score_day_v3 is missing risk score / grade columns")

            if risk_grade_col == "risk_grade":
                risk_status_case = """
                    CASE
                        WHEN SUM(CASE WHEN risk_grade='high' THEN 1 ELSE 0 END) > 0 THEN 'alert'
                        WHEN SUM(CASE WHEN risk_grade='medium' THEN 1 ELSE 0 END) > 0 THEN 'warning'
                        ELSE 'normal'
                    END
                """
            else:
                risk_status_case = """
                    CASE
                        WHEN SUM(CASE WHEN risk_status='alert' THEN 1 ELSE 0 END) > 0 THEN 'alert'
                        WHEN SUM(CASE WHEN risk_status='warning' THEN 1 ELSE 0 END) > 0 THEN 'warning'
                        ELSE 'normal'
                    END
                """

            risk_daily_sql = f"""
                SELECT
                    profile_id,
                    dt,
                    MAX({risk_score_col}) AS agg_risk_score,
                    {risk_status_case} AS agg_risk_status
                FROM data_risk_score_day_v3
                WHERE profile_id=%s
                  AND dt BETWEEN %s AND %s
                GROUP BY profile_id, dt
            """

            feature_sql = """
            SELECT
                m.profile_id,
                m.dt,

                MAX(CASE WHEN m.metric_name='daily_active_users' THEN m.metric_value END) AS daily_active_users,
                MAX(CASE WHEN m.metric_name='page_view_count' THEN m.metric_value END) AS page_view_count,
                MAX(CASE WHEN m.metric_name='avg_session_duration_sec' THEN m.metric_value END) AS avg_session_duration_value,
                MAX(CASE WHEN m.metric_name='avg_session_duration' THEN m.metric_value END) AS avg_session_duration_value_alt,
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
                COALESCE(MAX(ds.warn_cnt), 0) AS drift_warn_count
            FROM metric_value_day m
            LEFT JOIN (
                SELECT profile_id, dt, MAX(fail_count) AS fail_count, MAX(warn_count) AS warn_count
                FROM validation_summary_day
                GROUP BY profile_id, dt
            ) v
              ON m.profile_id = v.profile_id
             AND m.dt = v.dt
            LEFT JOIN (
                SELECT profile_id, dt,
                       SUM(CASE WHEN drift_status='alert' THEN 1 ELSE 0 END) AS alert_cnt,
                       SUM(CASE WHEN drift_status='warn' THEN 1 ELSE 0 END) AS warn_cnt
                FROM metric_drift_result_r
                GROUP BY profile_id, dt
            ) ds
              ON m.profile_id = ds.profile_id
             AND m.dt = ds.dt
            WHERE m.profile_id = %s
              AND m.dt BETWEEN %s AND %s
            GROUP BY m.profile_id, m.dt
            ORDER BY m.dt
            """

            if args.truncate:
                cur.execute(
                    "DELETE FROM ml_feature_vector_day WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                    (args.profile_id, args.dt_from, args.dt_to),
                )

            cur.execute(risk_daily_sql, (args.profile_id, args.dt_from, args.dt_to))
            risk_rows = cur.fetchall()
            risk_map = {(str(r["profile_id"]), str(r["dt"])): r for r in risk_rows}

            cur.execute(feature_sql, (args.profile_id, args.dt_from, args.dt_to))
            rows = cur.fetchall()

            insert_cols = [
                "profile_id", "dt", "daily_active_users", "page_view_count", avg_session_col,
                "new_user_ratio", "auth_attempt_count", "auth_success_count", "auth_fail_count",
                "auth_success_rate", "auth_fail_rate", "otp_request_count", "risk_login_count",
                "loan_view_count", "loan_apply_start_count", "loan_apply_submit_count",
                "card_apply_start_count", "card_apply_submit_count", "card_apply_submit_rate",
                "collector_event_count", "raw_event_count", "estimated_missing_rate",
                "validation_fail_count", "validation_warn_count", "drift_alert_count",
                "drift_warn_count", "ml_feature_alert_count", "ml_feature_warn_count",
                "target_risk_status", "target_risk_score", "run_id", "note"
            ]

            placeholders = ", ".join(["%s"] * len(insert_cols))
            insert_sql = f"REPLACE INTO ml_feature_vector_day ({', '.join(insert_cols)}) VALUES ({placeholders})"

            for row in rows:
                dt_text = str(row["dt"]).replace("-", "")
                avg_val = row.get("avg_session_duration_value")
                if avg_val is None:
                    avg_val = row.get("avg_session_duration_value_alt")

                risk = risk_map.get((str(row["profile_id"]), str(row["dt"])), {})
                target_risk_status = risk.get("agg_risk_status", "normal")
                target_risk_score = risk.get("agg_risk_score", 0)

                vals = (
                    row["profile_id"],
                    row["dt"],
                    float(d(row.get("daily_active_users"))),
                    float(d(row.get("page_view_count"))),
                    float(d(avg_val)),
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
                    0,
                    0,
                    target_risk_status,
                    float(d(target_risk_score)),
                    f"mlfv_{row['profile_id']}_{dt_text}",
                    f"risk aggregated across metric_nm; avg_col={avg_session_col}; risk_score_col={risk_score_col}; risk_grade_col={risk_grade_col}; metric_nm_col={metric_nm_col}",
                )
                cur.execute(insert_sql, vals)

            conn.commit()
            print(
                f"[OK] ml_feature_vector_builder_agg completed: rows={len(rows)}, "
                f"avg_col={avg_session_col}, risk_score_col={risk_score_col}, "
                f"risk_grade_col={risk_grade_col}, metric_nm_col={metric_nm_col}"
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
