#!/usr/bin/env python3
import argparse
import os
from decimal import Decimal

import pymysql


FEATURE_SCHEMA_VERSION = "v1"


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

    # IMPORTANT:
    # Avoid literal % patterns inside this SQL because pymysql uses Python % formatting
    # for parameter substitution. Use INSTR(...) instead of LIKE '%...%'.
    sql = """
    SELECT
        d.profile_id,
        d.dt,

        COALESCE(v.warn_count, 0) AS validation_warn_count,
        COALESCE(v.fail_count, 0) AS validation_fail_count,

        SUM(CASE WHEN d.drift_status = 'alert' THEN 1 ELSE 0 END) AS drift_alert_count,
        SUM(CASE WHEN d.drift_status = 'warn' THEN 1 ELSE 0 END) AS drift_warn_count,

        MAX(CASE WHEN d.metric_name = 'auth_success_rate' THEN d.observed_value END) AS auth_success_rate,
        MAX(CASE WHEN d.metric_name = 'card_funnel_conversion' THEN d.observed_value END) AS card_funnel_conversion,
        MAX(CASE WHEN d.metric_name = 'loan_funnel_conversion' THEN d.observed_value END) AS loan_funnel_conversion,
        MAX(CASE WHEN d.metric_name = 'submit_capture_rate' THEN d.observed_value END) AS submit_capture_rate,
        MAX(CASE WHEN d.metric_name = 'success_outcome_capture_rate' THEN d.observed_value END) AS success_outcome_capture_rate,

        COALESCE(MAX(mc.mapping_coverage), 1.0) AS mapping_coverage,

        SUM(CASE WHEN d.severity = 'high' THEN 1 ELSE 0 END) AS anomaly_alert_count,
        SUM(CASE WHEN d.severity = 'medium' THEN 1 ELSE 0 END) AS anomaly_warn_count,

        COALESCE(MAX(rc.funnel_distortion_count), 0) AS root_cause_funnel_distortion_count,
        COALESCE(MAX(rc.metric_drift_count), 0) AS root_cause_metric_drift_count,

        COALESCE(MAX(r.final_risk_score), 0) AS target_risk_score,
        COALESCE(MAX(r.risk_grade), 'low') AS target_risk_grade,
        CASE
            WHEN COALESCE(MAX(r.final_risk_score), 0) >= 0.70 THEN 1
            ELSE 0
        END AS target_risk_label

    FROM metric_drift_result_r d
    LEFT JOIN (
        SELECT
            profile_id,
            dt,
            MAX(warn_count) AS warn_count,
            MAX(fail_count) AS fail_count
        FROM validation_summary_day
        GROUP BY profile_id, dt
    ) v
      ON d.profile_id = v.profile_id
     AND d.dt = v.dt
    LEFT JOIN mapping_coverage_day mc
      ON d.dt = mc.dt
    LEFT JOIN (
        SELECT
            profile_id,
            dt,
            SUM(CASE WHEN INSTR(cause_type, 'funnel') > 0 THEN 1 ELSE 0 END) AS funnel_distortion_count,
            SUM(CASE WHEN cause_type = 'metric_drift' THEN 1 ELSE 0 END) AS metric_drift_count
        FROM data_risk_root_cause_day
        GROUP BY profile_id, dt
    ) rc
      ON d.profile_id = rc.profile_id
     AND d.dt = rc.dt
    LEFT JOIN data_risk_score_day_v3 r
      ON d.profile_id = r.profile_id
     AND d.dt = r.dt
     AND r.metric_nm = 'ALL'
    WHERE d.profile_id = %s
      AND d.dt BETWEEN %s AND %s
    GROUP BY d.profile_id, d.dt
    ORDER BY d.dt
    """

    try:
        with conn.cursor() as cur:
            if args.truncate:
                cur.execute(
                    """
                    DELETE FROM ml_feature_vector_day
                    WHERE profile_id=%s AND dt BETWEEN %s AND %s
                    """,
                    (args.profile_id, args.dt_from, args.dt_to),
                )

            cur.execute(sql, (args.profile_id, args.dt_from, args.dt_to))
            rows = cur.fetchall()

            insert_sql = """
            REPLACE INTO ml_feature_vector_day (
                profile_id,
                dt,
                validation_warn_count,
                validation_fail_count,
                drift_alert_count,
                drift_warn_count,
                auth_success_rate,
                card_funnel_conversion,
                loan_funnel_conversion,
                submit_capture_rate,
                success_outcome_capture_rate,
                mapping_coverage,
                anomaly_alert_count,
                anomaly_warn_count,
                root_cause_funnel_distortion_count,
                root_cause_metric_drift_count,
                target_risk_score,
                target_risk_grade,
                target_risk_label,
                feature_schema_version,
                run_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """

            for r in rows:
                run_id = f"mlfv_{r['profile_id']}_{str(r['dt']).replace('-', '')}"
                cur.execute(
                    insert_sql,
                    (
                        r["profile_id"],
                        r["dt"],
                        int(r.get("validation_warn_count") or 0),
                        int(r.get("validation_fail_count") or 0),
                        int(r.get("drift_alert_count") or 0),
                        int(r.get("drift_warn_count") or 0),
                        float(d(r.get("auth_success_rate"))),
                        float(d(r.get("card_funnel_conversion"))),
                        float(d(r.get("loan_funnel_conversion"))),
                        float(d(r.get("submit_capture_rate"))),
                        float(d(r.get("success_outcome_capture_rate"))),
                        float(d(r.get("mapping_coverage") or 1)),
                        int(r.get("anomaly_alert_count") or 0),
                        int(r.get("anomaly_warn_count") or 0),
                        int(r.get("root_cause_funnel_distortion_count") or 0),
                        int(r.get("root_cause_metric_drift_count") or 0),
                        float(d(r.get("target_risk_score"))),
                        r.get("target_risk_grade") or "low",
                        int(r.get("target_risk_label") or 0),
                        FEATURE_SCHEMA_VERSION,
                        run_id,
                    ),
                )

            conn.commit()
            print(
                f"[OK] ml feature vector completed: profile_id={args.profile_id}, "
                f"rows={len(rows)}, feature_schema_version={FEATURE_SCHEMA_VERSION}"
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
