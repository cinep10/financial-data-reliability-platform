from __future__ import annotations

import argparse
from datetime import datetime
import pymysql

FEATURE_SCHEMA_VERSION = "v2"

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
    "loan_funnel_conversion",
    "card_apply_start_count",
    "card_apply_submit_count",
    "card_apply_submit_rate",
    "card_funnel_conversion",
    "submit_capture_rate",
    "success_outcome_capture_rate",
    "collector_event_count",
    "raw_event_count",
    "estimated_missing_rate",
    "mapping_coverage",
    "validation_fail_count",
    "validation_warn_count",
    "drift_alert_count",
    "drift_warn_count",
    "anomaly_alert_count",
    "anomaly_warn_count",
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
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_feature_vector_day (
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          daily_active_users DECIMAL(20,6) NULL,
          page_view_count DECIMAL(20,6) NULL,
          avg_session_duration_sec DECIMAL(20,6) NULL,
          new_user_ratio DECIMAL(20,6) NULL,
          auth_attempt_count DECIMAL(20,6) NULL,
          auth_success_count DECIMAL(20,6) NULL,
          auth_fail_count DECIMAL(20,6) NULL,
          auth_success_rate DECIMAL(20,6) NULL,
          auth_fail_rate DECIMAL(20,6) NULL,
          otp_request_count DECIMAL(20,6) NULL,
          risk_login_count DECIMAL(20,6) NULL,
          loan_view_count DECIMAL(20,6) NULL,
          loan_apply_start_count DECIMAL(20,6) NULL,
          loan_apply_submit_count DECIMAL(20,6) NULL,
          loan_funnel_conversion DECIMAL(20,6) NULL,
          card_apply_start_count DECIMAL(20,6) NULL,
          card_apply_submit_count DECIMAL(20,6) NULL,
          card_apply_submit_rate DECIMAL(20,6) NULL,
          card_funnel_conversion DECIMAL(20,6) NULL,
          submit_capture_rate DECIMAL(20,6) NULL,
          success_outcome_capture_rate DECIMAL(20,6) NULL,
          collector_event_count DECIMAL(20,6) NULL,
          raw_event_count DECIMAL(20,6) NULL,
          estimated_missing_rate DECIMAL(20,6) NULL,
          mapping_coverage DECIMAL(20,6) NULL,
          validation_fail_count INT NOT NULL DEFAULT 0,
          validation_warn_count INT NOT NULL DEFAULT 0,
          drift_alert_count INT NOT NULL DEFAULT 0,
          drift_warn_count INT NOT NULL DEFAULT 0,
          anomaly_alert_count INT NOT NULL DEFAULT 0,
          anomaly_warn_count INT NOT NULL DEFAULT 0,
          ml_feature_alert_count INT NOT NULL DEFAULT 0,
          ml_feature_warn_count INT NOT NULL DEFAULT 0,
          total_signal_count INT NOT NULL DEFAULT 0,
          target_risk_grade VARCHAR(20) NULL,
          target_risk_score DECIMAL(20,6) NULL,
          target_risk_label TINYINT NULL,
          feature_schema_version VARCHAR(20) NOT NULL DEFAULT 'v2',
          run_id VARCHAR(64) NULL,
          note VARCHAR(255) NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (profile_id, dt)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def build_sql() -> str:
    return """
    SELECT
      d.profile_id,
      d.dt,
      MAX(CASE WHEN d.metric_name='daily_active_users' THEN d.metric_value END) AS daily_active_users,
      MAX(CASE WHEN d.metric_name='page_view_count' THEN d.metric_value END) AS page_view_count,
      MAX(CASE WHEN d.metric_name='avg_session_duration_sec' THEN d.metric_value END) AS avg_session_duration_sec,
      MAX(CASE WHEN d.metric_name='new_user_ratio' THEN d.metric_value END) AS new_user_ratio,
      MAX(CASE WHEN d.metric_name='auth_attempt_count' THEN d.metric_value END) AS auth_attempt_count,
      MAX(CASE WHEN d.metric_name='auth_success_count' THEN d.metric_value END) AS auth_success_count,
      MAX(CASE WHEN d.metric_name='auth_fail_count' THEN d.metric_value END) AS auth_fail_count,
      MAX(CASE WHEN d.metric_name='auth_success_rate' THEN d.metric_value END) AS auth_success_rate,
      MAX(CASE WHEN d.metric_name='auth_fail_rate' THEN d.metric_value END) AS auth_fail_rate,
      MAX(CASE WHEN d.metric_name='otp_request_count' THEN d.metric_value END) AS otp_request_count,
      MAX(CASE WHEN d.metric_name='risk_login_count' THEN d.metric_value END) AS risk_login_count,
      MAX(CASE WHEN d.metric_name='loan_view_count' THEN d.metric_value END) AS loan_view_count,
      MAX(CASE WHEN d.metric_name='loan_apply_start_count' THEN d.metric_value END) AS loan_apply_start_count,
      MAX(CASE WHEN d.metric_name='loan_apply_submit_count' THEN d.metric_value END) AS loan_apply_submit_count,
      MAX(CASE WHEN d.metric_name='loan_funnel_conversion' THEN d.metric_value END) AS loan_funnel_conversion,
      MAX(CASE WHEN d.metric_name='card_apply_start_count' THEN d.metric_value END) AS card_apply_start_count,
      MAX(CASE WHEN d.metric_name='card_apply_submit_count' THEN d.metric_value END) AS card_apply_submit_count,
      MAX(CASE WHEN d.metric_name='card_apply_submit_rate' THEN d.metric_value END) AS card_apply_submit_rate,
      MAX(CASE WHEN d.metric_name='card_funnel_conversion' THEN d.metric_value END) AS card_funnel_conversion,
      MAX(CASE WHEN d.metric_name='submit_capture_rate' THEN d.metric_value END) AS submit_capture_rate,
      MAX(CASE WHEN d.metric_name='success_outcome_capture_rate' THEN d.metric_value END) AS success_outcome_capture_rate,
      MAX(CASE WHEN d.metric_name='collector_event_count' THEN d.metric_value END) AS collector_event_count,
      MAX(CASE WHEN d.metric_name='raw_event_count' THEN d.metric_value END) AS raw_event_count,
      MAX(CASE WHEN d.metric_name='estimated_missing_rate' THEN d.metric_value END) AS estimated_missing_rate,
      COALESCE(mc.mapping_coverage, 0) AS mapping_coverage,
      COALESCE(v.fail_count, 0) AS validation_fail_count,
      COALESCE(v.warn_count, 0) AS validation_warn_count,
      COALESCE(r.drift_alert_count, 0) AS drift_alert_count,
      COALESCE(r.drift_warn_count, 0) AS drift_warn_count,
      COALESCE(r.anomaly_alert_count, 0) AS anomaly_alert_count,
      COALESCE(r.anomaly_warn_count, 0) AS anomaly_warn_count,
      COALESCE(m.ml_feature_alert_count, 0) AS ml_feature_alert_count,
      COALESCE(m.ml_feature_warn_count, 0) AS ml_feature_warn_count,
      COALESCE(s.total_signal_count, 0) AS total_signal_count,
      rs.risk_grade AS target_risk_grade,
      rs.final_risk_score AS target_risk_score,
      CASE
        WHEN rs.risk_grade = 'high' THEN 1
        WHEN rs.final_risk_score >= 0.7 THEN 1
        ELSE 0
      END AS target_risk_label
    FROM metric_value_day d
    LEFT JOIN validation_summary_day v
      ON d.profile_id = v.profile_id AND d.dt = v.dt
    LEFT JOIN mapping_coverage_day mc
      ON d.dt = mc.dt
    LEFT JOIN (
      SELECT
        profile_id,
        dt,
        SUM(CASE WHEN drift_status='alert' THEN 1 ELSE 0 END) AS drift_alert_count,
        SUM(CASE WHEN drift_status='warn' THEN 1 ELSE 0 END) AS drift_warn_count,
        SUM(CASE WHEN drift_status='alert' AND (
            metric_name LIKE '%conversion%' OR metric_name LIKE '%success%' OR metric_name LIKE '%submit%'
        ) THEN 1 ELSE 0 END) AS anomaly_alert_count,
        SUM(CASE WHEN drift_status='warn' AND (
            metric_name LIKE '%conversion%' OR metric_name LIKE '%success%' OR metric_name LIKE '%submit%'
        ) THEN 1 ELSE 0 END) AS anomaly_warn_count
      FROM metric_drift_result_r
      GROUP BY profile_id, dt
    ) r
      ON d.profile_id = r.profile_id AND d.dt = r.dt
    LEFT JOIN (
      SELECT
        profile_id,
        dt,
        SUM(CASE WHEN drift_status='alert' THEN 1 ELSE 0 END) AS ml_feature_alert_count,
        SUM(CASE WHEN drift_status='warn' THEN 1 ELSE 0 END) AS ml_feature_warn_count
      FROM ml_feature_drift_result
      GROUP BY profile_id, dt
    ) m
      ON d.profile_id = m.profile_id AND d.dt = m.dt
    LEFT JOIN (
      SELECT profile_id, dt, COUNT(*) AS total_signal_count
      FROM risk_signal_link_day
      GROUP BY profile_id, dt
    ) s
      ON d.profile_id = s.profile_id AND d.dt = s.dt
    LEFT JOIN data_risk_score_day_v3 rs
      ON d.profile_id = rs.profile_id AND d.dt = rs.dt AND rs.metric_nm='ALL'
    WHERE d.profile_id = %s
      AND d.dt BETWEEN %s AND %s
    GROUP BY d.profile_id, d.dt,
      mc.mapping_coverage,
      v.fail_count, v.warn_count,
      r.drift_alert_count, r.drift_warn_count, r.anomaly_alert_count, r.anomaly_warn_count,
      m.ml_feature_alert_count, m.ml_feature_warn_count,
      s.total_signal_count,
      rs.risk_grade, rs.final_risk_score
    ORDER BY d.dt
    """


def main() -> None:
    ap = argparse.ArgumentParser(description="Build daily ML feature vectors from data reliability tables")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    run_id = f"fv2_{args.profile_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    conn = connect_mysql(args.host, args.port, args.user, args.password, args.db)
    try:
        with conn.cursor() as cur:
            ensure_table(cur)
            if args.truncate:
                cur.execute(
                    "DELETE FROM ml_feature_vector_day WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                    (args.profile_id, args.dt_from, args.dt_to),
                )
            cur.execute(build_sql(), (args.profile_id, args.dt_from, args.dt_to))
            rows = cur.fetchall()
            inserts = []
            for r in rows:
                vals = [r.get(c) for c in FEATURE_COLS]
                inserts.append((
                    r["profile_id"], r["dt"],
                    *vals,
                    r.get("target_risk_grade"), r.get("target_risk_score"), r.get("target_risk_label"),
                    FEATURE_SCHEMA_VERSION, run_id, "daily feature vector from reliability platform v2"
                ))
            if inserts:
                cur.executemany(
                    f"""
                    INSERT INTO ml_feature_vector_day (
                      profile_id, dt,
                      {', '.join(FEATURE_COLS)},
                      target_risk_grade, target_risk_score, target_risk_label,
                      feature_schema_version, run_id, note
                    ) VALUES (
                      %s,%s,{','.join(['%s']*len(FEATURE_COLS))},%s,%s,%s,%s,%s,%s
                    )
                    ON DUPLICATE KEY UPDATE
                      {', '.join([f'{c}=VALUES({c})' for c in FEATURE_COLS])},
                      target_risk_grade=VALUES(target_risk_grade),
                      target_risk_score=VALUES(target_risk_score),
                      target_risk_label=VALUES(target_risk_label),
                      feature_schema_version=VALUES(feature_schema_version),
                      run_id=VALUES(run_id),
                      note=VALUES(note)
                    """
                , inserts)
        conn.commit()
        print(f"[OK] feature vector build completed: rows={len(inserts)} run_id={run_id} schema={FEATURE_SCHEMA_VERSION}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
