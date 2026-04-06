from __future__ import annotations

import argparse
import math
from datetime import date, datetime, timedelta
from typing import Iterable

import pymysql

DEFAULT_FEATURES = [
    "daily_active_users","page_view_count","avg_session_duration_sec","new_user_ratio",
    "auth_attempt_count","auth_success_count","auth_fail_count","auth_success_rate","auth_fail_rate",
    "otp_request_count","risk_login_count","loan_view_count","loan_apply_start_count",
    "loan_apply_submit_count","loan_funnel_conversion","card_apply_start_count",
    "card_apply_submit_count","card_apply_submit_rate","card_funnel_conversion",
    "submit_capture_rate","success_outcome_capture_rate","collector_event_count",
    "raw_event_count","estimated_missing_rate","mapping_coverage",
]

FEATURE_GROUPS = {
    "daily_active_users": "user_activity",
    "page_view_count": "user_activity",
    "avg_session_duration_sec": "user_activity",
    "new_user_ratio": "user_activity",
    "auth_attempt_count": "auth_security",
    "auth_success_count": "auth_security",
    "auth_fail_count": "auth_security",
    "auth_success_rate": "auth_security",
    "auth_fail_rate": "auth_security",
    "otp_request_count": "auth_security",
    "risk_login_count": "auth_security",
    "loan_view_count": "financial_service",
    "loan_apply_start_count": "financial_service",
    "loan_apply_submit_count": "financial_service",
    "loan_funnel_conversion": "financial_service",
    "card_apply_start_count": "financial_service",
    "card_apply_submit_count": "financial_service",
    "card_apply_submit_rate": "financial_service",
    "card_funnel_conversion": "financial_service",
    "submit_capture_rate": "financial_service",
    "success_outcome_capture_rate": "financial_service",
    "collector_event_count": "system_operation",
    "raw_event_count": "system_operation",
    "estimated_missing_rate": "system_operation",
    "mapping_coverage": "semantic_mapping",
}

PSI_FEATURES = set(DEFAULT_FEATURES) - {"avg_session_duration_sec"}


def connect_mysql(host: str, port: int, user: str, password: str, db: str):
    return pymysql.connect(host=host, port=port, user=user, password=password, database=db, charset="utf8mb4", autocommit=False, cursorclass=pymysql.cursors.DictCursor)


def ensure_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_feature_drift_result (
          feature_drift_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          feature_name VARCHAR(100) NOT NULL,
          feature_group VARCHAR(50) NOT NULL,
          baseline_value DECIMAL(20,6) NULL,
          observed_value DECIMAL(20,6) NULL,
          baseline_sd DECIMAL(20,6) NULL,
          drift_score DECIMAL(20,6) NULL,
          drift_method VARCHAR(50) NOT NULL,
          drift_status VARCHAR(20) NOT NULL,
          severity VARCHAR(20) NOT NULL,
          predicted_risk_status VARCHAR(20) NULL,
          prob_alert DECIMAL(20,6) NULL,
          run_id VARCHAR(64) NULL,
          note VARCHAR(255) NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          KEY idx_profile_dt (profile_id, dt),
          KEY idx_profile_feature_dt (profile_id, feature_name, dt)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_feature_drift_day (
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          drift_alert_count INT NOT NULL DEFAULT 0,
          drift_warn_count INT NOT NULL DEFAULT 0,
          top_drift_feature_1 VARCHAR(100) NULL,
          top_drift_feature_2 VARCHAR(100) NULL,
          top_drift_feature_3 VARCHAR(100) NULL,
          predicted_risk_status VARCHAR(20) NULL,
          prob_alert DECIMAL(20,6) NULL,
          run_id VARCHAR(64) NULL,
          note VARCHAR(255) NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (profile_id, dt)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def daterange(start_dt: date, end_dt: date):
    cur = start_dt
    while cur <= end_dt:
        yield cur
        cur += timedelta(days=1)


def feature_group(name: str) -> str:
    return FEATURE_GROUPS.get(name, "other")


def psi_like(obs: float, base: float) -> float:
    if base in (None, 0):
        return 0.0
    p = max(float(obs), 1e-6)
    q = max(float(base), 1e-6)
    return (p - q) * math.log(p / q)


def zscore(obs: float, mean: float, sd: float) -> float:
    if mean is None or sd in (None, 0):
        return 0.0
    return (float(obs) - float(mean)) / float(sd)


def status_from_score(method: str, score: float) -> str:
    if method == "psi_like":
        if abs(score) >= 1.0:
            return "alert"
        if abs(score) >= 0.3:
            return "warn"
        return "normal"
    if method == "zscore":
        if abs(score) >= 3.5:
            return "alert"
        if abs(score) >= 2.5:
            return "warn"
        return "normal"
    return "normal"


def severity_from_status(status: str) -> str:
    return {"alert": "high", "warn": "medium"}.get(status, "low")


def load_daily_baseline(cur, profile_id: str, target_date: str, baseline_days: int, features: Iterable[str]):
    feats = list(features)
    placeholders = ",".join(["%s"] * len(feats))
    sql = f"""
        SELECT metric_name, AVG(metric_value) AS baseline_value, STDDEV_SAMP(metric_value) AS baseline_sd
        FROM metric_value_day
        WHERE profile_id = %s
          AND dt >= DATE_SUB(%s, INTERVAL %s DAY)
          AND dt < %s
          AND metric_name IN ({placeholders})
        GROUP BY metric_name
    """
    cur.execute(sql, (profile_id, target_date, baseline_days, target_date, *feats))
    return {r["metric_name"]: r for r in cur.fetchall()}


def load_daily_observed(cur, profile_id: str, target_date: str, features: Iterable[str]):
    feats = list(features)
    placeholders = ",".join(["%s"] * len(feats))
    sql = f"SELECT metric_name, metric_value FROM metric_value_day WHERE profile_id=%s AND dt=%s AND metric_name IN ({placeholders})"
    cur.execute(sql, (profile_id, target_date, *feats))
    return {r["metric_name"]: r["metric_value"] for r in cur.fetchall()}


def load_prediction(cur, profile_id: str, target_date: str):
    cur.execute(
        "SELECT predicted_risk_status, prob_alert FROM ml_prediction_result WHERE profile_id=%s AND dt=%s ORDER BY created_at DESC LIMIT 1",
        (profile_id, target_date),
    )
    return cur.fetchone() or {}


def main() -> None:
    ap = argparse.ArgumentParser(description="ML feature drift analyzer v2 (daily summary + prediction linkage)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--date")
    ap.add_argument("--dt-from")
    ap.add_argument("--dt-to")
    ap.add_argument("--baseline-days", type=int, default=28)
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    if args.date:
        dt_from = dt_to = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        if not args.dt_from or not args.dt_to:
            raise SystemExit("Either --date or both --dt-from/--dt-to are required.")
        dt_from = datetime.strptime(args.dt_from, "%Y-%m-%d").date()
        dt_to = datetime.strptime(args.dt_to, "%Y-%m-%d").date()

    run_id = f"mlfd2_{args.profile_id}_{dt_from.strftime('%Y%m%d')}_{dt_to.strftime('%Y%m%d')}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    conn = connect_mysql(args.host, args.port, args.user, args.password, args.db)
    try:
        with conn.cursor() as cur:
            ensure_tables(cur)
            if args.truncate:
                cur.execute("DELETE FROM ml_feature_drift_result WHERE profile_id=%s AND dt BETWEEN %s AND %s", (args.profile_id, dt_from, dt_to))
                cur.execute("DELETE FROM ml_feature_drift_day WHERE profile_id=%s AND dt BETWEEN %s AND %s", (args.profile_id, dt_from, dt_to))

            for target_dt in daterange(dt_from, dt_to):
                target_date = target_dt.strftime('%Y-%m-%d')
                baseline = load_daily_baseline(cur, args.profile_id, target_date, args.baseline_days, DEFAULT_FEATURES)
                observed = load_daily_observed(cur, args.profile_id, target_date, DEFAULT_FEATURES)
                pred = load_prediction(cur, args.profile_id, target_date)
                rows = []
                ranking = []
                for feat in DEFAULT_FEATURES:
                    obs = observed.get(feat)
                    base_row = baseline.get(feat)
                    base = base_row.get("baseline_value") if base_row else None
                    sd = base_row.get("baseline_sd") if base_row else None
                    method = "psi_like" if feat in PSI_FEATURES else "zscore"
                    score = psi_like(obs or 0, base or 0) if method == "psi_like" else zscore(obs or 0, base or 0, sd or 0)
                    status = status_from_score(method, score)
                    severity = severity_from_status(status)
                    rows.append((
                        args.profile_id, target_date, feat, feature_group(feat),
                        base, obs, sd, score, method, status, severity,
                        pred.get("predicted_risk_status"), pred.get("prob_alert"),
                        run_id, f"baseline_days={args.baseline_days}"
                    ))
                    ranking.append((feat, abs(float(score)), status))
                cur.executemany(
                    """
                    INSERT INTO ml_feature_drift_result (
                      profile_id, dt, feature_name, feature_group, baseline_value, observed_value,
                      baseline_sd, drift_score, drift_method, drift_status, severity,
                      predicted_risk_status, prob_alert, run_id, note
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    rows,
                )
                ranking.sort(key=lambda x: x[1], reverse=True)
                top = [x[0] for x in ranking[:3]] + [None, None, None]
                alert_count = sum(1 for _, _, s in ranking if s == 'alert')
                warn_count = sum(1 for _, _, s in ranking if s == 'warn')
                cur.execute(
                    """
                    REPLACE INTO ml_feature_drift_day (
                      profile_id, dt, drift_alert_count, drift_warn_count,
                      top_drift_feature_1, top_drift_feature_2, top_drift_feature_3,
                      predicted_risk_status, prob_alert, run_id, note
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        args.profile_id, target_date, alert_count, warn_count,
                        top[0], top[1], top[2],
                        pred.get("predicted_risk_status"), pred.get("prob_alert"),
                        run_id, "daily ML feature drift summary"
                    ),
                )
                print(f"[OK] ml feature drift completed: dt={target_date}, alerts={alert_count}, warns={warn_count}")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
