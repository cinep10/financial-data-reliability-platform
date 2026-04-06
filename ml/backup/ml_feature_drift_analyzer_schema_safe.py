from __future__ import annotations

import argparse
import math
from datetime import datetime, timedelta, date
from typing import Iterable, List, Tuple

import pymysql


DEFAULT_FEATURES = [
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
    "card_apply_start_count",
    "card_apply_submit_count",
    "card_apply_submit_rate",
    "collector_event_count",
    "raw_event_count",
    "estimated_missing_rate",
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
    "card_apply_start_count": "financial_service",
    "card_apply_submit_count": "financial_service",
    "card_apply_submit_rate": "financial_service",
    "collector_event_count": "system_operation",
    "raw_event_count": "system_operation",
    "estimated_missing_rate": "system_operation",
}

PSI_FEATURES = {
    "daily_active_users",
    "page_view_count",
    "new_user_ratio",
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
}


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
        CREATE TABLE IF NOT EXISTS ml_feature_drift_result (
          feature_drift_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          hh TINYINT NULL,
          feature_name VARCHAR(100) NOT NULL,
          feature_group VARCHAR(50) NOT NULL,
          baseline_value DECIMAL(20,6) NULL,
          observed_value DECIMAL(20,6) NULL,
          baseline_sd DECIMAL(20,6) NULL,
          drift_score DECIMAL(20,6) NULL,
          drift_method VARCHAR(50) NOT NULL,
          drift_status VARCHAR(20) NOT NULL,
          severity VARCHAR(20) NOT NULL,
          run_id VARCHAR(64) NULL,
          note VARCHAR(255) NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          KEY idx_profile_dt (profile_id, dt),
          KEY idx_feature (feature_name),
          KEY idx_status (drift_status),
          KEY idx_profile_feature_dt (profile_id, feature_name, dt)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def get_table_columns(cur, table_name: str) -> set[str]:
    cur.execute(f"SHOW COLUMNS FROM {table_name}")
    return {row["Field"] for row in cur.fetchall()}


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
        if abs(score) >= 0.25:
            return "alert"
        if abs(score) >= 0.10:
            return "warn"
        return "normal"
    if method == "zscore":
        if abs(score) >= 3:
            return "alert"
        if abs(score) >= 2:
            return "warn"
        return "normal"
    return "normal"


def severity_from_status(status: str) -> str:
    return {"alert": "high", "warn": "medium"}.get(status, "low")


def load_hourly_baseline(cur, profile_id: str, target_date: str, baseline_days: int, features: Iterable[str]):
    feats = list(features)
    placeholders = ",".join(["%s"] * len(feats))
    sql = f"""
        SELECT
            metric_name,
            hh,
            AVG(metric_value) AS baseline_value,
            STDDEV_SAMP(metric_value) AS baseline_sd
        FROM metric_value_hh
        WHERE profile_id = %s
          AND dt >= DATE_SUB(%s, INTERVAL %s DAY)
          AND dt < %s
          AND DAYOFWEEK(dt) = DAYOFWEEK(%s)
          AND metric_name IN ({placeholders})
        GROUP BY metric_name, hh
    """
    cur.execute(sql, (profile_id, target_date, baseline_days, target_date, target_date, *feats))
    return {(r["metric_name"], int(r["hh"])): r for r in cur.fetchall()}


def load_hourly_observed(cur, profile_id: str, target_date: str, features: Iterable[str]):
    feats = list(features)
    placeholders = ",".join(["%s"] * len(feats))
    sql = f"""
        SELECT metric_name, hh, metric_value
        FROM metric_value_hh
        WHERE profile_id = %s
          AND dt = %s
          AND metric_name IN ({placeholders})
    """
    cur.execute(sql, (profile_id, target_date, *feats))
    return cur.fetchall()


def build_insert_rows(profile_id: str, target_date: str, observed_rows, baseline_rows, baseline_days: int, run_id: str):
    rows = []
    for row in observed_rows:
        feature_name = row["metric_name"]
        hh = int(row["hh"])
        observed = float(row["metric_value"] or 0)
        base_row = baseline_rows.get((feature_name, hh), {})
        baseline_value = float(base_row.get("baseline_value") or 0)
        baseline_sd = float(base_row.get("baseline_sd") or 0)

        method = "psi_like" if feature_name in PSI_FEATURES else "zscore"
        score = psi_like(observed, baseline_value) if method == "psi_like" else zscore(observed, baseline_value, baseline_sd)
        status = status_from_score(method, score)
        severity = severity_from_status(status)

        rows.append({
            "profile_id": profile_id,
            "dt": target_date,
            "hh": hh,
            "feature_name": feature_name,
            "feature_group": feature_group(feature_name),
            "baseline_value": round(baseline_value, 6),
            "observed_value": round(observed, 6),
            "baseline_sd": round(baseline_sd, 6),
            "drift_score": round(score, 6),
            "drift_method": method,
            "drift_status": status,
            "severity": severity,
            "run_id": run_id,
            "note": f"weekday+hour baseline; baseline_days={baseline_days}",
        })
    return rows


def insert_rows(cur, table_columns: set[str], rows: list[dict]):
    if not rows:
        return 0
    ordered_cols = [
        "profile_id", "dt", "hh", "feature_name", "feature_group",
        "baseline_value", "observed_value", "baseline_sd",
        "drift_score", "drift_method", "drift_status", "severity",
        "run_id", "note",
    ]
    cols = [c for c in ordered_cols if c in table_columns]
    placeholders = ",".join(["%s"] * len(cols))
    sql = f"""
        INSERT INTO ml_feature_drift_result ({",".join(cols)})
        VALUES ({placeholders})
    """
    values = [tuple(row.get(c) for c in cols) for row in rows]
    cur.executemany(sql, values)
    return len(values)


def main() -> None:
    ap = argparse.ArgumentParser(description="ML feature drift analyzer (range-capable, schema-tolerant)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--date", help="single target date, e.g. 2026-03-09")
    ap.add_argument("--dt-from", help="range start date, e.g. 2026-02-23")
    ap.add_argument("--dt-to", help="range end date, e.g. 2026-03-09")
    ap.add_argument("--baseline-days", type=int, default=28)
    ap.add_argument("--truncate", action="store_true", help="delete existing rows for target period before insert")
    args = ap.parse_args()

    if args.date:
        dt_from = dt_to = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        if not args.dt_from or not args.dt_to:
            raise SystemExit("Either --date or both --dt-from/--dt-to are required.")
        dt_from = datetime.strptime(args.dt_from, "%Y-%m-%d").date()
        dt_to = datetime.strptime(args.dt_to, "%Y-%m-%d").date()

    features = DEFAULT_FEATURES
    run_id = f"mlfd_{args.profile_id}_{dt_from.strftime('%Y%m%d')}_{dt_to.strftime('%Y%m%d')}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    conn = connect_mysql(args.host, args.port, args.user, args.password, args.db)
    try:
        with conn.cursor() as cur:
            ensure_table(cur)
            table_columns = get_table_columns(cur, "ml_feature_drift_result")

            # add baseline_sd automatically if missing
            if "baseline_sd" not in table_columns:
                try:
                    cur.execute("ALTER TABLE ml_feature_drift_result ADD COLUMN baseline_sd DECIMAL(20,6) NULL AFTER observed_value")
                    table_columns = get_table_columns(cur, "ml_feature_drift_result")
                    print("[ml_feature_drift_analyzer] added missing column: baseline_sd")
                except Exception:
                    # schema may be intentionally old; continue without it
                    table_columns = get_table_columns(cur, "ml_feature_drift_result")
                    print("[ml_feature_drift_analyzer] baseline_sd column missing; continuing without it")

            if args.truncate:
                cur.execute(
                    "DELETE FROM ml_feature_drift_result WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                    (args.profile_id, dt_from, dt_to),
                )

            total_rows = 0
            for target_dt in daterange(dt_from, dt_to):
                target_date = target_dt.strftime("%Y-%m-%d")
                hourly_baseline = load_hourly_baseline(cur, args.profile_id, target_date, args.baseline_days, features)
                hourly_observed = load_hourly_observed(cur, args.profile_id, target_date, features)
                rows = build_insert_rows(args.profile_id, target_date, hourly_observed, hourly_baseline, args.baseline_days, run_id)
                total_rows += insert_rows(cur, table_columns, rows)

        conn.commit()
        print(f"[OK] ml feature drift analyzer completed: rows={total_rows} run_id={run_id}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
