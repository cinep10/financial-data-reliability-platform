from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from decimal import Decimal
import pymysql

def daterange(start: str, end: str):
    cur = datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end, "%Y-%m-%d").date()
    while cur <= end_dt:
        yield cur.isoformat()
        cur += timedelta(days=1)

def connect_mysql(args):
    return pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )

def ensure_tables(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS data_risk_root_cause_day (
      profile_id VARCHAR(64) NOT NULL,
      dt DATE NOT NULL,
      cause_rank INT NOT NULL,
      cause_type VARCHAR(50) NOT NULL,
      cause_code VARCHAR(100) NOT NULL,
      confidence DECIMAL(8,4) NOT NULL DEFAULT 0,
      driver_source VARCHAR(50) NOT NULL,
      related_metric VARCHAR(100) NULL,
      observed_value DECIMAL(20,6) NULL,
      baseline_value DECIMAL(20,6) NULL,
      detail VARCHAR(255) NULL,
      run_id VARCHAR(64) NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (profile_id, dt, cause_rank)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS risk_signal_link_day (
      profile_id VARCHAR(64) NOT NULL,
      dt DATE NOT NULL,
      signal_group VARCHAR(50) NOT NULL,
      signal_name VARCHAR(100) NOT NULL,
      signal_count INT NOT NULL DEFAULT 0,
      weighted_contribution DECIMAL(20,6) NOT NULL DEFAULT 0,
      severity VARCHAR(20) NULL,
      note VARCHAR(255) NULL,
      run_id VARCHAR(64) NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (profile_id, dt, signal_group, signal_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

def d(v):
    return Decimal(str(v or 0))

def main():
    ap = argparse.ArgumentParser(description="Root cause analyzer for data reliability/risk")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--date")
    ap.add_argument("--dt-from")
    ap.add_argument("--dt-to")
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    dates = [args.date] if args.date else list(daterange(args.dt_from, args.dt_to))
    run_id = f"rca_{args.profile_id}_{dates[0].replace('-', '')}_{dates[-1].replace('-', '')}"

    conn = connect_mysql(args)
    try:
        with conn.cursor() as cur:
            ensure_tables(cur)
            if args.truncate:
                cur.execute(
                    "DELETE FROM data_risk_root_cause_day WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                    (args.profile_id, dates[0], dates[-1]),
                )
                cur.execute(
                    "DELETE FROM risk_signal_link_day WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                    (args.profile_id, dates[0], dates[-1]),
                )

            for dt in dates:
                cur.execute("""
                    SELECT risk_score, risk_status, validation_fail_count, validation_warn_count,
                           drift_alert_count, drift_warn_count,
                           ml_feature_alert_count, ml_feature_warn_count
                    FROM data_risk_score_day_v3
                    WHERE profile_id=%s AND dt=%s
                """, (args.profile_id, dt))
                risk = cur.fetchone() or {}

                links = []
                weights = {
                    "validation_fail_count": Decimal("5"),
                    "validation_warn_count": Decimal("2"),
                    "drift_alert_count": Decimal("4"),
                    "drift_warn_count": Decimal("2"),
                    "ml_feature_alert_count": Decimal("3"),
                    "ml_feature_warn_count": Decimal("1"),
                }
                for k, w in weights.items():
                    val = int(risk.get(k) or 0)
                    if val > 0:
                        grp = "validation" if k.startswith("validation") else ("ml_feature" if k.startswith("ml_") else "drift")
                        sev = "high" if "alert" in k or "fail" in k else "medium"
                        links.append((args.profile_id, dt, grp, k, val, float(d(val) * w), sev, "daily weighted contribution", run_id))

                if links:
                    cur.executemany("""
                        INSERT INTO risk_signal_link_day
                        (profile_id, dt, signal_group, signal_name, signal_count, weighted_contribution, severity, note, run_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                          signal_count=VALUES(signal_count),
                          weighted_contribution=VALUES(weighted_contribution),
                          severity=VALUES(severity),
                          note=VALUES(note),
                          run_id=VALUES(run_id)
                    """, links)

                candidates = []

                cur.execute("""
                    SELECT metric_name, baseline_value, observed_value, drift_score, drift_status
                    FROM metric_drift_result_r
                    WHERE profile_id=%s AND dt=%s AND drift_status IN ('alert','warn')
                    ORDER BY
                      CASE drift_status WHEN 'alert' THEN 2 WHEN 'warn' THEN 1 ELSE 0 END DESC,
                      ABS(drift_score) DESC
                    LIMIT 20
                """, (args.profile_id, dt))
                for r in cur.fetchall():
                    metric = r["metric_name"]
                    obs = d(r["observed_value"])
                    base = d(r["baseline_value"])
                    diff = obs - base
                    code = "traffic_drop" if metric in ("page_view_count","raw_event_count","collector_event_count","daily_active_users") and diff < 0 else (
                           "funnel_distortion" if metric in ("loan_apply_start_count","loan_view_count","card_apply_start_count") else
                           "quality_shift" if metric == "estimated_missing_rate" else
                           "metric_drift")
                    conf = min(abs(float(r["drift_score"])) / 10.0, 0.99)
                    detail = f"{metric}: observed={obs} baseline={base} drift={r['drift_score']}"
                    candidates.append((code, r["drift_status"], conf, "metric_drift_result_r", metric, float(obs), float(base), detail))

                cur.execute("""
                    SELECT rule_name, metric_name, validation_status, observed_value, expected_value
                    FROM validation_result
                    WHERE profile_id=%s AND dt=%s AND validation_status IN ('warn','fail')
                    ORDER BY
                      CASE validation_status WHEN 'fail' THEN 2 WHEN 'warn' THEN 1 ELSE 0 END DESC,
                      ABS(COALESCE(diff_value,0)) DESC
                    LIMIT 20
                """, (args.profile_id, dt))
                for r in cur.fetchall():
                    code = "mapping_gap" if "needs_" in r["rule_name"] else "validation_rule_breach"
                    conf = 0.9 if r["validation_status"] == "fail" else 0.7
                    detail = f"{r['rule_name']}: observed={r['observed_value']} expected={r['expected_value']}"
                    candidates.append((code, r["validation_status"], conf, "validation_result", r["metric_name"], float(d(r["observed_value"])), float(d(r["expected_value"])), detail))

                candidates.sort(key=lambda x: (x[2], x[0]), reverse=True)
                top = candidates[:5]

                rows = []
                for i, c in enumerate(top, start=1):
                    cause_type, cause_status, conf, src, metric, obs, base, detail = c
                    rows.append((args.profile_id, dt, i, cause_type, cause_status, round(conf,4), src, metric, obs, base, detail[:255], run_id))

                if rows:
                    cur.executemany("""
                        INSERT INTO data_risk_root_cause_day
                        (profile_id, dt, cause_rank, cause_type, cause_code, confidence, driver_source, related_metric, observed_value, baseline_value, detail, run_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                          cause_type=VALUES(cause_type),
                          cause_code=VALUES(cause_code),
                          confidence=VALUES(confidence),
                          driver_source=VALUES(driver_source),
                          related_metric=VALUES(related_metric),
                          observed_value=VALUES(observed_value),
                          baseline_value=VALUES(baseline_value),
                          detail=VALUES(detail),
                          run_id=VALUES(run_id)
                    """, rows)
        conn.commit()
        print(f"[OK] root cause analysis completed run_id={run_id} dates={len(dates)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
