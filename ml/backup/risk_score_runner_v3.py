from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import pymysql


def status_from_score(score: float) -> str:
    if score >= 1000:
        return "alert"
    if score >= 600:
        return "warning"
    return "normal"


def date_range(start: str, end: str):
    dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while dt <= end_dt:
        yield dt.strftime("%Y-%m-%d")
        dt += timedelta(days=1)


def get_table_columns(cur, table_name: str) -> set[str]:
    cur.execute(f"SHOW COLUMNS FROM {table_name}")
    return {row["Field"] for row in cur.fetchall()}


def ensure_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS data_risk_score_day_v3 (
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          risk_score DECIMAL(18,6) NOT NULL DEFAULT 0,
          risk_status VARCHAR(20) NOT NULL,
          validation_fail_count INT NOT NULL DEFAULT 0,
          validation_warn_count INT NOT NULL DEFAULT 0,
          drift_alert_count INT NOT NULL DEFAULT 0,
          drift_warn_count INT NOT NULL DEFAULT 0,
          ml_feature_alert_count INT NOT NULL DEFAULT 0,
          ml_feature_warn_count INT NOT NULL DEFAULT 0,
          total_signal_count INT NOT NULL DEFAULT 0,
          avg_abs_drift DECIMAL(18,6) NULL,
          max_drift DECIMAL(18,6) NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (profile_id, dt)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def ensure_optional_columns(cur) -> None:
    cols = get_table_columns(cur, "data_risk_score_day_v3")

    if "avg_abs_drift" not in cols:
        try:
            cur.execute(
                "ALTER TABLE data_risk_score_day_v3 "
                "ADD COLUMN avg_abs_drift DECIMAL(18,6) NULL AFTER total_signal_count"
            )
            print("[risk_score_runner_v3] added missing column: avg_abs_drift")
        except Exception:
            pass

    cols = get_table_columns(cur, "data_risk_score_day_v3")
    if "max_drift" not in cols:
        try:
            cur.execute(
                "ALTER TABLE data_risk_score_day_v3 "
                "ADD COLUMN max_drift DECIMAL(18,6) NULL AFTER avg_abs_drift"
            )
            print("[risk_score_runner_v3] added missing column: max_drift")
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser(description="Risk Score Runner v3 (schema-safe)")
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

    if args.date:
        dates = [args.date]
    elif args.dt_from and args.dt_to:
        dates = list(date_range(args.dt_from, args.dt_to))
    else:
        raise ValueError("Provide --date or --dt-from/--dt-to")

    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )

    try:
        with conn.cursor() as cur:
            ensure_table(cur)
            ensure_optional_columns(cur)
            table_cols = get_table_columns(cur, "data_risk_score_day_v3")

            if args.truncate:
                cur.execute(
                    "DELETE FROM data_risk_score_day_v3 "
                    "WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                    (args.profile_id, dates[0], dates[-1]),
                )
                conn.commit()

            rows = []
            for dt in dates:
                cur.execute(
                    """
                    SELECT
                        COALESCE(SUM(fail_count), 0) AS fail_count,
                        COALESCE(SUM(warn_count), 0) AS warn_count
                    FROM validation_summary_day
                    WHERE profile_id=%s AND dt=%s
                    """,
                    (args.profile_id, dt),
                )
                v = cur.fetchone() or {}
                validation_fail = int(v.get("fail_count") or 0)
                validation_warn = int(v.get("warn_count") or 0)

                cur.execute(
                    """
                    SELECT
                        COALESCE(SUM(CASE WHEN drift_status='alert' THEN 1 ELSE 0 END), 0) AS alert_count,
                        COALESCE(SUM(CASE WHEN drift_status='warn' THEN 1 ELSE 0 END), 0) AS warn_count
                    FROM metric_drift_result_r
                    WHERE profile_id=%s AND dt=%s
                    """,
                    (args.profile_id, dt),
                )
                d = cur.fetchone() or {}
                drift_alert = int(d.get("alert_count") or 0)
                drift_warn = int(d.get("warn_count") or 0)

                cur.execute(
                    """
                    SELECT
                        COALESCE(SUM(CASE WHEN drift_status='alert' THEN 1 ELSE 0 END), 0) AS alert_count,
                        COALESCE(SUM(CASE WHEN drift_status='warn' THEN 1 ELSE 0 END), 0) AS warn_count,
                        COALESCE(COUNT(*), 0) AS total_count,
                        COALESCE(AVG(ABS(drift_score)), 0) AS avg_abs_drift,
                        COALESCE(MAX(ABS(drift_score)), 0) AS max_drift
                    FROM ml_feature_drift_result
                    WHERE profile_id=%s AND dt=%s
                    """,
                    (args.profile_id, dt),
                )
                m = cur.fetchone() or {}
                ml_alert = int(m.get("alert_count") or 0)
                ml_warn = int(m.get("warn_count") or 0)
                total_signal = int(m.get("total_count") or 0)
                avg_abs_drift = float(m.get("avg_abs_drift") or 0.0)
                max_drift = float(m.get("max_drift") or 0.0)

                risk_score = (
                    validation_fail * 5
                    + validation_warn * 2
                    + drift_alert * 4
                    + drift_warn * 2
                    + ml_alert * 3
                    + ml_warn * 1
                )
                risk_status = status_from_score(risk_score)

                rows.append(
                    {
                        "dt": dt,
                        "profile_id": args.profile_id,
                        "risk_score": risk_score,
                        "risk_status": risk_status,
                        "validation_fail_count": validation_fail,
                        "validation_warn_count": validation_warn,
                        "drift_alert_count": drift_alert,
                        "drift_warn_count": drift_warn,
                        "ml_feature_alert_count": ml_alert,
                        "ml_feature_warn_count": ml_warn,
                        "total_signal_count": total_signal,
                        "avg_abs_drift": round(avg_abs_drift, 6),
                        "max_drift": round(max_drift, 6),
                    }
                )

            ordered_cols = [
                "dt",
                "profile_id",
                "risk_score",
                "risk_status",
                "validation_fail_count",
                "validation_warn_count",
                "drift_alert_count",
                "drift_warn_count",
                "ml_feature_alert_count",
                "ml_feature_warn_count",
                "total_signal_count",
                "avg_abs_drift",
                "max_drift",
            ]
            cols = [c for c in ordered_cols if c in table_cols]
            placeholders = ",".join(["%s"] * len(cols))
            updates = ",".join(
                [f"{c}=VALUES({c})" for c in cols if c not in ("dt", "profile_id")]
            )

            sql = f"""
                INSERT INTO data_risk_score_day_v3 ({",".join(cols)})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {updates}
            """

            vals = [tuple(r[c] for c in cols) for r in rows]
            if vals:
                cur.executemany(sql, vals)

        conn.commit()
        print(f"[risk_score_runner_v3] inserted/updated rows: {len(rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
