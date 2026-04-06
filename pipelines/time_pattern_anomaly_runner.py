from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from decimal import Decimal
from statistics import mean, pstdev

import pymysql


TARGET_METRICS = [
    "daily_active_users",
    "page_view_count",
    "collector_event_count",
    "raw_event_count",
    "estimated_missing_rate",
    "auth_success_rate",
    "card_apply_submit_rate",
]


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


def ensure_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS metric_time_anomaly_day (
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          metric_name VARCHAR(100) NOT NULL,
          metric_group VARCHAR(50) NULL,
          source_layer VARCHAR(50) NULL,
          observed_value DECIMAL(20,6) NULL,
          rolling_avg_7d DECIMAL(20,6) NULL,
          rolling_std_7d DECIMAL(20,6) NULL,
          zscore_7d DECIMAL(20,6) NULL,
          anomaly_status VARCHAR(20) NOT NULL,
          severity VARCHAR(20) NULL,
          note VARCHAR(255) NULL,
          run_id VARCHAR(64) NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (profile_id, dt, metric_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def q(v) -> str:
    return str(Decimal(str(v)).quantize(Decimal("0.000001")))


def main() -> None:
    ap = argparse.ArgumentParser(description="Rolling 7-day time anomaly runner")
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
    run_id = f"timeanom_{args.profile_id}_{dates[0].replace('-', '')}_{dates[-1].replace('-', '')}"

    conn = connect_mysql(args)
    try:
        with conn.cursor() as cur:
            ensure_table(cur)
            if args.truncate:
                cur.execute(
                    "DELETE FROM metric_time_anomaly_day WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                    (args.profile_id, dates[0], dates[-1]),
                )

            for dt in dates:
                dt_obj = datetime.strptime(dt, "%Y-%m-%d").date()
                start_hist = (dt_obj - timedelta(days=7)).isoformat()
                end_hist = (dt_obj - timedelta(days=1)).isoformat()

                rows = []
                for metric in TARGET_METRICS:
                    cur.execute(
                        """
                        SELECT dt, metric_name, metric_group, source_layer, metric_value
                        FROM metric_value_day
                        WHERE profile_id=%s
                          AND metric_name=%s
                          AND dt BETWEEN %s AND %s
                        ORDER BY dt
                        """,
                        (args.profile_id, metric, start_hist, end_hist),
                    )
                    hist = cur.fetchall()

                    cur.execute(
                        """
                        SELECT metric_name, metric_group, source_layer, metric_value
                        FROM metric_value_day
                        WHERE profile_id=%s
                          AND metric_name=%s
                          AND dt=%s
                        """,
                        (args.profile_id, metric, dt),
                    )
                    obs = cur.fetchone()
                    if not obs:
                        continue

                    values = [float(r["metric_value"]) for r in hist if r["metric_value"] is not None]
                    observed = float(obs["metric_value"])
                    if values:
                        avg = mean(values)
                        std = pstdev(values) if len(values) > 1 else 0.0
                    else:
                        avg = observed
                        std = 0.0

                    if std == 0:
                        zscore = 0.0 if avg == observed else (observed - avg)
                    else:
                        zscore = (observed - avg) / std

                    abs_z = abs(zscore)
                    if abs_z >= 3:
                        status = "alert"
                        severity = "high"
                    elif abs_z >= 2:
                        status = "warn"
                        severity = "medium"
                    else:
                        status = "normal"
                        severity = "low"

                    note = f"rolling_7d avg={avg:.4f} std={std:.4f}"
                    rows.append(
                        (
                            args.profile_id,
                            dt,
                            metric,
                            obs.get("metric_group"),
                            obs.get("source_layer"),
                            q(observed),
                            q(avg),
                            q(std),
                            q(zscore),
                            status,
                            severity,
                            note,
                            run_id,
                        )
                    )

                if rows:
                    cur.executemany(
                        """
                        INSERT INTO metric_time_anomaly_day
                        (profile_id, dt, metric_name, metric_group, source_layer, observed_value,
                         rolling_avg_7d, rolling_std_7d, zscore_7d, anomaly_status, severity, note, run_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                          metric_group=VALUES(metric_group),
                          source_layer=VALUES(source_layer),
                          observed_value=VALUES(observed_value),
                          rolling_avg_7d=VALUES(rolling_avg_7d),
                          rolling_std_7d=VALUES(rolling_std_7d),
                          zscore_7d=VALUES(zscore_7d),
                          anomaly_status=VALUES(anomaly_status),
                          severity=VALUES(severity),
                          note=VALUES(note),
                          run_id=VALUES(run_id)
                        """,
                        rows,
                    )
        conn.commit()
        print(f"[OK] time pattern anomaly completed: run_id={run_id}, dates={len(dates)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
