from __future__ import annotations

import argparse
from datetime import datetime

import pymysql


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


def get_table_columns(conn, table_name: str) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute(f"DESC {table_name}")
        rows = cur.fetchall()
    return {r["Field"].lower(): r["Type"].lower() for r in rows}


def choose_run_id(col_type: str | None, profile_id: str, dt: str):
    now = datetime.now()
    if col_type and any(x in col_type for x in ["bigint", "int", "smallint", "tinyint", "decimal", "numeric"]):
        return int(now.strftime("%Y%m%d%H%M%S"))
    return f"risk-{profile_id}-{dt}-{now.strftime('%Y%m%d%H%M%S')}"


def fetch_one(cur, sql: str, params: tuple):
    cur.execute(sql, params)
    row = cur.fetchone()
    return row or {}


def main():
    ap = argparse.ArgumentParser(description="Daily data risk score runner for MySQL")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--date", required=True)
    args = ap.parse_args()

    conn = connect_mysql(args.host, args.port, args.user, args.password, args.db)
    try:
        cols = get_table_columns(conn, "data_risk_score_day")
        run_id = choose_run_id(cols.get("run_id"), args.profile_id, args.date)

        with conn.cursor() as cur:
            val = fetch_one(
                cur,
                """
                SELECT
                  COALESCE(pass_count, 0) AS pass_count,
                  COALESCE(warn_count, 0) AS warn_count,
                  COALESCE(fail_count, 0) AS fail_count
                FROM validation_summary_day
                WHERE profile_id = %s
                  AND dt = %s
                """,
                (args.profile_id, args.date),
            )

            drift = fetch_one(
                cur,
                """
                SELECT
                  COALESCE(SUM(CASE WHEN drift_status = 'alert' THEN 1 ELSE 0 END), 0) AS alert_count,
                  COALESCE(SUM(CASE WHEN drift_status = 'warn' THEN 1 ELSE 0 END), 0) AS warn_count
                FROM metric_drift_result_r
                WHERE profile_id = %s
                  AND dt = %s
                """,
                (args.profile_id, args.date),
            )

            fail_count = int(val.get("fail_count", 0) or 0)
            val_warn = int(val.get("warn_count", 0) or 0)
            drift_alert = int(drift.get("alert_count", 0) or 0)
            drift_warn = int(drift.get("warn_count", 0) or 0)

            risk_score = fail_count * 5 + val_warn * 2 + drift_alert * 3 + drift_warn * 1
            if risk_score >= 6:
                risk_status = "alert"
            elif risk_score >= 3:
                risk_status = "warning"
            else:
                risk_status = "normal"

            upsert_sql = """
                INSERT INTO data_risk_score_day
                (
                  profile_id, dt,
                  validation_fail_count, validation_warn_count,
                  drift_alert_count, drift_warn_count,
                  risk_score, risk_status, run_id
                )
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  validation_fail_count = VALUES(validation_fail_count),
                  validation_warn_count = VALUES(validation_warn_count),
                  drift_alert_count = VALUES(drift_alert_count),
                  drift_warn_count = VALUES(drift_warn_count),
                  risk_score = VALUES(risk_score),
                  risk_status = VALUES(risk_status),
                  run_id = VALUES(run_id)
            """

            cur.execute(
                upsert_sql,
                (
                    args.profile_id,
                    args.date,
                    fail_count,
                    val_warn,
                    drift_alert,
                    drift_warn,
                    risk_score,
                    risk_status,
                    run_id,
                ),
            )

        conn.commit()
        print(
            f"[OK] risk score completed: run_id={run_id}, "
            f"risk_score={risk_score}, risk_status={risk_status}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
