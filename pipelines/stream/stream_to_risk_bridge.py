from __future__ import annotations

import argparse
import pymysql


def connect_mysql(args):
    return pymysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_pass,
        database=args.db_name,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_columns(cur):
    try:
        cur.execute("ALTER TABLE data_risk_score_day_v3 ADD COLUMN stream_risk_score DECIMAL(18,6) NULL")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE data_risk_score_day_v3 ADD COLUMN stream_primary_issue VARCHAR(50) NULL")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE data_risk_score_day_v3 ADD COLUMN stream_status VARCHAR(20) NULL")
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-host", default="127.0.0.1")
    ap.add_argument("--db-port", type=int, default=3306)
    ap.add_argument("--db-user", required=True)
    ap.add_argument("--db-pass", default="")
    ap.add_argument("--db-name", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    args = ap.parse_args()

    conn = connect_mysql(args)
    try:
        with conn.cursor() as cur:
            ensure_columns(cur)

            cur.execute(
                """
UPDATE data_risk_score_day_v3 d
JOIN (
    SELECT
        profile_id,
        dt,
        MAX(stream_risk_score) AS max_stream_risk_score
    FROM stream_risk_signal_day
    WHERE profile_id = %s
      AND dt BETWEEN %s AND %s
    GROUP BY profile_id, dt
) m
  ON d.profile_id = m.profile_id
 AND d.dt = m.dt
JOIN stream_risk_signal_day s
  ON s.profile_id = m.profile_id
 AND s.dt = m.dt
 AND s.stream_risk_score = m.max_stream_risk_score
SET
    d.stream_risk_score = s.stream_risk_score,
    d.stream_primary_issue = s.primary_stream_issue,
    d.stream_status = s.status
WHERE d.profile_id = %s
  AND d.dt BETWEEN %s AND %s;
                """,
                (args.profile_id, args.dt_from, args.dt_to,
                  args.profile_id, args.dt_from, args.dt_to),
            )

        conn.commit()
        print("[stream_to_risk_bridge] done")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
