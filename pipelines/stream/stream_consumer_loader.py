from __future__ import annotations

import argparse
import random
from datetime import timedelta

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


def ensure_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_event_stream (
          stream_ingest_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          raw_event_id BIGINT UNSIGNED NOT NULL,
          dt DATE NOT NULL,
          ts DATETIME NOT NULL,
          event_name VARCHAR(100) NOT NULL,
          service_domain VARCHAR(50) NULL,
          funnel_stage VARCHAR(50) NULL,
          is_conversion TINYINT(1) NOT NULL DEFAULT 0,
          uid VARCHAR(128) NULL,
          pcid VARCHAR(128) NULL,
          sid VARCHAR(128) NULL,
          stream_topic VARCHAR(100) NULL,
          stream_partition INT NULL,
          stream_offset BIGINT NULL,
          sequence_no BIGINT NULL,
          producer_ts DATETIME NULL,
          ingest_ts DATETIME NOT NULL,
          event_delay_ms BIGINT NULL,
          status INT NULL,
          latency_ms INT NULL,
          load_status VARCHAR(20) DEFAULT 'loaded',
          anomaly_tag VARCHAR(50) NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (stream_ingest_id),
          KEY idx_stg_event_stream_dt (dt),
          KEY idx_stg_event_stream_ingest_ts (ingest_ts),
          KEY idx_stg_event_stream_domain_ts (service_domain, ingest_ts),
          KEY idx_stg_event_stream_event_name (event_name),
          KEY idx_stg_event_stream_sid (sid)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def infer_event_name(r: dict) -> str:
    path = (r.get("path") or "").lower()
    evt = (r.get("evt") or "").lower()

    if "/card/" in path and path.endswith("/submit.do"):
        return "card_apply_submit"
    if "/card/" in path and path.endswith("/apply.do"):
        return "card_apply_start"
    if "/loan/" in path and path.endswith("/submit.do"):
        return "loan_apply_submit"
    if "/loan/" in path and path.endswith("/apply.do"):
        return "loan_apply_start"
    if "/loan/" in path:
        return "loan_view"
    if "/auth/" in path and path.endswith("/success.do"):
        return "auth_success"
    if "/auth/" in path and "otp" in path:
        return "otp_request"
    if "/auth/" in path:
        return "auth_attempt"
    if evt == "view":
        return "page_view"
    return "other"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-host", default="127.0.0.1")
    ap.add_argument("--db-port", type=int, default=3306)
    ap.add_argument("--db-user", required=True)
    ap.add_argument("--db-pass", default="")
    ap.add_argument("--db-name", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--truncate-target", action="store_true")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--max-delay-sec", type=int, default=5)
    args = ap.parse_args()

    random.seed(args.seed)

    conn = connect_mysql(args)
    try:
        with conn.cursor() as cur:
            ensure_table(cur)

            if args.truncate_target:
                cur.execute(
                    "DELETE FROM stg_event_stream WHERE dt BETWEEN %s AND %s",
                    (args.dt_from, args.dt_to),
                )

            cur.execute(
                """
                SELECT *
                FROM event_log_raw
                WHERE dt BETWEEN %s AND %s
                ORDER BY dt, ts, raw_event_id
                """,
                (args.dt_from, args.dt_to),
            )
            rows = cur.fetchall()

            inserts = []
            for i, r in enumerate(rows, start=1):
                delay_sec = random.randint(0, args.max_delay_sec)
                ingest_ts = r["ts"] + timedelta(seconds=delay_sec)
                event_delay_ms = delay_sec * 1000

                inserts.append(
                    (
                        r["raw_event_id"],
                        r["dt"],
                        r["ts"],
                        infer_event_name(r),
                        r["service_domain"],
                        r["funnel_stage"],
                        r["is_conversion"],
                        r["uid"],
                        r["pcid"],
                        r["sid"],
                        f"{r['service_domain'] or 'other'}_event_topic",
                        random.randint(0, 2),
                        i - 1,
                        i,
                        r["ts"],
                        ingest_ts,
                        event_delay_ms,
                        r["status"],
                        r["latency_ms"],
                        "loaded",
                        None,
                    )
                )

            if inserts:
                cur.executemany(
                    """
                    INSERT INTO stg_event_stream (
                      raw_event_id, dt, ts, event_name, service_domain, funnel_stage, is_conversion,
                      uid, pcid, sid, stream_topic, stream_partition, stream_offset, sequence_no,
                      producer_ts, ingest_ts, event_delay_ms, status, latency_ms, load_status, anomaly_tag
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    inserts,
                )

        conn.commit()
        print(f"[stream_consumer_loader] loaded rows={len(inserts)} dt_from={args.dt_from} dt_to={args.dt_to}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
