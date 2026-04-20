# pipelines/batch/collector_a_v3.py

from __future__ import annotations

import argparse
import random
import re
import pymysql

STATIC_EXT_RE = re.compile(r"\.(css|js|png|jpg|jpeg|gif|ico|map|woff|woff2|ttf|eot|svg|webp|zip|txt)$", re.I)


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
        CREATE TABLE IF NOT EXISTS stg_wc_log_hit (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          dt DATE NOT NULL,
          ts DATETIME NOT NULL,
          ip VARCHAR(45) NOT NULL,
          method VARCHAR(10) NOT NULL,
          url_raw TEXT NOT NULL,
          url_full TEXT NOT NULL,
          url_norm TEXT NOT NULL,
          host VARCHAR(255) NULL,
          path VARCHAR(2048) NULL,
          query TEXT NULL,
          status INT NOT NULL,
          bytes BIGINT NULL,
          ref TEXT NULL,
          ua TEXT NULL,
          kv_raw TEXT NULL,
          uid VARCHAR(128) NULL,
          PRIMARY KEY (id),
          KEY idx_dt_ts (dt, ts),
          KEY idx_uid (uid)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def is_page_event(row, page_event_mode: str) -> bool:
    evt = (row.get("evt") or "").lower()
    page_type = (row.get("page_type") or "").strip()
    path = (row.get("path") or "").lower()

    if STATIC_EXT_RE.search(path):
        return False

    if page_event_mode == "evt_or_page_type":
        return evt == "view" or page_type != ""
    return evt == "view"


def maybe_force_status_200(status: int, rate: float) -> int:
    if random.random() < rate:
        return 200
    return status


def main():
    ap = argparse.ArgumentParser(description="Collector A v3 source-first: stg_webserver_log_hit -> stg_wc_log_hit")
    ap.add_argument("--db-host", default="127.0.0.1")
    ap.add_argument("--db-port", type=int, default=3306)
    ap.add_argument("--db-user", required=True)
    ap.add_argument("--db-pass", default="")
    ap.add_argument("--db-name", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--drop-rate", type=float, default=0.05)
    ap.add_argument("--dup-rate", type=float, default=0.01)
    ap.add_argument("--force-status-200-rate", type=float, default=0.0)
    ap.add_argument("--page-event-mode", choices=["view_only", "evt_or_page_type"], default="evt_or_page_type")
    ap.add_argument("--truncate-target", action="store_true")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    random.seed(args.seed)

    conn = connect_mysql(args)
    try:
        with conn.cursor() as cur:
            ensure_table(cur)

            if args.truncate_target:
                cur.execute(
                    "DELETE FROM stg_wc_log_hit WHERE dt BETWEEN %s AND %s",
                    (args.dt_from, args.dt_to),
                )

            cur.execute(
                """
                SELECT
                    dt, ts, ip, method, url_raw, url_full, url_norm, host, path, query,
                    status, bytes, ref, ua, kv_raw, uid, evt, page_type
                FROM stg_webserver_log_hit
                WHERE dt BETWEEN %s AND %s
                ORDER BY dt, ts
                """,
                (args.dt_from, args.dt_to),
            )
            rows = cur.fetchall()

            inserts = []
            source_rows = len(rows)

            for r in rows:
                if not is_page_event(r, args.page_event_mode):
                    continue

                if random.random() < args.drop_rate:
                    continue

                status = maybe_force_status_200(int(r["status"]), args.force_status_200_rate)

                wc_row = (
                    r["dt"],
                    r["ts"],
                    r["ip"],
                    r["method"],
                    r["url_raw"],
                    r["url_full"],
                    r["url_norm"],
                    r["host"],
                    r["path"],
                    r["query"],
                    status,
                    r["bytes"],
                    r["ref"],
                    r["ua"],
                    r["kv_raw"],
                    r["uid"],
                )
                inserts.append(wc_row)

                if random.random() < args.dup_rate:
                    inserts.append(wc_row)

            if inserts:
                cur.executemany(
                    """
                    INSERT INTO stg_wc_log_hit
                    (dt, ts, ip, method, url_raw, url_full, url_norm, host, path, query,
                     status, bytes, ref, ua, kv_raw, uid)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    inserts,
                )

        conn.commit()
        print(f"[collector_a_v3_source_first] source_rows={source_rows} wc_rows={len(inserts)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
