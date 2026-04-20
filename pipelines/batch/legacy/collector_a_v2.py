from __future__ import annotations

import argparse
import pymysql


def connect_mysql(host: str, port: int, user: str, password: str, db: str):
    return pymysql.connect(
        host=host, port=port, user=user, password=password, database=db,
        charset="utf8mb4", autocommit=False, cursorclass=pymysql.cursors.DictCursor
    )


def ensure_table(cur) -> None:
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


def main() -> None:
    ap = argparse.ArgumentParser(description="Collector A: transform stg_webserver_log_hit -> stg_wc_log_hit")
    ap.add_argument("--db-host", default="127.0.0.1")
    ap.add_argument("--db-port", type=int, default=3306)
    ap.add_argument("--db-user", required=True)
    ap.add_argument("--db-pass", default="")
    ap.add_argument("--db-name", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--truncate-target", action="store_true")
    args = ap.parse_args()

    conn = connect_mysql(args.db_host, args.db_port, args.db_user, args.db_pass, args.db_name)
    try:
        with conn.cursor() as cur:
            ensure_table(cur)
            if args.truncate_target:
                cur.execute(
                    "DELETE FROM stg_wc_log_hit WHERE dt BETWEEN %s AND %s",
                    (args.dt_from, args.dt_to),
                )
                conn.commit()

            cur.execute(
                """
                SELECT dt, ts, ip, method, url_raw, url_full, url_norm, host, path, query,
                       status, bytes, ref, ua, kv_raw, uid
                FROM stg_webserver_log_hit
                WHERE dt BETWEEN %s AND %s
                ORDER BY dt, ts
                """,
                (args.dt_from, args.dt_to),
            )
            rows = cur.fetchall()

            inserts = []
            for r in rows:
                inserts.append(
                    (
                        r["dt"], r["ts"], r["ip"], r["method"], r["url_raw"], r["url_full"], r["url_norm"],
                        r["host"], r["path"], r["query"], r["status"], r["bytes"], r["ref"], r["ua"], r["kv_raw"], r["uid"]
                    )
                )

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
        print(f"[collector_a_v2] loaded rows={len(inserts)} dt_from={args.dt_from} dt_to={args.dt_to}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
