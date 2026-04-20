
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import random
import pymysql

STATIC_EXT_RE = re.compile(r'\.(css|js|png|jpg|jpeg|gif|ico|map|woff|woff2|ttf|eot|svg|webp|zip|txt)$', re.I)

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

def ensure_tables(cur):
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
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_event_batch (
          batch_ingest_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          raw_event_id BIGINT UNSIGNED NULL,
          dt DATE NOT NULL,
          ts DATETIME NOT NULL,
          event_name VARCHAR(100) NOT NULL,
          service_domain VARCHAR(50) NULL,
          funnel_stage VARCHAR(50) NULL,
          is_conversion TINYINT(1) NOT NULL DEFAULT 0,
          uid VARCHAR(128) NULL,
          pcid VARCHAR(128) NULL,
          sid VARCHAR(128) NULL,
          device_type VARCHAR(50) NULL,
          page_type VARCHAR(50) NULL,
          status INT NULL,
          latency_ms INT NULL,
          batch_dt DATE NOT NULL,
          parse_status VARCHAR(20) DEFAULT 'success',
          load_status VARCHAR(20) DEFAULT 'success',
          replay_source VARCHAR(50) DEFAULT 'stg_webserver_log_hit',
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (batch_ingest_id),
          KEY idx_batch_dt (batch_dt),
          KEY idx_domain_dt (service_domain, dt),
          KEY idx_uid (uid),
          KEY idx_sid (sid)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )

def has_column(cur, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND column_name = %s
        """,
        (table_name, column_name),
    )
    return int(cur.fetchone()["cnt"]) > 0

def infer_service_domain(path: str) -> str | None:
    p = (path or "").lower()
    if "/loan/" in p:
        return "loan"
    if "/card/" in p:
        return "card"
    if "/auth/" in p:
        return "auth"
    if "/deposit/" in p:
        return "deposit"
    if "/main" in p or p == "/" or p.startswith("/home"):
        return "main"
    return "common"

def infer_page_type(path: str) -> str | None:
    p = (path or "").lower()
    if "/apply" in p:
        return "apply"
    if "/submit" in p:
        return "submit"
    if "/success" in p:
        return "success"
    if "/detail" in p:
        return "detail"
    if "/list" in p:
        return "list"
    return "page"

def infer_funnel_stage(path: str) -> str | None:
    p = (path or "").lower()
    if p.endswith("/apply.do"):
        return "apply_start"
    if p.endswith("/submit.do"):
        return "apply_submit"
    if p.endswith("/success.do"):
        return "success"
    if "/view" in p or "/detail" in p:
        return "view"
    return "browse"

def infer_event_name(path: str, evt: str | None = None) -> str:
    p = (path or "").lower()
    e = (evt or "").lower()
    if "/card/" in p and p.endswith("/submit.do"):
        return "card_apply_submit"
    if "/card/" in p and p.endswith("/apply.do"):
        return "card_apply_start"
    if "/loan/" in p and p.endswith("/submit.do"):
        return "loan_apply_submit"
    if "/loan/" in p and p.endswith("/apply.do"):
        return "loan_apply_start"
    if "/loan/" in p:
        return "loan_view"
    if "/auth/" in p and p.endswith("/success.do"):
        return "auth_success"
    if "/auth/" in p and "otp" in p:
        return "otp_request"
    if "/auth/" in p:
        return "auth_attempt"
    if e == "view":
        return "page_view"
    return "page_view"

def infer_is_conversion(path: str) -> int:
    p = (path or "").lower()
    return 1 if (p.endswith("/submit.do") or p.endswith("/success.do")) else 0

def infer_device_type(ua: str | None) -> str | None:
    x = (ua or "").lower()
    if "iphone" in x or "android" in x or "mobile" in x:
        return "mobile"
    if "ipad" in x or "tablet" in x:
        return "tablet"
    return "desktop"

def extract_from_kv_raw(kv_raw: str | None, key: str) -> str | None:
    if not kv_raw:
        return None
    token = f"{key}="
    for part in str(kv_raw).split("&"):
        if part.startswith(token):
            return part[len(token):] or None
    return None

def is_page_event(row, page_event_mode: str) -> bool:
    path = (row.get("path") or "").lower()
    evt = (row.get("evt") or "").lower()
    page_type = (row.get("page_type") or "").strip()
    if STATIC_EXT_RE.search(path):
        return False
    if page_event_mode == "evt_or_page_type":
        return evt == "view" or page_type != "" or path != ""
    return evt == "view" or path != ""

def maybe_force_status_200(status: int, rate: float) -> int:
    if random.random() < rate:
        return 200
    return status

def main():
    ap = argparse.ArgumentParser(description="Collector A v3 source-first: stg_webserver_log_hit -> stg_wc_log_hit + stg_event_batch")
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
            ensure_tables(cur)

            if args.truncate_target:
                cur.execute("DELETE FROM stg_wc_log_hit WHERE dt BETWEEN %s AND %s", (args.dt_from, args.dt_to))
                cur.execute("DELETE FROM stg_event_batch WHERE dt BETWEEN %s AND %s", (args.dt_from, args.dt_to))

            select_cols = [
                "id", "dt", "ts", "ip", "method", "url_raw", "url_full", "url_norm",
                "host", "path", "query", "status", "bytes", "ref", "ua", "kv_raw", "uid"
            ]
            optional_cols = []
            for col in ["evt", "page_type", "service_domain", "funnel_stage", "is_conversion", "pcid", "sid", "device_type", "latency_ms"]:
                if has_column(cur, "stg_webserver_log_hit", col):
                    optional_cols.append(col)

            cur.execute(
                f"""
                SELECT {", ".join(select_cols + optional_cols)}
                FROM stg_webserver_log_hit
                WHERE dt BETWEEN %s AND %s
                ORDER BY dt, ts, id
                """,
                (args.dt_from, args.dt_to),
            )
            rows = cur.fetchall()

            wc_inserts = []
            batch_inserts = []

            for r in rows:
                if not is_page_event(r, args.page_event_mode):
                    continue
                if random.random() < args.drop_rate:
                    continue

                status = maybe_force_status_200(int(r["status"]), args.force_status_200_rate)
                path = r.get("path") or ""
                evt = r.get("evt")
                page_type = r.get("page_type") or infer_page_type(path)
                service_domain = r.get("service_domain") or infer_service_domain(path)
                funnel_stage = r.get("funnel_stage") or infer_funnel_stage(path)
                is_conversion = int(r.get("is_conversion") if r.get("is_conversion") is not None else infer_is_conversion(path))
                device_type = r.get("device_type") or infer_device_type(r.get("ua"))
                latency_ms = r.get("latency_ms")
                pcid = r.get("pcid") or extract_from_kv_raw(r.get("kv_raw"), "pcid")
                sid = r.get("sid") or extract_from_kv_raw(r.get("kv_raw"), "sid")
                event_name = infer_event_name(path, evt)

                wc_row = (
                    r["dt"], r["ts"], r["ip"], r["method"], r["url_raw"], r["url_full"], r["url_norm"],
                    r["host"], r["path"], r["query"], status, r["bytes"], r["ref"], r["ua"], r["kv_raw"], r["uid"]
                )
                batch_row = (
                    r["id"], r["dt"], r["ts"], event_name, service_domain, funnel_stage,
                    is_conversion, r["uid"], pcid, sid, device_type, page_type,
                    status, latency_ms, r["dt"], "success", "success", "stg_webserver_log_hit"
                )

                wc_inserts.append(wc_row)
                batch_inserts.append(batch_row)

                if random.random() < args.dup_rate:
                    wc_inserts.append(wc_row)
                    batch_inserts.append(batch_row)

            if wc_inserts:
                cur.executemany(
                    """
                    INSERT INTO stg_wc_log_hit
                    (dt, ts, ip, method, url_raw, url_full, url_norm, host, path, query,
                     status, bytes, ref, ua, kv_raw, uid)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    wc_inserts
                )

            if batch_inserts:
                cur.executemany(
                    """
                    INSERT INTO stg_event_batch
                    (raw_event_id, dt, ts, event_name, service_domain, funnel_stage, is_conversion,
                     uid, pcid, sid, device_type, page_type, status, latency_ms,
                     batch_dt, parse_status, load_status, replay_source)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    batch_inserts
                )

        conn.commit()
        print(f"[collector_a_v3_source_first] source_rows={len(rows)} wc_rows={len(wc_inserts)} batch_rows={len(batch_inserts)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
