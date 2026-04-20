#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from typing import Optional

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


def extract_kv(kv_raw: Optional[str], key: str) -> Optional[str]:
    if not kv_raw:
        return None
    prefix = f"{key}="
    for part in str(kv_raw).split("&"):
        if part.startswith(prefix):
            value = part[len(prefix):].strip()
            return value or None
    return None


def infer_service_domain(path: Optional[str], host: Optional[str] = None) -> str:
    p = (path or "").lower()
    h = (host or "").lower()
    if "/loan/" in p or "loan" in h:
        return "loan"
    if "/card/" in p or "card" in h:
        return "card"
    if "/auth/" in p or "auth" in h or "/login" in p:
        return "auth"
    if "/account/" in p:
        return "account"
    if "/customer/" in p:
        return "customer"
    if "/branch/" in p:
        return "branch"
    if "/transfer/" in p:
        return "transfer"
    if p == "/" or p.startswith("/main") or p.startswith("/home"):
        return "main"
    return "other"


def infer_page_type(path: Optional[str]) -> Optional[str]:
    p = (path or "").lower()
    if "/apply" in p:
        return "apply"
    if "/submit" in p:
        return "submit"
    if "/success" in p or "/complete" in p:
        return "success"
    if "/detail" in p or "/view" in p:
        return "detail"
    if "/list" in p:
        return "list"
    if p:
        return "page"
    return None


def infer_device_type(ua: Optional[str]) -> Optional[str]:
    x = (ua or "").lower()
    if not x:
        return None
    if "iphone" in x or "android" in x or "mobile" in x:
        return "mobile"
    if "ipad" in x or "tablet" in x:
        return "tablet"
    return "desktop"


def infer_evt(path: Optional[str]) -> str:
    p = (path or "").lower()
    if p.endswith("/submit.do") or "/submit" in p:
        return "submit"
    if p.endswith("/success.do") or "/success" in p:
        return "success"
    return "view"


def infer_funnel_stage(path: Optional[str]) -> str:
    p = (path or "").lower()
    if p.endswith("/apply.do") or "/apply" in p:
        return "apply_start"
    if p.endswith("/submit.do") or "/submit" in p:
        return "apply_submit"
    if p.endswith("/success.do") or "/success" in p or "/complete" in p:
        return "success"
    if "/detail" in p or "/view" in p:
        return "view"
    return "browse"


def infer_is_conversion(path: Optional[str], evt: Optional[str]) -> int:
    p = (path or "").lower()
    e = (evt or "").lower()
    return 1 if ("submit" in p or "success" in p or e in ("submit", "success")) else 0


def infer_source_type() -> str:
    return "wc"


def build_select_sql(cur) -> str:
    cols = [
        "id", "dt", "ts", "ip", "method", "url_raw", "url_full", "url_norm",
        "host", "path", "query", "status", "bytes", "ref", "ua", "kv_raw", "uid"
    ]
    optional = [
        "latency_ms", "pcid", "sid", "device_type", "evt",
        "accept_lang", "cc", "page_type", "service_domain",
        "funnel_stage", "is_conversion", "source_type"
    ]
    for col in optional:
        if has_column(cur, "stg_wc_log_hit", col):
            cols.append(col)
    return f"""
        SELECT {", ".join(cols)}
        FROM stg_wc_log_hit
        WHERE dt BETWEEN %s AND %s
        ORDER BY dt, ts, id
    """


def main():
    ap = argparse.ArgumentParser(description="Profile-safe event_log_raw builder from stg_wc_log_hit")
    ap.add_argument("--db-host", default=os.getenv("DB_HOST", "127.0.0.1"))
    ap.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "3306")))
    ap.add_argument("--db-user", default=os.getenv("DB_USER"))
    ap.add_argument("--db-pass", default=os.getenv("DB_PASSWORD", ""))
    ap.add_argument("--db-name", default=os.getenv("DB_NAME"))
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--truncate-target", action="store_true")
    ap.add_argument("--debug-preview", action="store_true")
    args = ap.parse_args()

    if not args.db_user or not args.db_name:
        raise SystemExit("db_user and db_name are required")

    conn = connect_mysql(args)
    inserted = 0
    selected = 0
    try:
        with conn.cursor() as cur:
            if args.truncate_target:
                cur.execute(
                    "DELETE FROM event_log_raw WHERE dt BETWEEN %s AND %s",
                    (args.dt_from, args.dt_to),
                )

            select_sql = build_select_sql(cur)
            cur.execute(select_sql, (args.dt_from, args.dt_to))
            rows = cur.fetchall()
            selected = len(rows)

            insert_sql = """
                INSERT INTO event_log_raw
                (
                    dt, ts, ip, method, url_raw, url_full, url_norm,
                    host, path, query, status, bytes, latency_ms, ref, ua,
                    kv_raw, uid, pcid, sid, device_type, evt,
                    accept_lang, cc, page_type, service_domain, funnel_stage,
                    is_conversion, source_type
                )
                VALUES
                (
                    %s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s
                )
            """

            batch = []

            for r in rows:
                path = r.get("path")
                kv_raw = r.get("kv_raw")
                ua = r.get("ua")

                evt = r.get("evt") or infer_evt(path)
                page_type = r.get("page_type") or infer_page_type(path)
                service_domain = r.get("service_domain") or infer_service_domain(path, r.get("host"))
                funnel_stage = r.get("funnel_stage") or infer_funnel_stage(path)
                is_conversion = r.get("is_conversion")
                if is_conversion is None:
                    is_conversion = infer_is_conversion(path, evt)

                row_map = {
                    "dt": r.get("dt"),
                    "ts": r.get("ts"),
                    "ip": r.get("ip"),
                    "method": r.get("method"),
                    "url_raw": r.get("url_raw"),
                    "url_full": r.get("url_full"),
                    "url_norm": r.get("url_norm"),
                    "host": r.get("host"),
                    "path": r.get("path"),
                    "query": r.get("query"),
                    "status": r.get("status"),
                    "bytes": r.get("bytes"),
                    "latency_ms": r.get("latency_ms") or extract_kv(kv_raw, "latency_ms"),
                    "ref": r.get("ref"),
                    "ua": r.get("ua"),
                    "kv_raw": r.get("kv_raw"),
                    "uid": r.get("uid"),
                    "pcid": r.get("pcid") or extract_kv(kv_raw, "pcid"),
                    "sid": r.get("sid") or extract_kv(kv_raw, "sid"),
                    "device_type": r.get("device_type") or infer_device_type(ua),
                    "evt": evt,
                    "accept_lang": r.get("accept_lang") or extract_kv(kv_raw, "accept_lang"),
                    "cc": r.get("cc") or extract_kv(kv_raw, "cc"),
                    "page_type": page_type,
                    "service_domain": service_domain,
                    "funnel_stage": funnel_stage,
                    "is_conversion": int(is_conversion or 0),
                    "source_type": r.get("source_type") or infer_source_type(),
                }

                batch.append((
                    row_map["dt"], row_map["ts"], row_map["ip"], row_map["method"], row_map["url_raw"], row_map["url_full"], row_map["url_norm"],
                    row_map["host"], row_map["path"], row_map["query"], row_map["status"], row_map["bytes"], row_map["latency_ms"], row_map["ref"], row_map["ua"],
                    row_map["kv_raw"], row_map["uid"], row_map["pcid"], row_map["sid"], row_map["device_type"], row_map["evt"],
                    row_map["accept_lang"], row_map["cc"], row_map["page_type"], row_map["service_domain"], row_map["funnel_stage"],
                    row_map["is_conversion"], row_map["source_type"],
                ))

            if args.debug_preview and batch:
                preview_keys = [
                    "dt","ts","ip","method","url_raw","url_full","url_norm","host","path","query","status","bytes","latency_ms",
                    "ref","ua","kv_raw","uid","pcid","sid","device_type","evt","accept_lang","cc","page_type",
                    "service_domain","funnel_stage","is_conversion","source_type"
                ]
                preview = dict(zip(preview_keys, batch[0]))
                print("[preview]", json.dumps(preview, ensure_ascii=False, default=str))

            if batch:
                cur.executemany(insert_sql, batch)
                inserted = cur.rowcount

        conn.commit()
        print(f"[event_log_raw_builder_profile_safe] rows_selected={selected} inserted={inserted}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
