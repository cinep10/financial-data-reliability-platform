#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from typing import Optional, Any

import pymysql


SOURCE_TABLE = "stg_wc_log_hit"
TARGET_TABLE = "event_log_raw"


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


def get_varchar_limits(cur, table_name: str) -> dict[str, int]:
    cur.execute(
        """
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
        """,
        (table_name,),
    )
    limits: dict[str, int] = {}
    for row in cur.fetchall():
        if (row["data_type"] or "").lower() in ("char", "varchar") and row["character_maximum_length"]:
            limits[row["column_name"]] = int(row["character_maximum_length"])
    return limits


def clip(value: Any, max_len: Optional[int]) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if max_len is None:
        return s
    return s[:max_len]


def extract_kv(kv_raw: Optional[str], key: str) -> Optional[str]:
    if not kv_raw:
        return None

    text = str(kv_raw)

    for delim in ("&", ",", ";", "|"):
        if delim in text:
            for part in text.split(delim):
                part = part.strip()
                if part.startswith(f"{key}="):
                    val = part.split("=", 1)[1].strip()
                    return val or None

    marker = f"{key}="
    pos = text.find(marker)
    if pos >= 0:
        val = text[pos + len(marker):]
        for stop in (" ", "\t", "\n"):
            if stop in val:
                val = val.split(stop, 1)[0]
        return val.strip() or None

    return None


def infer_service_domain(path: Optional[str], host: Optional[str]) -> str:
    p = (path or "").lower()
    h = (host or "").lower()

    if "/loan/" in p or "loan" in h:
        return "loan"
    if "/card/" in p or "card" in h:
        return "card"
    if "/auth/" in p or "/login" in p or "auth" in h:
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
    if "/detail" in p or "/view" in p or "/overview" in p or "/notice" in p:
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
    if "/submit" in p:
        return "submit"
    if "/success" in p or "/complete" in p:
        return "success"
    return "view"


def infer_funnel_stage(path: Optional[str]) -> str:
    p = (path or "").lower()
    if "/apply" in p and "/submit" not in p:
        return "apply_start"
    if "/submit" in p:
        return "apply_submit"
    if "/success" in p or "/complete" in p:
        return "success"
    if "/detail" in p or "/view" in p or "/overview" in p:
        return "view"
    return "browse"


def infer_is_conversion(path: Optional[str], evt: Optional[str]) -> int:
    p = (path or "").lower()
    e = (evt or "").lower()
    return 1 if ("/submit" in p or "/success" in p or e in ("submit", "success")) else 0


def build_select_sql(cur) -> str:
    cols = [
        "id", "dt", "ts", "ip", "method", "url_raw", "url_full", "url_norm",
        "host", "path", "query", "status", "bytes", "ref", "ua", "kv_raw", "uid"
    ]
    for col in [
        "latency_ms", "pcid", "sid", "device_type", "evt",
        "accept_lang", "cc", "page_type", "service_domain",
        "funnel_stage", "is_conversion", "source_type"
    ]:
        if has_column(cur, SOURCE_TABLE, col):
            cols.append(col)

    return f"""
        SELECT {", ".join(cols)}
        FROM {SOURCE_TABLE}
        WHERE dt BETWEEN %s AND %s
        ORDER BY dt, ts, id
    """


def main():
    ap = argparse.ArgumentParser()
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

    try:
        with conn.cursor() as cur:
            limits = get_varchar_limits(cur, TARGET_TABLE)

            if args.truncate_target:
                cur.execute(
                    f"DELETE FROM {TARGET_TABLE} WHERE dt BETWEEN %s AND %s",
                    (args.dt_from, args.dt_to),
                )

            select_sql = build_select_sql(cur)
            cur.execute(select_sql, (args.dt_from, args.dt_to))
            rows = cur.fetchall()

            insert_sql = f"""
                INSERT INTO {TARGET_TABLE}
                (
                    dt, ts, ip, method, url_raw, url_full, url_norm, host, path, query,
                    status, bytes, latency_ms, ref, ua, kv_raw, uid, pcid, sid,
                    device_type, evt, accept_lang, cc, page_type, service_domain,
                    funnel_stage, is_conversion, source_type
                )
                VALUES
                (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
            """

            batch = []

            for r in rows:
                path = r.get("path")
                ua = r.get("ua")
                kv_raw = r.get("kv_raw")

                evt = r.get("evt") or infer_evt(path)
                service_domain = r.get("service_domain") or infer_service_domain(path, r.get("host"))
                funnel_stage = r.get("funnel_stage") or infer_funnel_stage(path)
                page_type = r.get("page_type") or infer_page_type(path)
                is_conversion = r.get("is_conversion")
                if is_conversion is None:
                    is_conversion = infer_is_conversion(path, evt)

                cc_val = r.get("cc")
                if cc_val is None:
                    cc_val = extract_kv(kv_raw, "cc")

                accept_lang_val = r.get("accept_lang")
                if accept_lang_val is None:
                    accept_lang_val = extract_kv(kv_raw, "accept_lang")

                latency_val = r.get("latency_ms")
                if latency_val is None:
                    latency_val = extract_kv(kv_raw, "latency_ms")

                row_tuple = (
                    r.get("dt"),
                    r.get("ts"),
                    clip(r.get("ip"), limits.get("ip")),
                    clip(r.get("method"), limits.get("method")),
                    r.get("url_raw"),
                    r.get("url_full"),
                    r.get("url_norm"),
                    clip(r.get("host"), limits.get("host")),
                    clip(r.get("path"), limits.get("path")),
                    r.get("query"),
                    r.get("status"),
                    r.get("bytes"),
                    latency_val,
                    r.get("ref"),
                    r.get("ua"),
                    r.get("kv_raw"),
                    clip(r.get("uid"), limits.get("uid")),
                    clip(r.get("pcid") or extract_kv(kv_raw, "pcid"), limits.get("pcid")),
                    clip(r.get("sid") or extract_kv(kv_raw, "sid"), limits.get("sid")),
                    clip(r.get("device_type") or infer_device_type(ua), limits.get("device_type")),
                    clip(evt, limits.get("evt")),
                    clip(accept_lang_val, limits.get("accept_lang")),
                    clip(cc_val, limits.get("cc")),
                    clip(page_type, limits.get("page_type")),
                    clip(service_domain, limits.get("service_domain")),
                    clip(funnel_stage, limits.get("funnel_stage")),
                    int(is_conversion or 0),
                    clip(r.get("source_type") or "weblog", limits.get("source_type")),
                )
                batch.append(row_tuple)

            if args.debug_preview and batch:
                print("[preview_first_row]")
                for i, v in enumerate(batch[0], start=1):
                    print(i, repr(v))

            if batch:
                cur.executemany(insert_sql, batch)

        conn.commit()
        print(f"[event_log_raw_builder] done rows={len(rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
