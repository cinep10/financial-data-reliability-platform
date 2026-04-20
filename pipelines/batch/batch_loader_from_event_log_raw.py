from __future__ import annotations

import argparse
import re
from datetime import datetime
from typing import Optional

import pymysql

STATIC_EXT_RE = re.compile(r"\.(css|js|png|jpg|jpeg|gif|ico|map|woff|woff2|ttf|eot|svg|webp|zip|txt)$", re.I)
KV_PAIR_RE = re.compile(r"(?:^|;\s*)([A-Za-z0-9_\-]+)=([^;]*)", re.I)


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
    row = cur.fetchone() or {}
    return int(row.get("cnt") or 0) > 0


def parse_kv(kv_raw: Optional[str]) -> dict[str, str]:
    if not kv_raw:
        return {}
    out: dict[str, str] = {}
    for m in KV_PAIR_RE.finditer(str(kv_raw).strip()):
        out[m.group(1).lower()] = m.group(2).strip()
    return out


def is_page_event(row: dict, page_event_mode: str = "evt_or_page_type") -> bool:
    evt = (row.get("evt") or "").lower()
    page_type = (row.get("page_type") or "").strip()
    path = (row.get("path") or "").lower()

    if STATIC_EXT_RE.search(path):
        return False

    if page_event_mode == "evt_or_page_type":
        return evt == "view" or page_type != ""
    return evt == "view"


def infer_event_name(row: dict) -> str:
    path = (row.get("path") or "").lower()
    evt = (row.get("evt") or "").lower()

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


def infer_service_domain(row: dict) -> Optional[str]:
    path = (row.get("path") or "").lower()
    host = (row.get("host") or "").lower()

    if "/loan/" in path or "loan" in host:
        return "loan"
    if "/card/" in path or "card" in host:
        return "card"
    if "/auth/" in path or "auth" in host or "login" in path:
        return "auth"
    if "/account/" in path:
        return "account"
    if "/customer/" in path:
        return "customer"
    if "/branch/" in path:
        return "branch"
    if path.startswith("/main") or path == "/":
        return "main"
    return "other"


def infer_funnel_stage(event_name: str) -> Optional[str]:
    if event_name in ("loan_apply_start", "card_apply_start"):
        return "apply_start"
    if event_name in ("loan_apply_submit", "card_apply_submit"):
        return "apply_submit"
    if event_name in ("auth_attempt", "auth_success", "otp_request"):
        return "auth"
    if event_name == "loan_view":
        return "view"
    if event_name == "page_view":
        return "browse"
    return None


def infer_is_conversion(event_name: str) -> int:
    return 1 if event_name in ("loan_apply_submit", "card_apply_submit", "auth_success") else 0


def ensure_stg_event_batch(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_event_batch (
          batch_ingest_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
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
          device_type VARCHAR(50) NULL,
          page_type VARCHAR(50) NULL,
          status INT NULL,
          latency_ms INT NULL,
          batch_dt DATE NOT NULL,
          parse_status VARCHAR(20) DEFAULT 'success',
          load_status VARCHAR(20) DEFAULT 'success',
          replay_source VARCHAR(50) DEFAULT 'event_log_raw',
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (batch_ingest_id),
          KEY idx_batch_dt (batch_dt),
          KEY idx_domain_dt (service_domain, dt),
          KEY idx_uid (uid),
          KEY idx_sid (sid),
          UNIQUE KEY uq_raw_event_id (raw_event_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def build_select_sql(cur) -> str:
    cols = ["raw_event_id", "dt", "ts", "status"]
    optional = [
        "event_name",
        "service_domain",
        "funnel_stage",
        "is_conversion",
        "uid",
        "pcid",
        "sid",
        "device_type",
        "page_type",
        "latency_ms",
        "evt",
        "host",
        "path",
        "query",
        "kv_raw",
    ]
    for c in optional:
        if has_column(cur, "event_log_raw", c):
            cols.append(c)

    return f"""
        SELECT {", ".join(cols)}
        FROM event_log_raw
        WHERE dt BETWEEN %s AND %s
        ORDER BY dt, ts, raw_event_id
    """


def main() -> None:
    ap = argparse.ArgumentParser(description="Load canonical event_log_raw into stg_event_batch (schema-adaptive)")
    ap.add_argument("--db-host", default="127.0.0.1")
    ap.add_argument("--db-port", type=int, default=3306)
    ap.add_argument("--db-user", required=True)
    ap.add_argument("--db-pass", default="")
    ap.add_argument("--db-name", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--truncate-target", action="store_true")
    ap.add_argument("--page-event-mode", choices=["view_only", "evt_or_page_type"], default="evt_or_page_type")
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()

    dt_from = datetime.strptime(args.dt_from, "%Y-%m-%d").date()
    dt_to = datetime.strptime(args.dt_to, "%Y-%m-%d").date()
    run_id = args.run_id or f"batchload_{dt_from.strftime('%Y%m%d')}_{dt_to.strftime('%Y%m%d')}"

    conn = connect_mysql(args)
    try:
        with conn.cursor() as cur:
            ensure_stg_event_batch(cur)

            if args.truncate_target:
                cur.execute(
                    "DELETE FROM stg_event_batch WHERE dt BETWEEN %s AND %s",
                    (dt_from, dt_to),
                )

            sql = build_select_sql(cur)
            cur.execute(sql, (dt_from, dt_to))
            rows = cur.fetchall()

            insert_sql = """
                INSERT INTO stg_event_batch
                (
                    raw_event_id, dt, ts, event_name, service_domain, funnel_stage, is_conversion,
                    uid, pcid, sid, device_type, page_type, status, latency_ms,
                    batch_dt, parse_status, load_status, replay_source
                )
                VALUES
                (
                    %s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s
                )
                ON DUPLICATE KEY UPDATE
                    dt=VALUES(dt),
                    ts=VALUES(ts),
                    event_name=VALUES(event_name),
                    service_domain=VALUES(service_domain),
                    funnel_stage=VALUES(funnel_stage),
                    is_conversion=VALUES(is_conversion),
                    uid=VALUES(uid),
                    pcid=VALUES(pcid),
                    sid=VALUES(sid),
                    device_type=VALUES(device_type),
                    page_type=VALUES(page_type),
                    status=VALUES(status),
                    latency_ms=VALUES(latency_ms),
                    batch_dt=VALUES(batch_dt),
                    parse_status=VALUES(parse_status),
                    load_status=VALUES(load_status),
                    replay_source=VALUES(replay_source)
            """

            inserts = []
            for r in rows:
                if not is_page_event(r, args.page_event_mode):
                    continue

                kv = parse_kv(r.get("kv_raw"))
                event_name = (r.get("event_name") or "").strip() or infer_event_name(r)
                service_domain = (r.get("service_domain") or "").strip() or infer_service_domain(r)
                funnel_stage = (r.get("funnel_stage") or "").strip() or infer_funnel_stage(event_name)
                is_conversion = int(r.get("is_conversion") if r.get("is_conversion") is not None else infer_is_conversion(event_name))

                uid = r.get("uid") or kv.get("uid") or kv.get("nth_uid")
                pcid = r.get("pcid") or kv.get("pcid") or kv.get("nth_pcid")
                sid = r.get("sid") or kv.get("sid") or kv.get("nth_sid")
                device_type = r.get("device_type") or kv.get("device") or kv.get("device_type")
                page_type = r.get("page_type") or kv.get("page_type")
                latency_ms = r.get("latency_ms") or kv.get("latency_ms")

                inserts.append(
                    (
                        r["raw_event_id"],
                        r["dt"],
                        r["ts"],
                        event_name,
                        service_domain,
                        funnel_stage,
                        is_conversion,
                        uid,
                        pcid,
                        sid,
                        device_type,
                        page_type,
                        r.get("status"),
                        latency_ms,
                        r["dt"],
                        "success",
                        "success",
                        "event_log_raw",
                    )
                )

            if inserts:
                cur.executemany(insert_sql, inserts)

        conn.commit()
        print(
            f"[batch_loader_from_event_log_raw] run_id={run_id} "
            f"rows={len(inserts)} dt_from={dt_from} dt_to={dt_to}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
