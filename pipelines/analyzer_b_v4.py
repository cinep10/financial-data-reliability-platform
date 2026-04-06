from __future__ import annotations

import argparse
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Optional

import pymysql

KV_PAIR_RE = re.compile(r'(?:^|;\s*)([A-Za-z0-9_\-]+)=([^;]*)', re.I)
STATIC_EXT_RE = re.compile(r'\.(css|js|png|jpg|jpeg|gif|ico|map|woff|woff2|ttf|eot|svg|webp|zip|txt)$', re.I)

METRIC_META = {
    "daily_active_users": {"group": "user_activity", "source_layer": "collector"},
    "page_view_count": {"group": "user_activity", "source_layer": "collector"},
    "avg_session_duration_sec": {"group": "user_activity", "source_layer": "collector"},
    "new_user_ratio": {"group": "user_activity", "source_layer": "collector"},
    "login_success_count": {"group": "user_activity", "source_layer": "collector"},
    "auth_attempt_count": {"group": "auth_security", "source_layer": "collector"},
    "auth_success_count": {"group": "auth_security", "source_layer": "collector"},
    "auth_fail_count": {"group": "auth_security", "source_layer": "collector"},
    "auth_success_rate": {"group": "auth_security", "source_layer": "collector"},
    "auth_fail_rate": {"group": "auth_security", "source_layer": "collector"},
    "otp_request_count": {"group": "auth_security", "source_layer": "collector"},
    "risk_login_count": {"group": "auth_security", "source_layer": "collector"},
    "loan_view_count": {"group": "financial_service", "source_layer": "collector"},
    "loan_apply_start_count": {"group": "financial_service", "source_layer": "collector"},
    "loan_apply_submit_count": {"group": "financial_service", "source_layer": "collector"},
    "card_apply_start_count": {"group": "financial_service", "source_layer": "collector"},
    "card_apply_submit_count": {"group": "financial_service", "source_layer": "collector"},
    "card_apply_submit_rate": {"group": "financial_service", "source_layer": "collector"},
    "raw_event_count": {"group": "system_operation", "source_layer": "raw"},
    "collector_event_count": {"group": "system_operation", "source_layer": "collector"},
    "estimated_missing_rate": {"group": "system_operation", "source_layer": "control"},
    "schema_change_count": {"group": "system_operation", "source_layer": "control"},
    "batch_delay_sec": {"group": "system_operation", "source_layer": "control"},
}

LEGACY_WIDE_METRICS = ("visit", "uv", "pageview")


def parse_kv(kv_raw: Optional[str]) -> dict[str, str]:
    if not kv_raw:
        return {}
    out: dict[str, str] = {}
    for m in KV_PAIR_RE.finditer(kv_raw.strip()):
        out[m.group(1).lower()] = m.group(2).strip()
    return out


def pick_identity(row: dict, kv: dict, mode: str) -> str:
    uid = (row.get("uid") or kv.get("uid") or kv.get("nth_uid") or "").strip()
    pcid = (kv.get("pcid") or kv.get("nth_pcid") or "").strip()
    ip = (row.get("ip") or "").strip()
    if mode == "ip":
        return f"IP:{ip}"
    if mode == "pcid_ip":
        return f"PCID:{pcid}" if pcid else f"IP:{ip}"
    return f"UID:{uid}" if uid else (f"PCID:{pcid}" if pcid else f"IP:{ip}")


def is_pageview(row: dict, kv: dict, pv_mode: str) -> bool:
    method = (row.get("method") or "").upper()
    status = row.get("status")
    path = row.get("path") or ""
    evt = (kv.get("evt") or "").strip().lower()
    if status is None or not (200 <= int(status) <= 599):
        return False
    if method not in ("GET", "POST"):
        return False
    if method == "GET" and STATIC_EXT_RE.search(path):
        return False
    if pv_mode == "view_only":
        return evt in ("", "view")
    return True


def contains_any(text: str, keywords: list[str]) -> bool:
    return any(k in text for k in keywords)


def infer_event_name(row: dict, kv: dict, pv_mode: str) -> str:
    path = (row.get("path") or "").lower()
    url_norm = (row.get("url_norm") or "").lower()
    query = (row.get("query") or "").lower()
    method = (row.get("method") or "").upper()

    evt = (kv.get("evt") or kv.get("event") or kv.get("action") or "").strip().lower()
    auth_result = (kv.get("auth_result") or kv.get("result") or "").strip().lower()
    joined = " | ".join([evt, auth_result, path, url_norm, query])

    if "card" in joined and contains_any(joined, ["/card/apply/submit", "/card/application/submit", "/card/complete", "card_apply_submit", "card_apply=submit", "application_result=submitted", "submit", "complete"]):
        return "card_apply_submit"
    if "card" in joined and contains_any(joined, ["/card/apply", "/card/application/start", "card_apply_start", "card_apply=start", "start", "step1", "apply"]):
        return "card_apply_start"

    if "loan" in joined and contains_any(joined, ["/loan/apply/submit", "/loan/application/submit", "/loan/complete", "loan_apply_submit", "loan_apply=submit", "application_result=submitted", "submit", "complete"]):
        return "loan_apply_submit"
    if "loan" in joined and contains_any(joined, ["/loan/apply", "/loan/application/start", "loan_apply_start", "loan_apply=start", "start", "step1", "apply"]):
        return "loan_apply_start"
    if "loan" in joined and contains_any(joined, ["/loan", "/loan/product", "/loan/detail", "loan_view", "product=loan", "view"]) and method in ("GET", "POST"):
        return "loan_view"

    if contains_any(joined, ["/auth/success", "/auth/complete", "/cert/success", "auth_success", "auth_result=success"]) or (
        ("auth" in joined or "login" in joined) and contains_any(joined, ["success", "ok"])
    ):
        return "auth_success"

    if contains_any(joined, ["/auth/fail", "/login/fail", "/cert/fail", "auth_fail", "auth_result=fail"]) or (
        ("auth" in joined or "login" in joined) and contains_any(joined, ["fail", "error", "denied"])
    ):
        return "auth_fail"

    if contains_any(joined, ["/otp", "/auth/otp", "/mfa", "otp_request", "otp=request", "mfa=request"]) and contains_any(joined, ["otp", "mfa", "request", "send"]):
        return "otp_request"

    if contains_any(joined, ["/risk-login", "/auth/risk", "risk_login", "risk_login=1", "fraud_flag=1"]):
        return "risk_login"

    if contains_any(joined, ["/login/success", "/auth/login/success", "/signin/success", "login_success"]):
        return "login_success"

    if contains_any(joined, ["/login", "/auth/login", "/signin", "/cert", "auth_attempt", "login_attempt", "auth_step=attempt"]):
        return "auth_attempt"

    if is_pageview(row, kv, pv_mode):
        return "page_view"

    return "other"


def daterange(start_dt: date, end_dt: date):
    cur = start_dt
    while cur <= end_dt:
        yield cur
        cur += timedelta(days=1)


def ensure_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS metric_value_hh (
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          hh TINYINT NOT NULL,
          metric_name VARCHAR(100) NOT NULL,
          metric_group VARCHAR(50) NOT NULL,
          source_layer VARCHAR(50) NOT NULL,
          metric_value DECIMAL(18,6) NOT NULL,
          numerator_value DECIMAL(18,6) NULL,
          denominator_value DECIMAL(18,6) NULL,
          run_id VARCHAR(64) NULL,
          note VARCHAR(255) NULL,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (profile_id, dt, hh, metric_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS metric_value_day (
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          metric_name VARCHAR(100) NOT NULL,
          metric_group VARCHAR(50) NOT NULL,
          source_layer VARCHAR(50) NOT NULL,
          metric_value DECIMAL(18,6) NOT NULL,
          numerator_value DECIMAL(18,6) NULL,
          denominator_value DECIMAL(18,6) NULL,
          run_id VARCHAR(64) NULL,
          note VARCHAR(255) NULL,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (profile_id, dt, metric_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_ds_metric_hh (
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          hh TINYINT NOT NULL,
          metric_nm VARCHAR(100) NOT NULL,
          metric_val DECIMAL(18,6) NOT NULL,
          note VARCHAR(255) NULL,
          PRIMARY KEY (profile_id, dt, hh, metric_nm)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_ds_metric_hh_wide (
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          hh TINYINT NOT NULL,
          visit DECIMAL(18,6) NOT NULL DEFAULT 0,
          uv DECIMAL(18,6) NOT NULL DEFAULT 0,
          pageview DECIMAL(18,6) NOT NULL DEFAULT 0,
          note VARCHAR(255) NULL,
          PRIMARY KEY (profile_id, dt, hh)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def upsert_metric_hh(cur, profile_id, dt, hh, metric_name, metric_value, numerator, denominator, run_id, note):
    meta = METRIC_META[metric_name]
    cur.execute(
        """
        INSERT INTO metric_value_hh (
            profile_id, dt, hh, metric_name, metric_group, source_layer,
            metric_value, numerator_value, denominator_value, run_id, note
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            metric_group=VALUES(metric_group),
            source_layer=VALUES(source_layer),
            metric_value=VALUES(metric_value),
            numerator_value=VALUES(numerator_value),
            denominator_value=VALUES(denominator_value),
            run_id=VALUES(run_id),
            note=VALUES(note)
        """,
        (
            profile_id, dt, hh, metric_name, meta["group"], meta["source_layer"],
            metric_value, numerator, denominator, run_id, note,
        ),
    )


def upsert_metric_day(cur, profile_id, dt, metric_name, metric_value, numerator, denominator, run_id, note):
    meta = METRIC_META[metric_name]
    cur.execute(
        """
        INSERT INTO metric_value_day (
            profile_id, dt, metric_name, metric_group, source_layer,
            metric_value, numerator_value, denominator_value, run_id, note
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            metric_group=VALUES(metric_group),
            source_layer=VALUES(source_layer),
            metric_value=VALUES(metric_value),
            numerator_value=VALUES(numerator_value),
            denominator_value=VALUES(denominator_value),
            run_id=VALUES(run_id),
            note=VALUES(note)
        """,
        (
            profile_id, dt, metric_name, meta["group"], meta["source_layer"],
            metric_value, numerator, denominator, run_id, note,
        ),
    )


def upsert_legacy_hh(cur, profile_id, dt, hh, metric_nm, metric_val, note):
    cur.execute(
        """
        INSERT INTO stg_ds_metric_hh (profile_id, dt, hh, metric_nm, metric_val, note)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            metric_val=VALUES(metric_val),
            note=VALUES(note)
        """,
        (profile_id, dt, hh, metric_nm, metric_val, note),
    )


def upsert_legacy_wide(cur, profile_id, dt, hh, visit, uv, pageview, note):
    cur.execute(
        """
        INSERT INTO stg_ds_metric_hh_wide (profile_id, dt, hh, visit, uv, pageview, note)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            visit=VALUES(visit),
            uv=VALUES(uv),
            pageview=VALUES(pageview),
            note=VALUES(note)
        """,
        (profile_id, dt, hh, visit, uv, pageview, note),
    )


def truncate_targets(cur, profile_id, dt_from, dt_to, write_legacy: bool):
    cur.execute(
        "DELETE FROM metric_value_hh WHERE profile_id=%s AND dt BETWEEN %s AND %s",
        (profile_id, dt_from, dt_to),
    )
    cur.execute(
        "DELETE FROM metric_value_day WHERE profile_id=%s AND dt BETWEEN %s AND %s",
        (profile_id, dt_from, dt_to),
    )
    if write_legacy:
        cur.execute(
            "DELETE FROM stg_ds_metric_hh WHERE profile_id=%s AND dt BETWEEN %s AND %s",
            (profile_id, dt_from, dt_to),
        )
        cur.execute(
            "DELETE FROM stg_ds_metric_hh_wide WHERE profile_id=%s AND dt BETWEEN %s AND %s",
            (profile_id, dt_from, dt_to),
        )


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyzer B v4 for stg_wc_log_hit -> metric tables / legacy hh tables")
    ap.add_argument("--db-host", default="127.0.0.1")
    ap.add_argument("--db-port", type=int, default=3306)
    ap.add_argument("--db-user", required=True)
    ap.add_argument("--db-pass", default="")
    ap.add_argument("--db-name", required=True)
    ap.add_argument("--profile-id", default="default")
    ap.add_argument("--date", help="single target date, e.g. 2026-03-02")
    ap.add_argument("--dt-from", help="range start date, e.g. 2026-02-23")
    ap.add_argument("--dt-to", help="range end date, e.g. 2026-03-09")
    ap.add_argument("--lookback-days", type=int, default=30)
    ap.add_argument("--identity-mode", choices=["uid_pcid_ip", "pcid_ip", "ip"], default="uid_pcid_ip")
    ap.add_argument("--session-timeout-sec", type=int, default=1800)
    ap.add_argument("--pv-mode", choices=["view_only", "all_hits"], default="view_only")
    ap.add_argument("--truncate-target", action="store_true")
    ap.add_argument("--write-legacy", action="store_true", help="also write stg_ds_metric_hh and stg_ds_metric_hh_wide")
    args = ap.parse_args()

    if args.date:
        dt_from = dt_to = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        if not args.dt_from or not args.dt_to:
            raise SystemExit("Either --date or both --dt-from/--dt-to are required.")
        dt_from = datetime.strptime(args.dt_from, "%Y-%m-%d").date()
        dt_to = datetime.strptime(args.dt_to, "%Y-%m-%d").date()

    query_start_dt = dt_from - timedelta(days=args.lookback_days)
    query_end_dt = dt_to + timedelta(days=1)
    run_id = f"metric_{args.profile_id}_{dt_from.strftime('%Y%m%d')}_{dt_to.strftime('%Y%m%d')}"

    conn = pymysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_pass,
        database=args.db_name,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )

    try:
        with conn.cursor() as cur:
            ensure_tables(cur)
            if args.truncate_target:
                truncate_targets(cur, args.profile_id, dt_from, dt_to, args.write_legacy)

            cur.execute(
                """
                SELECT dt, ts, ip, method, path, query, status, uid, kv_raw, url_norm
                FROM stg_wc_log_hit
                WHERE dt >= %s AND dt < %s
                ORDER BY dt, ts, id
                """,
                (query_start_dt, query_end_dt),
            )
            rows = cur.fetchall()

        rows_by_dt: dict[date, list[dict]] = defaultdict(list)
        for row in rows:
            rows_by_dt[row["dt"]].append(row)

        total_target_rows = 0
        processed_days = 0

        with conn.cursor() as cur:
            for target_dt in daterange(dt_from, dt_to):
                historical_users = set()
                target_rows = rows_by_dt.get(target_dt, [])
                total_target_rows += len(target_rows)

                for hist_dt in daterange(query_start_dt, target_dt - timedelta(days=1)):
                    for row in rows_by_dt.get(hist_dt, []):
                        kv = parse_kv(row.get("kv_raw"))
                        identity = pick_identity(row, kv, args.identity_mode)
                        historical_users.add(identity)

                dau_users_by_hh: dict[int, set[str]] = defaultdict(set)
                session_duration_values: dict[int, list[int]] = defaultdict(list)
                raw_event_count: dict[int, int] = defaultdict(int)
                collector_event_count: dict[int, int] = defaultdict(int)
                pv_count: dict[int, int] = defaultdict(int)
                visit_count: dict[int, int] = defaultdict(int)

                auth_attempt: dict[int, int] = defaultdict(int)
                auth_success: dict[int, int] = defaultdict(int)
                auth_fail: dict[int, int] = defaultdict(int)
                otp_request: dict[int, int] = defaultdict(int)
                risk_login: dict[int, int] = defaultdict(int)
                login_success: dict[int, int] = defaultdict(int)
                loan_view: dict[int, int] = defaultdict(int)
                loan_start: dict[int, int] = defaultdict(int)
                loan_submit: dict[int, int] = defaultdict(int)
                card_start: dict[int, int] = defaultdict(int)
                card_submit: dict[int, int] = defaultdict(int)

                last_seen_by_identity: dict[str, datetime] = {}
                current_session_start: dict[str, datetime] = {}
                current_session_seq: dict[str, int] = defaultdict(int)
                seen_session_hour: set[tuple[int, str, int]] = set()

                for row in target_rows:
                    kv = parse_kv(row.get("kv_raw"))
                    identity = pick_identity(row, kv, args.identity_mode)
                    ts = row["ts"]
                    hh = ts.hour

                    raw_event_count[hh] += 1
                    event_name = infer_event_name(row, kv, args.pv_mode)
                    if event_name != "other":
                        collector_event_count[hh] += 1

                    if is_pageview(row, kv, args.pv_mode):
                        pv_count[hh] += 1
                        dau_users_by_hh[hh].add(identity)

                        prev = last_seen_by_identity.get(identity)
                        if prev is None or (ts - prev) > timedelta(seconds=args.session_timeout_sec):
                            current_session_start[identity] = ts
                            current_session_seq[identity] += 1
                        last_seen_by_identity[identity] = ts

                        duration = int((ts - current_session_start[identity]).total_seconds())
                        session_duration_values[hh].append(max(duration, 0))

                        sk = (hh, identity, current_session_seq[identity])
                        if sk not in seen_session_hour:
                            seen_session_hour.add(sk)
                            visit_count[hh] += 1

                    if event_name == "login_success":
                        login_success[hh] += 1
                    elif event_name == "auth_attempt":
                        auth_attempt[hh] += 1
                    elif event_name == "auth_success":
                        auth_attempt[hh] += 1
                        auth_success[hh] += 1
                        login_success[hh] += 1
                    elif event_name == "auth_fail":
                        auth_attempt[hh] += 1
                        auth_fail[hh] += 1
                    elif event_name == "otp_request":
                        otp_request[hh] += 1
                    elif event_name == "risk_login":
                        risk_login[hh] += 1
                    elif event_name == "loan_view":
                        loan_view[hh] += 1
                    elif event_name == "loan_apply_start":
                        loan_start[hh] += 1
                    elif event_name == "loan_apply_submit":
                        loan_submit[hh] += 1
                    elif event_name == "card_apply_start":
                        card_start[hh] += 1
                    elif event_name == "card_apply_submit":
                        card_submit[hh] += 1

                target_users = set()
                for users in dau_users_by_hh.values():
                    target_users |= users
                new_users = {u for u in target_users if u not in historical_users}

                hh_list = sorted(set(range(24)) & set(
                    list(raw_event_count.keys()) + list(collector_event_count.keys()) + list(pv_count.keys())
                    + list(auth_attempt.keys()) + list(auth_success.keys()) + list(auth_fail.keys())
                    + list(otp_request.keys()) + list(risk_login.keys())
                    + list(loan_view.keys()) + list(loan_start.keys()) + list(loan_submit.keys())
                    + list(card_start.keys()) + list(card_submit.keys())
                ))

                if not hh_list and target_rows:
                    hh_list = sorted({r["ts"].hour for r in target_rows})

                note = (
                    f"identity={args.identity_mode}; pv_mode={args.pv_mode}; "
                    f"source=stg_wc_log_hit; lookback_days={args.lookback_days}"
                )

                for hh in hh_list:
                    dau = len(dau_users_by_hh.get(hh, set()))
                    pageviews = pv_count.get(hh, 0)
                    attempt = auth_attempt.get(hh, 0)
                    success = auth_success.get(hh, 0)
                    fail = auth_fail.get(hh, 0)
                    otp = otp_request.get(hh, 0)
                    risk = risk_login.get(hh, 0)
                    loan_v = loan_view.get(hh, 0)
                    loan_s = loan_start.get(hh, 0)
                    loan_sub = loan_submit.get(hh, 0)
                    card_s = card_start.get(hh, 0)
                    card_sub = card_submit.get(hh, 0)
                    raw_cnt = raw_event_count.get(hh, 0)
                    collector_cnt = collector_event_count.get(hh, 0)
                    avg_sess = round(
                        sum(session_duration_values.get(hh, [0])) / max(len(session_duration_values.get(hh, [])), 1),
                        6,
                    )
                    new_ratio = round(len(new_users) / max(len(target_users), 1), 6) if target_users else 0.0
                    succ_rate = round(success / attempt, 6) if attempt else 0.0
                    fail_rate = round(fail / attempt, 6) if attempt else 0.0
                    card_submit_rate = round(card_sub / card_s, 6) if card_s else 0.0
                    missing_rate = round(max(raw_cnt - collector_cnt, 0) / raw_cnt, 6) if raw_cnt else 0.0

                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "daily_active_users", dau, dau, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "page_view_count", pageviews, pageviews, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "avg_session_duration_sec", avg_sess, avg_sess, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "new_user_ratio", new_ratio, len(new_users), len(target_users), run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "login_success_count", login_success.get(hh, 0), login_success.get(hh, 0), None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "auth_attempt_count", attempt, attempt, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "auth_success_count", success, success, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "auth_fail_count", fail, fail, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "auth_success_rate", succ_rate, success, attempt, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "auth_fail_rate", fail_rate, fail, attempt, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "otp_request_count", otp, otp, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "risk_login_count", risk, risk, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "loan_view_count", loan_v, loan_v, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "loan_apply_start_count", loan_s, loan_s, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "loan_apply_submit_count", loan_sub, loan_sub, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "card_apply_start_count", card_s, card_s, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "card_apply_submit_count", card_sub, card_sub, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "card_apply_submit_rate", card_submit_rate, card_sub, card_s, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "raw_event_count", raw_cnt, raw_cnt, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "collector_event_count", collector_cnt, collector_cnt, None, run_id, note)
                    upsert_metric_hh(cur, args.profile_id, target_dt, hh, "estimated_missing_rate", missing_rate, max(raw_cnt - collector_cnt, 0), raw_cnt, run_id, note)

                    if args.write_legacy:
                        upsert_legacy_hh(cur, args.profile_id, target_dt, hh, "visit", visit_count.get(hh, 0), note)
                        upsert_legacy_hh(cur, args.profile_id, target_dt, hh, "uv", dau, note)
                        upsert_legacy_hh(cur, args.profile_id, target_dt, hh, "pageview", pageviews, note)
                        upsert_legacy_wide(cur, args.profile_id, target_dt, hh, visit_count.get(hh, 0), dau, pageviews, note)

                def day_sum(src: dict[int, int]) -> int:
                    return int(sum(src.values()))

                target_user_count = len(target_users)
                new_user_count = len(new_users)
                attempt_day = day_sum(auth_attempt)
                success_day = day_sum(auth_success)
                fail_day = day_sum(auth_fail)
                card_start_day = day_sum(card_start)
                card_submit_day = day_sum(card_submit)
                raw_day = day_sum(raw_event_count)
                collector_day = day_sum(collector_event_count)
                missing_day = round(max(raw_day - collector_day, 0) / raw_day, 6) if raw_day else 0.0
                avg_sess_day = round(
                    sum(sum(v) for v in session_duration_values.values()) /
                    max(sum(len(v) for v in session_duration_values.values()), 1),
                    6,
                )

                daily_metrics = [
                    ("daily_active_users", target_user_count, target_user_count, None),
                    ("page_view_count", day_sum(pv_count), day_sum(pv_count), None),
                    ("avg_session_duration_sec", avg_sess_day, avg_sess_day, None),
                    ("new_user_ratio", round(new_user_count / max(target_user_count, 1), 6) if target_user_count else 0.0, new_user_count, target_user_count),
                    ("login_success_count", day_sum(login_success), day_sum(login_success), None),
                    ("auth_attempt_count", attempt_day, attempt_day, None),
                    ("auth_success_count", success_day, success_day, None),
                    ("auth_fail_count", fail_day, fail_day, None),
                    ("auth_success_rate", round(success_day / attempt_day, 6) if attempt_day else 0.0, success_day, attempt_day),
                    ("auth_fail_rate", round(fail_day / attempt_day, 6) if attempt_day else 0.0, fail_day, attempt_day),
                    ("otp_request_count", day_sum(otp_request), day_sum(otp_request), None),
                    ("risk_login_count", day_sum(risk_login), day_sum(risk_login), None),
                    ("loan_view_count", day_sum(loan_view), day_sum(loan_view), None),
                    ("loan_apply_start_count", day_sum(loan_start), day_sum(loan_start), None),
                    ("loan_apply_submit_count", day_sum(loan_submit), day_sum(loan_submit), None),
                    ("card_apply_start_count", card_start_day, card_start_day, None),
                    ("card_apply_submit_count", card_submit_day, card_submit_day, None),
                    ("card_apply_submit_rate", round(card_submit_day / card_start_day, 6) if card_start_day else 0.0, card_submit_day, card_start_day),
                    ("raw_event_count", raw_day, raw_day, None),
                    ("collector_event_count", collector_day, collector_day, None),
                    ("estimated_missing_rate", missing_day, max(raw_day - collector_day, 0), raw_day),
                    ("schema_change_count", 0, 0, None),
                    ("batch_delay_sec", 0, 0, None),
                ]

                for metric_name, metric_value, numerator, denominator in daily_metrics:
                    upsert_metric_day(cur, args.profile_id, target_dt, metric_name, metric_value, numerator, denominator, run_id, note)

                processed_days += 1

        conn.commit()
        print(
            f"[analyzer_b_v4] source=stg_wc_log_hit profile_id={args.profile_id} "
            f"dt_from={dt_from} dt_to={dt_to} processed_days={processed_days} rows={total_target_rows}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
