from __future__ import annotations

import argparse
import random
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlsplit

import pymysql

STATIC_EXT_RE = re.compile(r"\.(css|js|png|jpg|jpeg|gif|ico|map|woff|woff2|ttf|eot|svg|webp|zip|txt)$", re.I)
KV_PAIR_RE = re.compile(r'(?:^|;\s*)([A-Za-z0-9_\-]+)=([^;]*)', re.I)


def parse_kv(kv_raw: Optional[str]) -> dict[str, str]:
    if not kv_raw:
        return {}
    out: dict[str, str] = {}
    for m in KV_PAIR_RE.finditer(kv_raw.strip()):
        out[m.group(1)] = m.group(2).strip()
    return out


def pick_uid(kv: dict[str, str]) -> str:
    for k in ('uid', 'UID', 'nth_uid', 'NTH_UID'):
        v = kv.get(k, '').strip()
        if v:
            return v
    return ''


def norm_url(url_raw: str, base_url: str) -> tuple[str, str, str, str, str | None]:
    u = (url_raw or '').strip()
    if not u:
        return '', '', '', '', None
    if u.startswith(('http://', 'https://')):
        full = u
    elif u.startswith('/'):
        full = base_url.rstrip('/') + u
    else:
        full = base_url.rstrip('/') + '/' + u.lstrip('/')
    p = urlsplit(full)
    host = p.netloc
    path = re.sub(r';jsessionid=[^/?]+', '', p.path or '/', flags=re.I)
    query = p.query if p.query else None
    url_norm = f'{p.scheme}://{host}{path}' if p.scheme and host else path
    return full, url_norm, host, path, query


@dataclass
class CollectorRules:
    base_url: str = 'https://www.weather.go.kr'
    drop_rate: float = 0.05
    dup_rate: float = 0.01
    keep_methods: tuple[str, ...] = ('GET', 'POST')
    keep_status_min: int = 200
    keep_status_max: int = 599
    require_page_like: bool = False
    page_like_kv_keys: tuple[str, ...] = ('page_type=', 'evt=view')
    force_status_200_rate: float = 0.0


def is_page_like(kv_raw: Optional[str], rules: CollectorRules) -> bool:
    if not rules.require_page_like:
        return True
    if not kv_raw:
        return False
    s = kv_raw.lower()
    return any(k.lower() in s for k in rules.page_like_kv_keys)


def connect_mysql(host: str, port: int, user: str, password: str, db: str):
    return pymysql.connect(
        host=host, port=port, user=user, password=password, database=db,
        charset='utf8mb4', autocommit=False, cursorclass=pymysql.cursors.DictCursor
    )


def run(mysql_host: str, mysql_port: int, mysql_user: str, mysql_pass: str, mysql_db: str,
        dt_from: str, dt_to: str, rules: CollectorRules, seed: int | None = None,
        truncate_target: bool = False):
    if seed is not None:
        random.seed(seed)

    conn = connect_mysql(mysql_host, mysql_port, mysql_user, mysql_pass, mysql_db)
    try:
        with conn.cursor() as cur:
            if truncate_target:
                cur.execute('DELETE FROM stg_wc_log_hit WHERE dt BETWEEN %s AND %s', (dt_from, dt_to))
                conn.commit()

            cur.execute(
                """
                SELECT id, dt, ts, ip, method, url_raw, status, bytes, ref, ua, kv_raw
                FROM stg_webserver_log_hit
                WHERE dt BETWEEN %s AND %s
                  AND ts IS NOT NULL
                ORDER BY ts, id
                """,
                (dt_from, dt_to),
            )
            rows = cur.fetchall()

        out_rows = []
        for r in rows:
            method = (r.get('method') or '').upper()
            status = int(r['status']) if r.get('status') is not None else None
            url_raw = r.get('url_raw') or ''
            if method not in rules.keep_methods:
                continue
            if status is None or not (rules.keep_status_min <= status <= rules.keep_status_max):
                continue
            if not url_raw:
                continue

            url_full, url_norm, host, path, query = norm_url(url_raw, rules.base_url)
            if not url_norm:
                continue
            if method == 'GET' and STATIC_EXT_RE.search(path):
                continue

            kv_raw = r.get('kv_raw')
            if not is_page_like(kv_raw, rules):
                continue
            if random.random() < rules.drop_rate:
                continue
            if rules.force_status_200_rate > 0 and random.random() < rules.force_status_200_rate:
                status = 200

            kv = parse_kv(kv_raw)
            uid = pick_uid(kv) or None
            out = (
                r['dt'], r['ts'], r['ip'], method,
                url_raw, url_full, url_norm, host, path, query,
                status, r.get('bytes'), r.get('ref'), r.get('ua'), kv_raw, uid,
            )
            out_rows.append(out)
            if random.random() < rules.dup_rate:
                out_rows.append(out)

        if out_rows:
            conn2 = connect_mysql(mysql_host, mysql_port, mysql_user, mysql_pass, mysql_db)
            try:
                with conn2.cursor() as cur2:
                    cur2.executemany(
                        """
                        INSERT INTO stg_wc_log_hit
                        (dt, ts, ip, method, url_raw, url_full, url_norm, host, path, query, status, bytes, ref, ua, kv_raw, uid)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        out_rows,
                    )
                conn2.commit()
            finally:
                conn2.close()

        print(f'[collector_a] read={len(rows)} wrote={len(out_rows)}')
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description='Collector A: stg_webserver_log_hit -> stg_wc_log_hit')
    ap.add_argument('--db-host', default='127.0.0.1')
    ap.add_argument('--db-port', type=int, default=3306)
    ap.add_argument('--db-user', required=True)
    ap.add_argument('--db-pass', default='')
    ap.add_argument('--db-name', required=True)
    ap.add_argument('--dt-from', required=True)
    ap.add_argument('--dt-to', required=True)
    ap.add_argument('--base-url', default='https://www.weather.go.kr')
    ap.add_argument('--drop-rate', type=float, default=0.05)
    ap.add_argument('--dup-rate', type=float, default=0.01)
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--require-page-like', action='store_true')
    ap.add_argument('--force-status-200-rate', type=float, default=0.0)
    ap.add_argument('--truncate-target', action='store_true')
    args = ap.parse_args()

    rules = CollectorRules(
        base_url=args.base_url,
        drop_rate=args.drop_rate,
        dup_rate=args.dup_rate,
        require_page_like=args.require_page_like,
        force_status_200_rate=args.force_status_200_rate,
    )
    run(args.db_host, args.db_port, args.db_user, args.db_pass, args.db_name,
        args.dt_from, args.dt_to, rules, seed=args.seed, truncate_target=args.truncate_target)


if __name__ == '__main__':
    main()
