#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from typing import Optional

import pymysql

try:
    from kafka import KafkaProducer
except Exception:
    KafkaProducer = None


def connect_mysql(args):
    return pymysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_pass,
        database=args.db_name,
        charset='utf8mb4',
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_event_log_raw(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS event_log_raw (
            raw_event_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            dt DATE NOT NULL,
            ts DATETIME NOT NULL,
            source_row_id BIGINT UNSIGNED NULL,
            source_table VARCHAR(64) NOT NULL DEFAULT 'stg_wc_log_hit',
            profile_id VARCHAR(64) NULL,
            event_name VARCHAR(100) NOT NULL,
            service_domain VARCHAR(50) NULL,
            funnel_stage VARCHAR(50) NULL,
            is_conversion TINYINT(1) NOT NULL DEFAULT 0,
            uid VARCHAR(128) NULL,
            pcid VARCHAR(128) NULL,
            sid VARCHAR(128) NULL,
            device_type VARCHAR(32) NULL,
            page_type VARCHAR(32) NULL,
            ip VARCHAR(45) NULL,
            method VARCHAR(10) NULL,
            host VARCHAR(255) NULL,
            path VARCHAR(2048) NULL,
            query TEXT NULL,
            url_raw TEXT NULL,
            url_full TEXT NULL,
            url_norm TEXT NULL,
            status INT NULL,
            bytes BIGINT NULL,
            latency_ms INT NULL,
            ref TEXT NULL,
            ua TEXT NULL,
            kv_raw TEXT NULL,
            evt VARCHAR(20) NULL,
            accept_lang VARCHAR(255) NULL,
            cc CHAR(2) NULL,
            payload_json JSON NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (raw_event_id),
            KEY idx_event_log_raw_dt_ts (dt, ts),
            KEY idx_event_log_raw_uid (uid),
            KEY idx_event_log_raw_domain_dt (service_domain, dt),
            KEY idx_event_log_raw_source (source_table, source_row_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def extract_kv(kv_raw: Optional[str], key: str) -> Optional[str]:
    if not kv_raw:
        return None
    prefix = f"{key}="
    for part in str(kv_raw).split('&'):
        if part.startswith(prefix):
            value = part[len(prefix):].strip()
            return value or None
    return None


def infer_service_domain(path: Optional[str], host: Optional[str] = None) -> str:
    p = (path or '').lower()
    h = (host or '').lower()
    if '/loan/' in p or 'loan' in h:
        return 'loan'
    if '/card/' in p or 'card' in h:
        return 'card'
    if '/auth/' in p or 'auth' in h or '/login' in p:
        return 'auth'
    if '/account/' in p:
        return 'account'
    if '/customer/' in p:
        return 'customer'
    if '/branch/' in p:
        return 'branch'
    if '/transfer/' in p:
        return 'transfer'
    if p == '/' or p.startswith('/main') or p.startswith('/home'):
        return 'main'
    return 'other'


def infer_page_type(path: Optional[str]) -> Optional[str]:
    p = (path or '').lower()
    if '/apply' in p:
        return 'apply'
    if '/submit' in p:
        return 'submit'
    if '/success' in p or '/complete' in p:
        return 'success'
    if '/detail' in p or '/view' in p:
        return 'detail'
    if '/list' in p:
        return 'list'
    if p:
        return 'page'
    return None


def infer_device_type(ua: Optional[str]) -> Optional[str]:
    x = (ua or '').lower()
    if not x:
        return None
    if 'iphone' in x or 'android' in x or 'mobile' in x:
        return 'mobile'
    if 'ipad' in x or 'tablet' in x:
        return 'tablet'
    return 'desktop'


def infer_evt(path: Optional[str]) -> str:
    p = (path or '').lower()
    if p.endswith('/submit.do') or '/submit' in p:
        return 'submit'
    if p.endswith('/success.do') or '/success' in p:
        return 'success'
    return 'view'


def infer_funnel_stage(path: Optional[str]) -> str:
    p = (path or '').lower()
    if p.endswith('/apply.do') or '/apply' in p:
        return 'apply_start'
    if p.endswith('/submit.do') or '/submit' in p:
        return 'apply_submit'
    if p.endswith('/success.do') or '/success' in p or '/complete' in p:
        return 'success'
    if '/detail' in p or '/view' in p:
        return 'view'
    return 'browse'


def infer_event_name(path: Optional[str], evt: Optional[str] = None) -> str:
    p = (path or '').lower()
    e = (evt or '').lower()
    if '/card/' in p and 'submit' in p:
        return 'card_apply_submit'
    if '/card/' in p and 'apply' in p:
        return 'card_apply_start'
    if '/loan/' in p and 'submit' in p:
        return 'loan_apply_submit'
    if '/loan/' in p and 'apply' in p:
        return 'loan_apply_start'
    if '/loan/' in p:
        return 'loan_view'
    if '/auth/' in p and 'success' in p:
        return 'auth_success'
    if '/auth/' in p:
        return 'auth_attempt'
    if '/transfer/' in p and 'submit' in p:
        return 'transfer_submit'
    if '/transfer/' in p:
        return 'transfer_view'
    if e == 'submit':
        return 'submit'
    return 'page_view'


def infer_is_conversion(path: Optional[str], evt: Optional[str]) -> int:
    p = (path or '').lower()
    e = (evt or '').lower()
    return 1 if ('submit' in p or 'success' in p or e in ('submit', 'success')) else 0


def kafka_topic(service_domain: Optional[str], prefix: str) -> str:
    domain = (service_domain or 'other').strip() or 'other'
    return f"{prefix}{domain}"


def kafka_key(payload_obj: dict) -> bytes:
    candidate = payload_obj.get('uid') or payload_obj.get('sid') or payload_obj.get('pcid') or payload_obj.get('source_row_id')
    return str(candidate).encode('utf-8')


def main():
    ap = argparse.ArgumentParser(description='Build event_log_raw from stg_wc_log_hit and optionally publish to Kafka')
    ap.add_argument('--db-host', default=os.getenv('DB_HOST', '127.0.0.1'))
    ap.add_argument('--db-port', type=int, default=int(os.getenv('DB_PORT', '3306')))
    ap.add_argument('--db-user', default=os.getenv('DB_USER'))
    ap.add_argument('--db-pass', default=os.getenv('DB_PASSWORD', ''))
    ap.add_argument('--db-name', default=os.getenv('DB_NAME'))
    ap.add_argument('--dt-from', required=True)
    ap.add_argument('--dt-to', required=True)
    ap.add_argument('--profile-id', default=os.getenv('PROFILE_ID'))
    ap.add_argument('--truncate-target', action='store_true')
    ap.add_argument('--publish-kafka', action='store_true')
    ap.add_argument('--kafka-bootstrap', default=os.getenv('KAFKA_BOOTSTRAP', '127.0.0.1:9092'))
    ap.add_argument('--kafka-topic-prefix', default=os.getenv('KAFKA_TOPIC_PREFIX', 'event-'))
    args = ap.parse_args()

    if not args.db_user or not args.db_name:
        raise SystemExit('db_user and db_name are required')

    producer = None
    if args.publish_kafka:
        if KafkaProducer is None:
            raise SystemExit('kafka-python not installed. Run: pip install kafka-python')
        producer = KafkaProducer(
            bootstrap_servers=args.kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
            acks='all',
        )

    conn = connect_mysql(args)
    inserted = 0
    sent = 0
    selected_rows = 0
    try:
        with conn.cursor() as cur:
            ensure_event_log_raw(cur)

            if args.truncate_target:
                if args.profile_id:
                    cur.execute(
                        'DELETE FROM event_log_raw WHERE dt BETWEEN %s AND %s AND (profile_id=%s OR profile_id IS NULL)',
                        (args.dt_from, args.dt_to, args.profile_id),
                    )
                else:
                    cur.execute(
                        'DELETE FROM event_log_raw WHERE dt BETWEEN %s AND %s',
                        (args.dt_from, args.dt_to),
                    )

            cur.execute(
                """
                SELECT
                    id, dt, ts, ip, method, url_raw, url_full, url_norm, host, path, query,
                    status, bytes, ref, ua, kv_raw, uid
                FROM stg_wc_log_hit
                WHERE dt BETWEEN %s AND %s
                ORDER BY dt, ts, id
                """,
                (args.dt_from, args.dt_to),
            )
            rows = cur.fetchall()
            selected_rows = len(rows)

            insert_sql = """
                INSERT INTO event_log_raw
                (
                    dt, ts, source_row_id, source_table, profile_id,
                    event_name, service_domain, funnel_stage, is_conversion,
                    uid, pcid, sid, device_type, page_type,
                    ip, method, host, path, query,
                    url_raw, url_full, url_norm,
                    status, bytes, latency_ms, ref, ua, kv_raw,
                    evt, accept_lang, cc, payload_json
                )
                VALUES
                (
                    %s,%s,%s,'stg_wc_log_hit',%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s
                )
            """

            batch = []
            kafka_rows = []
            for r in rows:
                path = r.get('path')
                kv_raw = r.get('kv_raw')
                ua = r.get('ua')
                evt = infer_evt(path)
                page_type = infer_page_type(path)
                service_domain = infer_service_domain(path, r.get('host'))
                funnel_stage = infer_funnel_stage(path)
                event_name = infer_event_name(path, evt)
                is_conversion = infer_is_conversion(path, evt)
                pcid = extract_kv(kv_raw, 'pcid')
                sid = extract_kv(kv_raw, 'sid')
                device_type = infer_device_type(ua)
                latency_ms = extract_kv(kv_raw, 'latency_ms')
                accept_lang = extract_kv(kv_raw, 'accept_lang')
                cc = extract_kv(kv_raw, 'cc')
                latency_val = int(latency_ms) if latency_ms and str(latency_ms).isdigit() else None

                payload_obj = {
                    'source_row_id': r['id'],
                    'source_table': 'stg_wc_log_hit',
                    'profile_id': args.profile_id,
                    'dt': str(r['dt']),
                    'ts': r['ts'].strftime('%Y-%m-%d %H:%M:%S') if isinstance(r['ts'], datetime) else str(r['ts']),
                    'event_name': event_name,
                    'service_domain': service_domain,
                    'funnel_stage': funnel_stage,
                    'is_conversion': is_conversion,
                    'uid': r.get('uid'),
                    'pcid': pcid,
                    'sid': sid,
                    'device_type': device_type,
                    'page_type': page_type,
                    'ip': r.get('ip'),
                    'method': r.get('method'),
                    'host': r.get('host'),
                    'path': r.get('path'),
                    'query': r.get('query'),
                    'status': r.get('status'),
                    'bytes': r.get('bytes'),
                    'latency_ms': latency_val,
                    'evt': evt,
                    'ref': r.get('ref'),
                }

                batch.append((
                    r['dt'], r['ts'], r['id'], args.profile_id,
                    event_name, service_domain, funnel_stage, is_conversion,
                    r.get('uid'), pcid, sid, device_type, page_type,
                    r.get('ip'), r.get('method'), r.get('host'), r.get('path'), r.get('query'),
                    r.get('url_raw'), r.get('url_full'), r.get('url_norm'),
                    r.get('status'), r.get('bytes'), latency_val,
                    r.get('ref'), r.get('ua'), r.get('kv_raw'),
                    evt, accept_lang, cc,
                    json.dumps(payload_obj, ensure_ascii=False),
                ))
                kafka_rows.append(payload_obj)

            if batch:
                cur.executemany(insert_sql, batch)
                inserted = cur.rowcount

        conn.commit()

        if producer:
            for row in kafka_rows:
                topic = kafka_topic(row.get('service_domain'), args.kafka_topic_prefix)
                producer.send(topic, key=kafka_key(row), value=row)
                sent += 1
            producer.flush()

        print(f"[event_log_raw_builder_final] rows_selected={selected_rows} inserted={inserted} kafka_sent={sent}")
    finally:
        if producer:
            producer.close()
        conn.close()


if __name__ == '__main__':
    main()
