#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime

import pymysql
from kafka import KafkaConsumer


INSERT_SQL = """
INSERT INTO stg_event_stream
(
    raw_event_id,
    dt,
    ts,
    event_name,
    service_domain,
    funnel_stage,
    is_conversion,
    uid,
    pcid,
    sid,
    stream_topic,
    stream_partition,
    stream_offset,
    sequence_no,
    producer_ts,
    ingest_ts,
    event_delay_ms,
    status,
    latency_ms,
    source_type,
    path,
    evt,
    load_status,
    anomaly_tag
)
VALUES
(
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s
)
"""


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db-host', required=True)
    ap.add_argument('--db-port', type=int, default=3306)
    ap.add_argument('--db-user', required=True)
    ap.add_argument('--db-pass', default='')
    ap.add_argument('--db-name', required=True)
    ap.add_argument('--kafka-bootstrap', required=True)
    ap.add_argument('--topic', required=True)
    ap.add_argument('--consumer-group', required=True)
    ap.add_argument('--truncate-target-for-date', required=True)
    ap.add_argument('--max-messages', type=int, default=50000)
    ap.add_argument('--idle-timeout-sec', type=int, default=10)
    ap.add_argument('--poll-timeout-ms', type=int, default=1000)
    return ap.parse_args()


def connect_mysql(args):
    return pymysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_pass,
        database=args.db_name,
        charset='utf8mb4',
        autocommit=False,
    )


def parse_dt(s):
    if not s:
        return None
    if isinstance(s, datetime):
        return s
    s = str(s).replace('T', ' ')
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


def row_tuple(msg_topic, msg_partition, msg_offset, value, seq, ingest_dt):
    producer_ts = parse_dt(value.get('producer_ts') or value.get('ts'))
    event_dt = value.get('dt') or (producer_ts.date().isoformat() if producer_ts else None)
    event_ts = value.get('ts') or value.get('producer_ts')
    latency_ms = int(value.get('latency_ms') or 0)

    return (
        value.get('raw_event_id'),
        event_dt,
        event_ts,
        value.get('event_name') or value.get('evt') or value.get('page_type') or 'unknown',
        value.get('service_domain') or 'other',
        value.get('funnel_stage') or 'browse',
        int(value.get('is_conversion') or 0),
        value.get('uid'),
        value.get('pcid'),
        value.get('sid'),
        msg_topic,
        msg_partition,
        msg_offset,
        seq,
        producer_ts.strftime('%Y-%m-%d %H:%M:%S') if producer_ts else event_ts,
        ingest_dt.strftime('%Y-%m-%d %H:%M:%S'),
        latency_ms,
        int(value.get('status') or 200),
        latency_ms,
        value.get('source_type') or 'weblog',
        value.get('path'),
        value.get('evt'),
        'loaded',
        value.get('anomaly_tag'),
    )


def main():
    args = parse_args()
    conn = connect_mysql(args)
    cur = conn.cursor()

    cur.execute('DELETE FROM stg_event_stream WHERE dt = %s', (args.truncate_target_for_date,))
    conn.commit()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.kafka_bootstrap.split(','),
        group_id=args.consumer_group,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode('utf-8')),
    )

    print(f'[INFO] consumer started topic={args.topic} group={args.consumer_group}')

    total = 0
    seq = 0
    batch = []
    last_msg_time = time.time()

    def flush():
        nonlocal batch
        if not batch:
            return
        cur.executemany(INSERT_SQL, batch)
        conn.commit()
        print(f'[kafka_consumer_v4] flushed={len(batch)} total={total}')
        batch = []

    try:
        while True:
            polled = consumer.poll(timeout_ms=args.poll_timeout_ms)
            now = time.time()
            got_any = False

            for tp, messages in polled.items():
                for msg in messages:
                    got_any = True
                    seq += 1
                    total += 1
                    last_msg_time = now
                    ingest_dt = datetime.now()
                    batch.append(row_tuple(tp.topic, tp.partition, msg.offset, msg.value, seq, ingest_dt))

                    if len(batch) >= 1000:
                        flush()

                    if total >= args.max_messages:
                        flush()
                        print(f'[kafka_consumer_v4] reached max_messages={args.max_messages}')
                        print(f'[kafka_consumer_v4] consumed={total}')
                        return

            if not got_any and (now - last_msg_time) >= args.idle_timeout_sec:
                flush()
                print(f'[kafka_consumer_v4] idle timeout reached ({args.idle_timeout_sec}s), exiting')
                break
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    print(f'[kafka_consumer_v4] consumed={total}')


if __name__ == '__main__':
    main()
