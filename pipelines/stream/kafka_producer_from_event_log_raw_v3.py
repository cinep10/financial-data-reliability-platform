#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, date
from typing import Any, Dict

import pymysql
from kafka import KafkaProducer


def json_default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat(sep=" ")
    return str(obj)


def connect_mysql(args):
    return pymysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_pass,
        database=args.db_name,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def normalize_row(row: Dict[str, Any], topic: str) -> Dict[str, Any]:
    ts = row.get("ts")
    if isinstance(ts, str):
        producer_ts = ts
    elif ts is not None:
        producer_ts = ts.isoformat(sep=" ")
    else:
        producer_ts = None

    return {
        "raw_event_id": row.get("raw_event_id") or row.get("id"),
        "dt": str(row.get("dt")) if row.get("dt") is not None else (producer_ts[:10] if producer_ts else None),
        "ts": producer_ts,
        "event_name": row.get("event_name") or row.get("evt") or row.get("page_type") or "unknown",
        "service_domain": row.get("service_domain") or "other",
        "funnel_stage": row.get("funnel_stage") or "browse",
        "is_conversion": int(row.get("is_conversion") or 0),
        "uid": row.get("uid"),
        "pcid": row.get("pcid"),
        "sid": row.get("sid"),
        "producer_ts": producer_ts,
        "status": int(row.get("status") or 200),
        "latency_ms": int(row.get("latency_ms") or 0),
        "source_type": row.get("source_type") or "weblog",
        "path": row.get("path"),
        "evt": row.get("evt"),
        "anomaly_tag": row.get("anomaly_tag"),
        "stream_topic": topic,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-host", required=True)
    ap.add_argument("--db-port", type=int, default=3306)
    ap.add_argument("--db-user", required=True)
    ap.add_argument("--db-pass", default="")
    ap.add_argument("--db-name", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--kafka-bootstrap", required=True)
    args = ap.parse_args()

    conn = connect_mysql(args)
    producer = KafkaProducer(
        bootstrap_servers=args.kafka_bootstrap.split(","),
        value_serializer=lambda x: json.dumps(x, default=json_default).encode("utf-8"),
        key_serializer=lambda x: str(x).encode("utf-8") if x is not None else None,
    )

    sent = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM event_log_raw
                WHERE dt BETWEEN %s AND %s
                ORDER BY dt, ts, raw_event_id
                """,
                (args.dt_from, args.dt_to),
            )
            rows = cur.fetchall()

            for row in rows:
                payload = normalize_row(row, args.topic)
                key = payload.get("raw_event_id") or f"{payload.get('dt')}:{sent}"
                producer.send(args.topic, key=key, value=payload)
                sent += 1

        producer.flush()
    finally:
        producer.close()
        conn.close()

    print(f"[kafka_producer_from_event_log_raw_v3] topic={args.topic} sent={sent}")


if __name__ == "__main__":
    main()
