#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
from datetime import datetime, date
from decimal import Decimal

import pymysql


def json_default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    return str(obj)


def connect_mysql(args):
    return pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
        autocommit=False,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_result_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS scenario_injection_result_log (
            injection_result_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            injection_id BIGINT UNSIGNED NULL,
            profile_id VARCHAR(64) NOT NULL,
            dt DATE NOT NULL,
            scenario_name VARCHAR(100) NOT NULL,
            target_table VARCHAR(100) NOT NULL,
            anomaly_type VARCHAR(50) NOT NULL,
            target_row_count INT NOT NULL DEFAULT 0,
            affected_row_count INT NOT NULL DEFAULT 0,
            result_status VARCHAR(20) NOT NULL DEFAULT 'ok',
            detail JSON NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (injection_result_id),
            KEY idx_scenario_injection_result_log_01 (profile_id, dt, scenario_name),
            KEY idx_scenario_injection_result_log_02 (target_table, anomaly_type, result_status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def fetch_specs(cur, profile_id: str, dt_from: str, dt_to: str):
    cur.execute(
        """
        SELECT *
        FROM scenario_injection_log
        WHERE profile_id=%s
          AND dt BETWEEN %s AND %s
          AND target_table='stg_event_stream'
          AND status='ready'
        ORDER BY dt, injection_id
        """,
        (profile_id, dt_from, dt_to),
    )
    return cur.fetchall()


def fetch_target_ids(cur, dt: str):
    cur.execute(
        """
        SELECT stream_ingest_id
        FROM stg_event_stream
        WHERE dt=%s
        ORDER BY stream_ingest_id
        """,
        (dt,),
    )
    return [r["stream_ingest_id"] for r in cur.fetchall()]


def mark_missing(cur, dt: str, ids):
    if not ids:
        return 0
    fmt = ",".join(["%s"] * len(ids))
    cur.execute(
        f"""
        DELETE FROM stg_event_stream
        WHERE dt=%s
          AND stream_ingest_id IN ({fmt})
        """,
        [dt] + ids,
    )
    return cur.rowcount


def inject_duplicate(cur, dt: str, ids):
    if not ids:
        return 0
    fmt = ",".join(["%s"] * len(ids))
    cur.execute(
        f"""
        INSERT INTO stg_event_stream
        (
            raw_event_id, dt, ts, event_name, service_domain, funnel_stage,
            is_conversion, uid, pcid, sid,
            stream_topic, stream_partition, stream_offset, sequence_no,
            producer_ts, ingest_ts, event_delay_ms,
            status, latency_ms, source_type, path, evt,
            load_status, anomaly_tag
        )
        SELECT
            raw_event_id, dt, ts, event_name, service_domain, funnel_stage,
            is_conversion, uid, pcid, sid,
            stream_topic, stream_partition, stream_offset, sequence_no,
            producer_ts, ingest_ts, event_delay_ms,
            status, latency_ms, source_type, path, evt,
            load_status,
            CASE
              WHEN anomaly_tag IS NULL OR anomaly_tag='' THEN 'duplicate_injected'
              ELSE CONCAT(anomaly_tag, ',duplicate_injected')
            END
        FROM stg_event_stream
        WHERE dt=%s
          AND stream_ingest_id IN ({fmt})
        """,
        [dt] + ids,
    )
    return cur.rowcount


def inject_delay(cur, dt: str, ids, delay_ms: int):
    if not ids or delay_ms <= 0:
        return 0
    fmt = ",".join(["%s"] * len(ids))
    cur.execute(
        f"""
        UPDATE stg_event_stream
        SET
            ingest_ts = DATE_ADD(ingest_ts, INTERVAL %s MICROSECOND),
            event_delay_ms =
                CASE
                    WHEN producer_ts IS NOT NULL AND ingest_ts IS NOT NULL
                    THEN TIMESTAMPDIFF(MICROSECOND, producer_ts, DATE_ADD(ingest_ts, INTERVAL %s MICROSECOND)) / 1000
                    ELSE event_delay_ms
                END,
            anomaly_tag =
                CASE
                    WHEN anomaly_tag IS NULL OR anomaly_tag='' THEN 'delay_injected'
                    ELSE CONCAT(anomaly_tag, ',delay_injected')
                END
        WHERE dt=%s
          AND stream_ingest_id IN ({fmt})
        """,
        [delay_ms * 1000, delay_ms * 1000, dt] + ids,
    )
    return cur.rowcount


def inject_ordering_break(cur, dt: str, ids):
    if len(ids) < 2:
        return 0

    affected = 0
    ids = sorted(ids)

    for i in range(0, len(ids) - 1, 2):
        a = ids[i]
        b = ids[i + 1]

        cur.execute(
            """
            SELECT stream_ingest_id, sequence_no
            FROM stg_event_stream
            WHERE dt=%s AND stream_ingest_id IN (%s, %s)
            ORDER BY stream_ingest_id
            """,
            (dt, a, b),
        )
        rows = cur.fetchall()
        if len(rows) != 2:
            continue

        seq1 = rows[0]["sequence_no"]
        seq2 = rows[1]["sequence_no"]

        # sequence_no가 있으면 서로 바꿔서 ordering break
        if seq1 is not None and seq2 is not None:
            cur.execute(
                """
                UPDATE stg_event_stream
                SET
                    sequence_no=%s,
                    anomaly_tag=CASE
                        WHEN anomaly_tag IS NULL OR anomaly_tag='' THEN 'ordering_injected'
                        ELSE CONCAT(anomaly_tag, ',ordering_injected')
                    END
                WHERE stream_ingest_id=%s
                """,
                (seq2, a),
            )
            affected += cur.rowcount

            cur.execute(
                """
                UPDATE stg_event_stream
                SET
                    sequence_no=%s,
                    anomaly_tag=CASE
                        WHEN anomaly_tag IS NULL OR anomaly_tag='' THEN 'ordering_injected'
                        ELSE CONCAT(anomaly_tag, ',ordering_injected')
                    END
                WHERE stream_ingest_id=%s
                """,
                (seq1, b),
            )
            affected += cur.rowcount
        else:
            # sequence_no 없으면 producer_ts를 뒤틀기
            cur.execute(
                """
                UPDATE stg_event_stream
                SET
                    producer_ts = DATE_SUB(producer_ts, INTERVAL 3 SECOND),
                    anomaly_tag=CASE
                        WHEN anomaly_tag IS NULL OR anomaly_tag='' THEN 'ordering_injected'
                        ELSE CONCAT(anomaly_tag, ',ordering_injected')
                    END
                WHERE dt=%s AND stream_ingest_id=%s
                """,
                (dt, b),
            )
            affected += cur.rowcount

    return affected


def write_result(cur, spec, target_count: int, affected_count: int, detail: dict):
    cur.execute(
        """
        INSERT INTO scenario_injection_result_log
        (injection_id, profile_id, dt, scenario_name, target_table, anomaly_type, target_row_count, affected_row_count, result_status, detail)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            spec["injection_id"],
            spec["profile_id"],
            spec["dt"],
            spec["scenario_name"],
            spec["target_table"],
            spec["anomaly_type"],
            target_count,
            affected_count,
            "ok",
            json.dumps(detail, ensure_ascii=False, default=json_default),
        ),
    )


def main():
    ap = argparse.ArgumentParser(description="Apply scenario-driven stream anomalies from scenario_injection_log")
    ap.add_argument("--host", default=os.getenv("DB_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", "3306")))
    ap.add_argument("--user", default=os.getenv("DB_USER", "nethru"))
    ap.add_argument("--password", default=os.getenv("DB_PASSWORD", "nethru1234"))
    ap.add_argument("--db", default=os.getenv("DB_NAME", "weblog"))
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    random.seed(args.seed)

    conn = connect_mysql(args)
    try:
        with conn.cursor() as cur:
            ensure_result_table(cur)
            specs = fetch_specs(cur, args.profile_id, args.dt_from, args.dt_to)
            total_specs = 0

            for spec in specs:
                dt = str(spec["dt"])
                ids = fetch_target_ids(cur, dt)
                target_count = len(ids)

                if target_count == 0:
                    write_result(cur, spec, 0, 0, {"message": "no target rows"})
                    continue

                affected = 0
                example_ids = []

                if spec["missing_rate"] and float(spec["missing_rate"]) > 0:
                    n = max(1, int(target_count * float(spec["missing_rate"])))
                    missing_ids = random.sample(ids, min(n, len(ids)))
                    example_ids.extend(missing_ids[:5])
                    affected += mark_missing(cur, dt, missing_ids)

                if spec["duplicate_rate"] and float(spec["duplicate_rate"]) > 0:
                    n = max(1, int(target_count * float(spec["duplicate_rate"])))
                    dup_ids = random.sample(ids, min(n, len(ids)))
                    example_ids.extend(dup_ids[:5])
                    affected += inject_duplicate(cur, dt, dup_ids)

                if spec["ordering_break_rate"] and float(spec["ordering_break_rate"]) > 0:
                    n = max(2, int(target_count * float(spec["ordering_break_rate"])))
                    ord_ids = random.sample(ids, min(n, len(ids)))
                    example_ids.extend(ord_ids[:5])
                    affected += inject_ordering_break(cur, dt, ord_ids)

                if spec["delay_ms"] and int(spec["delay_ms"]) > 0:
                    n = max(1, int(target_count * 0.2))
                    delay_ids = random.sample(ids, min(n, len(ids)))
                    example_ids.extend(delay_ids[:5])
                    affected += inject_delay(cur, dt, delay_ids, int(spec["delay_ms"]))

                detail = {
                    "anomaly_type": spec["anomaly_type"],
                    "missing_rate": float(spec["missing_rate"] or 0),
                    "duplicate_rate": float(spec["duplicate_rate"] or 0),
                    "ordering_break_rate": float(spec["ordering_break_rate"] or 0),
                    "delay_ms": int(spec["delay_ms"] or 0),
                    "example_stream_ingest_ids": example_ids[:10],
                }

                write_result(cur, spec, target_count, affected, detail)
                cur.execute(
                    "UPDATE scenario_injection_log SET status='applied' WHERE injection_id=%s",
                    (spec["injection_id"],),
                )
                total_specs += 1

        conn.commit()
        print(f"[OK] generate_stream_anomaly_cases_v2 completed: profile={args.profile_id}, specs={total_specs}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
