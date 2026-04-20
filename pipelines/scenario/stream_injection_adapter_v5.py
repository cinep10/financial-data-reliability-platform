#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from datetime import date, datetime, timedelta
from typing import Any, Dict, List

import pymysql


INTENSITY_MAP = {
    "mild":   {"missing_ratio": 0.03, "duplicate_ratio": 0.02, "delay_ms": 1500,  "ordering_window": 5},
    "medium": {"missing_ratio": 0.08, "duplicate_ratio": 0.05, "delay_ms": 5000,  "ordering_window": 10},
    "high":   {"missing_ratio": 0.15, "duplicate_ratio": 0.10, "delay_ms": 15000, "ordering_window": 20},
}

ALL_DOMAINS = ["account", "auth", "branch", "card", "customer", "loan", "main", "other", "transfer"]


def connect_mysql(args):
    return pymysql.connect(
        host=args.db_host, port=args.db_port, user=args.db_user, password=args.db_pass,
        database=args.db_name, charset="utf8mb4", autocommit=False,
        cursorclass=pymysql.cursors.DictCursor
    )


def daterange(dt_from: str, dt_to: str):
    cur = datetime.strptime(dt_from, "%Y-%m-%d").date()
    end = datetime.strptime(dt_to, "%Y-%m-%d").date()
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def scenario_config(scenario_name: str, intensity: str) -> Dict[str, Any]:
    intensity = (intensity or "medium").lower()
    base = dict(INTENSITY_MAP.get(intensity, INTENSITY_MAP["medium"]))
    lname = (scenario_name or "baseline").lower()

    cfg = {
        "scenario_name": scenario_name or "baseline",
        "intensity": intensity,
        "mode": "baseline",
        "target_domains": ALL_DOMAINS[:],
        "missing_ratio": 0.0,
        "duplicate_ratio": 0.0,
        "delay_ms": 0,
        "ordering_window": 0,
    }

    for d in ALL_DOMAINS:
        if d in lname:
            cfg["target_domains"] = [d]
            break

    if "baseline" in lname:
        return cfg
    if "partial_missing" in lname or "weather_drop" in lname:
        cfg["mode"] = "partial_missing"
        cfg["missing_ratio"] = base["missing_ratio"]
        return cfg
    if "duplicate" in lname:
        cfg["mode"] = "duplicate"
        cfg["duplicate_ratio"] = base["duplicate_ratio"]
        return cfg
    if "delay" in lname or "auth_failure" in lname:
        cfg["mode"] = "delay"
        cfg["delay_ms"] = base["delay_ms"]
        return cfg
    if "ordering" in lname:
        cfg["mode"] = "ordering"
        cfg["ordering_window"] = base["ordering_window"]
        return cfg
    if "mixed" in lname:
        cfg["mode"] = "mixed"
        cfg["missing_ratio"] = base["missing_ratio"]
        cfg["duplicate_ratio"] = base["duplicate_ratio"]
        cfg["delay_ms"] = base["delay_ms"]
        cfg["ordering_window"] = base["ordering_window"]
        return cfg
    return cfg


def ensure_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stream_injection_event_queue (
            queue_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            profile_id VARCHAR(64) NOT NULL,
            dt DATE NOT NULL,
            ts DATETIME NOT NULL,
            raw_event_id BIGINT UNSIGNED NULL,
            event_name VARCHAR(100) NULL,
            service_domain VARCHAR(50) NULL,
            funnel_stage VARCHAR(50) NULL,
            is_conversion TINYINT(1) NOT NULL DEFAULT 0,
            uid VARCHAR(128) NULL,
            pcid VARCHAR(128) NULL,
            sid VARCHAR(128) NULL,
            status INT NULL,
            latency_ms INT NULL,
            source_type VARCHAR(50) NULL,
            path VARCHAR(255) NULL,
            evt VARCHAR(50) NULL,
            anomaly_tag VARCHAR(100) NULL,
            scenario_name VARCHAR(100) NULL,
            scenario_intensity VARCHAR(20) NULL,
            dup_group_id VARCHAR(64) NULL,
            ordering_group_id VARCHAR(64) NULL,
            queue_sequence BIGINT NULL,
            payload_json LONGTEXT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (queue_id),
            KEY idx_profile_dt (profile_id, dt),
            KEY idx_profile_ts (profile_id, ts),
            KEY idx_service_domain (service_domain),
            KEY idx_raw_event_id (raw_event_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scenario_adapter_result_log (
          adapter_result_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          hh TINYINT(4) NULL,
          scenario_name VARCHAR(100) NOT NULL,
          adapter_name VARCHAR(50) NOT NULL,
          result_metric VARCHAR(100) NOT NULL,
          result_value DECIMAL(20,6) NULL,
          result_status VARCHAR(20) NOT NULL DEFAULT 'ok',
          detail TEXT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (adapter_result_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


def fetch_run(cur, profile_id: str, dt: date) -> Dict[str, Any]:
    cur.execute("""
        SELECT
            profile_id,
            scenario_name,
            COALESCE(
                JSON_UNQUOTE(JSON_EXTRACT(parameters_json, '$.intensity')),
                'medium'
            ) AS intensity
        FROM scenario_experiment_run
        WHERE profile_id=%s
          AND dt_from <= %s
          AND dt_to >= %s
        ORDER BY dt_from DESC, scenario_name
        LIMIT 1
    """, (profile_id, dt, dt))
    row = cur.fetchone()
    if not row:
        return {"profile_id": profile_id, "scenario_name": "baseline", "intensity": "medium"}
    return row


def fetch_events(cur, dt: date) -> List[Dict[str, Any]]:
    cur.execute("""
        SELECT
            raw_event_id,
            dt,
            ts,
            COALESCE(evt, page_type, path, 'unknown') AS event_name,
            service_domain,
            funnel_stage,
            is_conversion,
            uid,
            pcid,
            sid,
            status,
            latency_ms,
            source_type,
            path,
            evt
        FROM event_log_raw
        WHERE dt=%s
        ORDER BY ts, raw_event_id
    """, (dt,))
    return cur.fetchall()


def insert_queue_rows(cur, profile_id: str, dt: date, scenario_name: str, intensity: str, rows: List[Dict[str, Any]]):
    sql = """
        INSERT INTO stream_injection_event_queue
        (
            profile_id, dt, ts, raw_event_id, event_name, service_domain, funnel_stage, is_conversion,
            uid, pcid, sid, status, latency_ms, source_type, path, evt, anomaly_tag,
            scenario_name, scenario_intensity, dup_group_id, ordering_group_id, queue_sequence, payload_json
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    seq = 0
    for r in rows:
        seq += 1
        payload = {
            "raw_event_id": r.get("raw_event_id"),
            "dt": str(r.get("dt")),
            "ts": str(r.get("ts")),
            "event_name": r.get("event_name"),
            "service_domain": r.get("service_domain"),
            "funnel_stage": r.get("funnel_stage"),
            "is_conversion": int(r.get("is_conversion") or 0),
            "uid": r.get("uid"),
            "pcid": r.get("pcid"),
            "sid": r.get("sid"),
            "status": r.get("status"),
            "latency_ms": r.get("latency_ms"),
            "source_type": r.get("source_type"),
            "path": r.get("path"),
            "evt": r.get("evt"),
            "anomaly_tag": r.get("anomaly_tag"),
        }
        cur.execute(sql, (
            profile_id, r["dt"], r["ts"], r.get("raw_event_id"), r.get("event_name"), r.get("service_domain"),
            r.get("funnel_stage"), int(r.get("is_conversion") or 0), r.get("uid"), r.get("pcid"), r.get("sid"),
            r.get("status"), r.get("latency_ms"), r.get("source_type"), r.get("path"), r.get("evt"),
            r.get("anomaly_tag"), scenario_name, intensity, r.get("dup_group_id"), r.get("ordering_group_id"),
            seq, json.dumps(payload, ensure_ascii=False)
        ))


def log_result(cur, profile_id: str, dt: date, scenario_name: str, metric: str, value: float, detail: Dict[str, Any] | None = None):
    cur.execute("""
        INSERT INTO scenario_adapter_result_log
        (profile_id, dt, scenario_name, adapter_name, result_metric, result_value, result_status, detail)
        VALUES (%s,%s,%s,'stream_injection_adapter_v5',%s,%s,'ok',%s)
    """, (profile_id, dt, scenario_name, metric, value, json.dumps(detail or {}, ensure_ascii=False)))


def apply_missing(rows: List[Dict[str, Any]], cfg: Dict[str, Any], rng: random.Random):
    kept = []
    dropped = 0
    for r in rows:
        if r.get("service_domain") in cfg["target_domains"] and rng.random() < cfg["missing_ratio"]:
            dropped += 1
            continue
        kept.append(dict(r))
    return kept, dropped


def apply_duplicate(rows: List[Dict[str, Any]], cfg: Dict[str, Any], rng: random.Random):
    out = []
    dup_cnt = 0
    for idx, r in enumerate(rows):
        out.append(dict(r))
        if r.get("service_domain") in cfg["target_domains"] and rng.random() < cfg["duplicate_ratio"]:
            rr = dict(r)
            rr["anomaly_tag"] = "duplicate"
            rr["dup_group_id"] = f"dup_{r.get('service_domain','all')}_{idx}"
            out.append(rr)
            dup_cnt += 1
    return out, dup_cnt


def apply_delay(rows: List[Dict[str, Any]], cfg: Dict[str, Any]):
    out = []
    delay_cnt = 0
    for r in rows:
        rr = dict(r)
        if rr.get("service_domain") in cfg["target_domains"]:
            rr["latency_ms"] = int(rr.get("latency_ms") or 0) + int(cfg["delay_ms"])
            rr["anomaly_tag"] = "delay"
            rr["ts"] = rr["ts"] + timedelta(milliseconds=int(cfg["delay_ms"]))
            delay_cnt += 1
        out.append(rr)
    return out, delay_cnt


def apply_ordering(rows: List[Dict[str, Any]], cfg: Dict[str, Any]):
    window = int(cfg["ordering_window"])
    if window <= 1:
        return rows, 0
    target = [dict(r) for r in rows if r.get("service_domain") in cfg["target_domains"]]
    other = [dict(r) for r in rows if r.get("service_domain") not in cfg["target_domains"]]
    if len(target) < 2:
        return rows, 0
    out_target = []
    changed = 0
    for i in range(0, len(target), window):
        block = target[i:i+window]
        if len(block) > 1:
            block = list(reversed(block))
            for j, rr in enumerate(block):
                rr["anomaly_tag"] = "ordering"
                rr["ordering_group_id"] = f"ord_{rr.get('service_domain','all')}_{i}"
                rr["ts"] = rr["ts"] + timedelta(milliseconds=j)
                changed += 1
        out_target.extend(block)
    merged = other + out_target
    merged.sort(key=lambda x: (x["ts"], x.get("raw_event_id") or 0))
    return merged, changed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-host", required=True)
    ap.add_argument("--db-port", type=int, required=True)
    ap.add_argument("--db-user", required=True)
    ap.add_argument("--db-pass", default="")
    ap.add_argument("--db-name", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--clear-range", action="store_true")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    conn = connect_mysql(args)
    try:
        with conn.cursor() as cur:
            ensure_tables(cur)

            if args.clear_range:
                cur.execute("""
                    DELETE FROM stream_injection_event_queue
                    WHERE profile_id=%s AND dt BETWEEN %s AND %s
                """, (args.profile_id, args.dt_from, args.dt_to))

            for dt in daterange(args.dt_from, args.dt_to):
                run = fetch_run(cur, args.profile_id, dt)
                cfg = scenario_config(run["scenario_name"], run["intensity"])
                rows = fetch_events(cur, dt)
                original_count = len(rows)

                working = [dict(r) for r in rows]
                dropped = dup_cnt = delay_cnt = ordering_cnt = 0

                if cfg["mode"] == "partial_missing":
                    working, dropped = apply_missing(working, cfg, rng)
                elif cfg["mode"] == "duplicate":
                    working, dup_cnt = apply_duplicate(working, cfg, rng)
                elif cfg["mode"] == "delay":
                    working, delay_cnt = apply_delay(working, cfg)
                elif cfg["mode"] == "ordering":
                    working, ordering_cnt = apply_ordering(working, cfg)
                elif cfg["mode"] == "mixed":
                    working, dropped = apply_missing(working, cfg, rng)
                    working, dup_cnt = apply_duplicate(working, cfg, rng)
                    working, delay_cnt = apply_delay(working, cfg)
                    working, ordering_cnt = apply_ordering(working, cfg)

                insert_queue_rows(cur, args.profile_id, dt, cfg["scenario_name"], cfg["intensity"], working)

                tags = Counter((r.get("anomaly_tag") or "normal") for r in working)
                detail = {
                    "mode": cfg["mode"],
                    "target_domains": cfg["target_domains"],
                    "missing_ratio": cfg["missing_ratio"],
                    "duplicate_ratio": cfg["duplicate_ratio"],
                    "delay_ms": cfg["delay_ms"],
                    "ordering_window": cfg["ordering_window"],
                    "original_rows": original_count,
                    "queue_rows": len(working),
                    "dropped_rows": dropped,
                    "duplicate_rows": dup_cnt,
                    "delay_rows": delay_cnt,
                    "ordering_rows": ordering_cnt,
                    "tag_counts": dict(tags),
                }
                log_result(cur, args.profile_id, dt, cfg["scenario_name"], "source_rows", original_count, detail)
                log_result(cur, args.profile_id, dt, cfg["scenario_name"], "queue_rows", len(working), detail)
                log_result(cur, args.profile_id, dt, cfg["scenario_name"], "missing_dropped_rows", dropped, detail)
                log_result(cur, args.profile_id, dt, cfg["scenario_name"], "duplicate_rows", dup_cnt, detail)
                log_result(cur, args.profile_id, dt, cfg["scenario_name"], "delay_rows", delay_cnt, detail)
                log_result(cur, args.profile_id, dt, cfg["scenario_name"], "ordering_rows", ordering_cnt, detail)

        conn.commit()
        print("[stream_injection_adapter_v5] done")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
