#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import timedelta
from typing import Dict, Any, List, Set

import pymysql


INTENSITY_MAP = {
    "mild": {
        "missing_rate": 0.03,
        "duplicate_rate": 0.02,
        "delay_ms": 1500,
        "shuffle_window": 5,
    },
    "medium": {
        "missing_rate": 0.08,
        "duplicate_rate": 0.05,
        "delay_ms": 5000,
        "shuffle_window": 10,
    },
    "high": {
        "missing_rate": 0.15,
        "duplicate_rate": 0.10,
        "delay_ms": 15000,
        "shuffle_window": 20,
    },
}


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


def parse_scenario(scenario_name: str, intensity: str) -> Dict[str, Any]:
    cfg = {
        "scenario_name": scenario_name,
        "intensity": intensity,
        "mode": "baseline",
        "target_service_domain": None,
        "missing_rate": 0.0,
        "duplicate_rate": 0.0,
        "delay_ms": 0,
        "shuffle_window": 0,
        "drop_enabled": False,
        "duplicate_enabled": False,
        "delay_enabled": False,
        "ordering_enabled": False,
    }
    base = INTENSITY_MAP.get(intensity, INTENSITY_MAP["medium"])

    if scenario_name == "baseline":
        return cfg

    if scenario_name.startswith("partial_missing_"):
        domain = scenario_name.replace("partial_missing_", "", 1)
        cfg.update({
            "mode": "partial_missing",
            "target_service_domain": domain,
            "missing_rate": base["missing_rate"],
            "drop_enabled": True,
        })
    elif scenario_name.startswith("duplicate_"):
        domain = scenario_name.replace("duplicate_", "", 1)
        cfg.update({
            "mode": "duplicate",
            "target_service_domain": domain,
            "duplicate_rate": base["duplicate_rate"],
            "duplicate_enabled": True,
        })
    elif scenario_name.startswith("delay_"):
        domain = scenario_name.replace("delay_", "", 1)
        cfg.update({
            "mode": "delay",
            "target_service_domain": domain,
            "delay_ms": base["delay_ms"],
            "delay_enabled": True,
        })
    elif scenario_name.startswith("ordering_"):
        domain = scenario_name.replace("ordering_", "", 1)
        cfg.update({
            "mode": "ordering",
            "target_service_domain": domain,
            "shuffle_window": base["shuffle_window"],
            "ordering_enabled": True,
        })
    elif scenario_name.startswith("mixed_"):
        domain = scenario_name.replace("mixed_", "", 1)
        cfg.update({
            "mode": "mixed",
            "target_service_domain": domain,
            "missing_rate": base["missing_rate"],
            "duplicate_rate": base["duplicate_rate"],
            "delay_ms": base["delay_ms"],
            "shuffle_window": base["shuffle_window"],
            "drop_enabled": True,
            "duplicate_enabled": True,
            "delay_enabled": True,
            "ordering_enabled": True,
        })
    return cfg


def table_columns(cur, table_name: str) -> Set[str]:
    cur.execute(f"SHOW COLUMNS FROM {table_name}")
    return {r["Field"] for r in cur.fetchall()}


def build_insert_sql(table_name: str, cols: List[str]) -> str:
    placeholders = ",".join(["%s"] * len(cols))
    return f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({placeholders})"


def build_update_sql(table_name: str, cols: List[str], key_cols: Set[str]) -> str:
    non_keys = [c for c in cols if c not in key_cols]
    if not non_keys:
        return ""
    updates = ",".join([f"{c}=VALUES({c})" for c in non_keys])
    return f" ON DUPLICATE KEY UPDATE {updates}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-host", required=True)
    ap.add_argument("--db-port", type=int, default=3306)
    ap.add_argument("--db-user", required=True)
    ap.add_argument("--db-pass", default="")
    ap.add_argument("--db-name", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--target-table", default="exogenous_state_timeline")
    ap.add_argument("--clear-range", action="store_true")
    args = ap.parse_args()

    conn = connect_mysql(args)
    try:
        with conn.cursor() as cur:
            cols = table_columns(cur, args.target_table)

            if args.clear_range and {"profile_id", "dt"}.issubset(cols):
                cur.execute(
                    f"DELETE FROM {args.target_table} WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                    (args.profile_id, args.dt_from, args.dt_to),
                )

            cur.execute(
                """
                SELECT profile_id, scenario_name, scenario_type, dt_from, dt_to, parameters_json
                FROM scenario_experiment_run
                WHERE profile_id=%s
                  AND dt_from >= %s
                  AND dt_to <= %s
                ORDER BY dt_from, dt_to, scenario_name
                """,
                (args.profile_id, args.dt_from, args.dt_to),
            )
            rows = cur.fetchall()
            inserted = 0

            for row in rows:
                params = json.loads(row.get("parameters_json") or "{}")
                intensity = params.get("intensity", "medium")
                cfg = parse_scenario(row["scenario_name"], intensity)

                day = row["dt_from"]
                while day <= row["dt_to"]:
                    payload = {
                        "scenario_name": row["scenario_name"],
                        "scenario_type": row.get("scenario_type") or "unified",
                        **cfg,
                    }

                    record: Dict[str, Any] = {}
                    if "profile_id" in cols:
                        record["profile_id"] = row["profile_id"]
                    if "dt" in cols:
                        record["dt"] = day
                    if "hh" in cols:
                        record["hh"] = 23
                    if "scenario_name" in cols:
                        record["scenario_name"] = row["scenario_name"]
                    if "scenario_type" in cols:
                        record["scenario_type"] = row.get("scenario_type") or "unified"
                    if "intensity" in cols:
                        record["intensity"] = intensity
                    if "target_service_domain" in cols:
                        record["target_service_domain"] = cfg["target_service_domain"]
                    if "mode" in cols:
                        record["mode"] = cfg["mode"]
                    if "state_json" in cols:
                        record["state_json"] = json.dumps(payload, ensure_ascii=False)
                    if "state_value" in cols:
                        record["state_value"] = json.dumps(payload, ensure_ascii=False)

                    if not record:
                        raise RuntimeError(f"No compatible writable columns found in {args.target_table}")

                    key_cols = {"profile_id", "dt", "hh", "scenario_name"} & set(record.keys())
                    sql = build_insert_sql(args.target_table, list(record.keys()))
                    upd = build_update_sql(args.target_table, list(record.keys()), key_cols)
                    cur.execute(sql + upd, list(record.values()))
                    inserted += 1

                    day = day + timedelta(days=1)

        conn.commit()
        print(f"[build_exogenous_timeline_from_registry_v3] done inserted={inserted}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
