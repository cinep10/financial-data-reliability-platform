#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List

import pymysql

# import 안정화
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
AI_DIR = os.path.dirname(CURRENT_DIR)
PIPELINES_DIR = os.path.dirname(AI_DIR)
PROJECT_ROOT = os.path.dirname(PIPELINES_DIR)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from pipelines.ai.common.explain_rules import explain_stream_issue
except ModuleNotFoundError:
    from ai.common.explain_rules import explain_stream_issue


def connect(args):
    return pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


DDL_SQL = """
CREATE TABLE IF NOT EXISTS ai_stream_incident_context_day (
    profile_id VARCHAR(64) NOT NULL,
    dt DATE NOT NULL,
    service_domain VARCHAR(50) NOT NULL,
    predicted_label VARCHAR(20) NULL,
    predicted_risk_score DOUBLE NULL,
    primary_stream_issue VARCHAR(50) NULL,
    technical_reason TEXT NULL,
    ops_reason TEXT NULL,
    short_message TEXT NULL,
    metrics_json JSON NULL,
    truth_label VARCHAR(20) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (profile_id, dt, service_domain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SELECT_SQL = """
SELECT
    p.profile_id,
    p.dt,
    p.service_domain,
    p.predicted_label,
    r.predicted_risk_score,
    s.primary_stream_issue,
    s.missing_rate,
    s.duplicate_ratio,
    s.ordering_gap_score,
    s.avg_event_delay_ms,
    t.truth_label
FROM stream_ml_prediction_day p
LEFT JOIN stream_ml_risk_prediction_day r
  ON p.profile_id = r.profile_id
 AND p.dt = r.dt
 AND p.service_domain = r.service_domain
LEFT JOIN stream_risk_signal_day s
  ON p.profile_id = s.profile_id
 AND p.dt = s.dt
 AND p.service_domain = s.service_domain
LEFT JOIN stream_anomaly_truth_day t
  ON p.profile_id = t.profile_id
 AND p.dt = t.dt
 AND p.service_domain = t.service_domain
WHERE p.profile_id = %s
  AND p.dt BETWEEN %s AND %s
ORDER BY p.dt, p.service_domain
"""

DELETE_SQL = """
DELETE FROM ai_stream_incident_context_day
WHERE profile_id = %s
  AND dt BETWEEN %s AND %s
"""

INSERT_SQL = """
INSERT INTO ai_stream_incident_context_day (
    profile_id,
    dt,
    service_domain,
    predicted_label,
    predicted_risk_score,
    primary_stream_issue,
    technical_reason,
    ops_reason,
    short_message,
    metrics_json,
    truth_label
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


from decimal import Decimal

def json_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} is not JSON serializable")

def build_metrics_json(row):
    metrics = {
        "missing_rate": row.get("missing_rate"),
        "duplicate_ratio": row.get("duplicate_ratio"),
        "ordering_gap_score": row.get("ordering_gap_score"),
        "avg_event_delay_ms": row.get("avg_event_delay_ms"),
        "predicted_risk": row.get("predicted_risk"),
    }
    return json.dumps(metrics, ensure_ascii=False, default=json_default)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    args = ap.parse_args()

    conn = connect(args)
    try:
        with conn.cursor() as cur:
            cur.execute(DDL_SQL)
            cur.execute(SELECT_SQL, (args.profile_id, args.dt_from, args.dt_to))
            rows = cur.fetchall()

            cur.execute(DELETE_SQL, (args.profile_id, args.dt_from, args.dt_to))

            insert_rows: List[tuple] = []
            for row in rows:
                explain = explain_stream_issue(row)
                insert_rows.append(
                    (
                        row["profile_id"],
                        row["dt"],
                        row["service_domain"],
                        row.get("predicted_label"),
                        row.get("predicted_risk"),
                        row.get("primary_stream_issue"),
                        explain["technical_reason"],
                        explain["ops_reason"],
                        explain["short_message"],
                        build_metrics_json(row),
                        row.get("truth_label"),
                    )
                )

            if insert_rows:
                cur.executemany(INSERT_SQL, insert_rows)

        conn.commit()
        print(f"[build_stream_ai_context_v1] done rows={len(insert_rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

