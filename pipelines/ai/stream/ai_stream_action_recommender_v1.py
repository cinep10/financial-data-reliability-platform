#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

import pymysql

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
AI_DIR = os.path.dirname(CURRENT_DIR)
PIPELINES_DIR = os.path.dirname(AI_DIR)
PROJECT_ROOT = os.path.dirname(PIPELINES_DIR)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from pipelines.ai.common.explain_rules import recommend_actions
except ModuleNotFoundError:
    from ai.common.explain_rules import recommend_actions


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
CREATE TABLE IF NOT EXISTS ai_recommended_action_day (
    profile_id VARCHAR(64) NOT NULL,
    dt DATE NOT NULL,
    service_domain VARCHAR(50) NOT NULL,
    priority VARCHAR(20) NULL,
    action_type VARCHAR(50) NULL,
    action_message TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (profile_id, dt, service_domain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SELECT_SQL = """
SELECT
    profile_id,
    dt,
    service_domain,
    predicted_label,
    predicted_risk_score,
    primary_stream_issue,
    technical_reason,
    ops_reason,
    short_message,
    truth_label
FROM ai_stream_incident_context_day
WHERE profile_id = %s
  AND dt BETWEEN %s AND %s
ORDER BY dt, service_domain
"""

DELETE_SQL = """
DELETE FROM ai_recommended_action_day
WHERE profile_id = %s
  AND dt BETWEEN %s AND %s
"""

INSERT_SQL = """
INSERT INTO ai_recommended_action_day (
    profile_id,
    dt,
    service_domain,
    priority,
    action_type,
    action_message
) VALUES (%s, %s, %s, %s, %s, %s)
"""


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
    ap.add_argument("--force-fallback", action="store_true")
    args = ap.parse_args()

    conn = connect(args)
    try:
        with conn.cursor() as cur:
            cur.execute(DDL_SQL)
            cur.execute(SELECT_SQL, (args.profile_id, args.dt_from, args.dt_to))
            rows = cur.fetchall()

            cur.execute(DELETE_SQL, (args.profile_id, args.dt_from, args.dt_to))

            insert_rows = []
            for row in rows:
                rec = recommend_actions(row)
                insert_rows.append(
                    (
                        row["profile_id"],
                        row["dt"],
                        row["service_domain"],
                        rec["priority"],
                        rec["action_type"],
                        rec["action_message"],
                    )
                )

            if insert_rows:
                cur.executemany(INSERT_SQL, insert_rows)

        conn.commit()
        print(f"[ai_stream_action_recommender_v1] done rows={len(insert_rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
