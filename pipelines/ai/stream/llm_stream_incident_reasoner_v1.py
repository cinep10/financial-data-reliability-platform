#!/usr/bin/env python3
from __future__ import annotations

import argparse
import pymysql


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
CREATE TABLE IF NOT EXISTS ai_incident_summary_day (
    profile_id VARCHAR(64) NOT NULL,
    dt DATE NOT NULL,
    service_domain VARCHAR(50) NOT NULL,
    incident_summary TEXT NULL,
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
DELETE FROM ai_incident_summary_day
WHERE profile_id = %s
  AND dt BETWEEN %s AND %s
"""

INSERT_SQL = """
INSERT INTO ai_incident_summary_day (
    profile_id,
    dt,
    service_domain,
    incident_summary
) VALUES (%s, %s, %s, %s)
"""


def build_summary(row: dict) -> str:
    predicted_label = row.get("predicted_label") or "normal"
    predicted_risk = row.get("predicted_risk")
    primary_issue = row.get("primary_stream_issue") or predicted_label
    technical_reason = row.get("technical_reason") or ""
    ops_reason = row.get("ops_reason") or ""
    truth_label = row.get("truth_label") or "unknown"

    return (
        f"[{row['service_domain']}] predicted_label={predicted_label}, "
        f"predicted_risk={predicted_risk}, primary_issue={primary_issue}. "
        f"Technical: {technical_reason}. "
        f"Ops: {ops_reason}. "
        f"Truth={truth_label}."
    )


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
                summary = build_summary(row)
                insert_rows.append(
                    (
                        row["profile_id"],
                        row["dt"],
                        row["service_domain"],
                        summary,
                    )
                )

            if insert_rows:
                cur.executemany(INSERT_SQL, insert_rows)

        conn.commit()
        print(f"[llm_stream_incident_reasoner_v1] done rows={len(insert_rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
