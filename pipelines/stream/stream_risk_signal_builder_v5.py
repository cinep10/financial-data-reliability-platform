#!/usr/bin/env python3
from __future__ import annotations
import argparse
import pymysql

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
    args = ap.parse_args()

    conn = connect_mysql(args)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stream_risk_signal_day (
                  profile_id VARCHAR(64) NOT NULL,
                  dt DATE NOT NULL,
                  service_domain VARCHAR(50) NOT NULL,
                  missing_rate DECIMAL(18,6) NOT NULL DEFAULT 0,
                  duplicate_ratio DECIMAL(18,6) NOT NULL DEFAULT 0,
                  ordering_gap_score DECIMAL(18,6) NOT NULL DEFAULT 0,
                  avg_event_delay_ms DECIMAL(18,6) NOT NULL DEFAULT 0,
                  stream_risk_score DECIMAL(18,6) NOT NULL DEFAULT 0,
                  primary_stream_issue VARCHAR(50) NULL,
                  status VARCHAR(20) NOT NULL DEFAULT 'ok',
                  note VARCHAR(255) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (profile_id, dt, service_domain)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(
                "DELETE FROM stream_risk_signal_day WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                (args.profile_id, args.dt_from, args.dt_to),
            )
            cur.execute("""
                 INSERT INTO stream_risk_signal_day
(
    profile_id,
    dt,
    service_domain,
    missing_rate,
    duplicate_ratio,
    ordering_gap_score,
    avg_event_delay_ms,
    stream_risk_score,
    primary_stream_issue,
    status,
    note
)
SELECT
    s.profile_id,
    s.dt,
    s.service_domain,

    s.max_missing_rate AS missing_rate,
    s.max_duplicate_ratio AS duplicate_ratio,
    s.max_ordering_gap_score AS ordering_gap_score,
    s.max_event_delay_ms AS avg_event_delay_ms,

    /* =========================
       RISK SCORE (threshold 기반)
       ========================= */
    GREATEST(
        CASE 
            WHEN s.max_missing_rate >= t.missing_warn THEN
                LEAST(
                    (s.max_missing_rate - t.missing_warn) /
                    NULLIF(t.missing_fail - t.missing_warn, 0),
                1) * 100 * t.missing_weight
            ELSE 0
        END,

        CASE 
            WHEN s.max_duplicate_ratio >= t.duplicate_warn THEN
                LEAST(
                    (s.max_duplicate_ratio - t.duplicate_warn) /
                    NULLIF(t.duplicate_fail - t.duplicate_warn, 0),
                1) * 100 * t.duplicate_weight
            ELSE 0
        END,

        CASE 
            WHEN s.max_event_delay_ms >= t.delay_warn THEN
                LEAST(
                    (s.max_event_delay_ms - t.delay_warn) /
                    NULLIF(t.delay_fail - t.delay_warn, 0),
                1) * 100 * t.delay_weight
            ELSE 0
        END
    ) AS stream_risk_score,

    /* =========================
       PRIMARY ISSUE
       ========================= */
    CASE
        WHEN s.max_missing_rate >= t.missing_warn THEN 'missing'
        WHEN s.max_duplicate_ratio >= t.duplicate_warn THEN 'duplicate'
        WHEN s.max_event_delay_ms >= t.delay_warn THEN 'delay'
        ELSE NULL
    END AS primary_stream_issue,

    /* =========================
       STATUS
       ========================= */
    CASE
        WHEN 
            GREATEST(
                CASE 
                    WHEN s.max_missing_rate >= t.missing_warn THEN
                        LEAST((s.max_missing_rate - t.missing_warn) /
                              NULLIF(t.missing_fail - t.missing_warn, 0),1) * 100 * t.missing_weight
                    ELSE 0
                END,
                CASE 
                    WHEN s.max_duplicate_ratio >= t.duplicate_warn THEN
                        LEAST((s.max_duplicate_ratio - t.duplicate_warn) /
                              NULLIF(t.duplicate_fail - t.duplicate_warn, 0),1) * 100 * t.duplicate_weight
                    ELSE 0
                END,
                CASE 
                    WHEN s.max_event_delay_ms >= t.delay_warn THEN
                        LEAST((s.max_event_delay_ms - t.delay_warn) /
                              NULLIF(t.delay_fail - t.delay_warn, 0),1) * 100 * t.delay_weight
                    ELSE 0
                END
            ) >= 70 THEN 'fail'

        WHEN 
            GREATEST(
                CASE 
                    WHEN s.max_missing_rate >= t.missing_warn THEN
                        LEAST((s.max_missing_rate - t.missing_warn) /
                              NULLIF(t.missing_fail - t.missing_warn, 0),1) * 100 * t.missing_weight
                    ELSE 0
                END,
                CASE 
                    WHEN s.max_duplicate_ratio >= t.duplicate_warn THEN
                        LEAST((s.max_duplicate_ratio - t.duplicate_warn) /
                              NULLIF(t.duplicate_fail - t.duplicate_warn, 0),1) * 100 * t.duplicate_weight
                    ELSE 0
                END,
                CASE 
                    WHEN s.max_event_delay_ms >= t.delay_warn THEN
                        LEAST((s.max_event_delay_ms - t.delay_warn) /
                              NULLIF(t.delay_fail - t.delay_warn, 0),1) * 100 * t.delay_weight
                    ELSE 0
                END
            ) >= 25 THEN 'warn'

        ELSE 'ok'
    END AS status,

    /* =========================
       NOTE
       ========================= */
    CASE
        WHEN s.max_missing_rate >= t.missing_warn THEN 'missing anomaly detected'
        WHEN s.max_duplicate_ratio >= t.duplicate_warn THEN 'duplicate anomaly detected'
        WHEN s.max_event_delay_ms >= t.delay_warn THEN 'delay anomaly detected'
        ELSE 'normal'
    END AS note

FROM stream_reliability_summary_day s
JOIN stream_risk_threshold_profile t
  ON s.profile_id = t.profile_id
 AND t.threshold_scope = 'auto_v3'

WHERE s.profile_id = %s
  AND s.dt BETWEEN %s AND %s
                        """, (args.profile_id, args.dt_from, args.dt_to))
        conn.commit()
        print("[stream_risk_signal_builder_v5] done")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
