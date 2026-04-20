
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

def ensure_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stream_reliability_summary_minute (
          profile_id VARCHAR(64) NOT NULL DEFAULT 'default',
          metric_minute DATETIME NOT NULL,
          service_domain VARCHAR(50) NOT NULL DEFAULT 'all',
          missing_rate DECIMAL(18,6) NOT NULL DEFAULT 0,
          duplicate_ratio DECIMAL(18,6) NOT NULL DEFAULT 0,
          ordering_gap_score DECIMAL(18,6) NOT NULL DEFAULT 0,
          avg_event_delay_ms DECIMAL(18,6) NOT NULL DEFAULT 0,
          stream_risk_score DECIMAL(18,6) NOT NULL DEFAULT 0,
          issue_flags VARCHAR(255) NULL,
          note VARCHAR(255) NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (profile_id, metric_minute, service_domain)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stream_reliability_summary_day (
          profile_id VARCHAR(64) NOT NULL DEFAULT 'default',
          dt DATE NOT NULL,
          service_domain VARCHAR(50) NOT NULL DEFAULT 'all',
          avg_missing_rate DECIMAL(18,6) NOT NULL DEFAULT 0,
          max_missing_rate DECIMAL(18,6) NOT NULL DEFAULT 0,
          avg_duplicate_ratio DECIMAL(18,6) NOT NULL DEFAULT 0,
          max_duplicate_ratio DECIMAL(18,6) NOT NULL DEFAULT 0,
          max_ordering_gap_score DECIMAL(18,6) NOT NULL DEFAULT 0,
          total_ordering_violations BIGINT NOT NULL DEFAULT 0,
          avg_event_delay_ms DECIMAL(18,6) NOT NULL DEFAULT 0,
          max_event_delay_ms DECIMAL(18,6) NOT NULL DEFAULT 0,
          stream_risk_score DECIMAL(18,6) NOT NULL DEFAULT 0,
          primary_stream_issue VARCHAR(50) NULL,
          note VARCHAR(255) NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (profile_id, dt, service_domain)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-host", default="127.0.0.1")
    ap.add_argument("--db-port", type=int, default=3306)
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
            ensure_tables(cur)
            cur.execute(
                "DELETE FROM stream_reliability_summary_minute WHERE profile_id=%s AND DATE(metric_minute) BETWEEN %s AND %s",
                (args.profile_id, args.dt_from, args.dt_to),
            )
            cur.execute(
                "DELETE FROM stream_reliability_summary_day WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                (args.profile_id, args.dt_from, args.dt_to),
            )

            cur.execute("""
                INSERT INTO stream_reliability_summary_minute
                (profile_id, metric_minute, service_domain, missing_rate, duplicate_ratio, ordering_gap_score, avg_event_delay_ms, stream_risk_score, issue_flags, note)
                SELECT
                    g.profile_id,
                    g.metric_minute,
                    g.service_domain,
                    COALESCE(c.missing_rate, 0) AS missing_rate,
                    COALESCE(d.duplicate_ratio, 0) AS duplicate_ratio,
                    COALESCE(o.ordering_gap_score, 0) AS ordering_gap_score,
                    COALESCE(l.avg_event_delay_ms, 0) AS avg_event_delay_ms,
                    GREATEST(
                        LEAST(COALESCE(c.missing_rate, 0) / 0.10, 1.0) * 85,
                        LEAST(COALESCE(d.duplicate_ratio, 0) / 0.20, 1.0) * 70,
                        LEAST(COALESCE(o.ordering_gap_score, 0) / 20.0, 1.0) * 55,
                        LEAST(GREATEST(COALESCE(l.avg_event_delay_ms, 0) - 1000, 0) / 10000.0, 1.0) * 35
                    ) AS stream_risk_score,
                    CONCAT_WS(',',
                        CASE WHEN COALESCE(c.missing_rate, 0) >= 0.01 THEN 'missing' END,
                        CASE WHEN COALESCE(d.duplicate_ratio, 0) >= 0.02 THEN 'duplicate' END,
                        CASE WHEN COALESCE(o.ordering_gap_score, 0) >= 5 THEN 'ordering' END,
                        CASE WHEN COALESCE(l.avg_event_delay_ms, 0) >= 1500 THEN 'delay' END
                    ) AS issue_flags,
                    'tuned minute risk: lower baseline ordering false-positive, stronger missing/duplicate weighting' AS note
                FROM (
                    SELECT profile_id, metric_minute, service_domain FROM stream_completeness_result WHERE profile_id=%s AND DATE(metric_minute) BETWEEN %s AND %s
                    UNION
                    SELECT profile_id, metric_minute, service_domain FROM stream_duplicate_result WHERE profile_id=%s AND DATE(metric_minute) BETWEEN %s AND %s
                    UNION
                    SELECT profile_id, metric_minute, service_domain FROM stream_ordering_result WHERE profile_id=%s AND DATE(metric_minute) BETWEEN %s AND %s
                    UNION
                    SELECT profile_id, metric_minute, service_domain FROM stream_latency_result WHERE profile_id=%s AND DATE(metric_minute) BETWEEN %s AND %s
                ) g
                LEFT JOIN stream_completeness_result c
                  ON g.profile_id=c.profile_id AND g.metric_minute=c.metric_minute AND g.service_domain=c.service_domain
                LEFT JOIN stream_duplicate_result d
                  ON g.profile_id=d.profile_id AND g.metric_minute=d.metric_minute AND g.service_domain=d.service_domain
                LEFT JOIN stream_ordering_result o
                  ON g.profile_id=o.profile_id AND g.metric_minute=o.metric_minute AND g.service_domain=o.service_domain
                LEFT JOIN stream_latency_result l
                  ON g.profile_id=l.profile_id AND g.metric_minute=l.metric_minute AND g.service_domain=l.service_domain
            """, (
                args.profile_id, args.dt_from, args.dt_to,
                args.profile_id, args.dt_from, args.dt_to,
                args.profile_id, args.dt_from, args.dt_to,
                args.profile_id, args.dt_from, args.dt_to,
            ))

            cur.execute("""
                INSERT INTO stream_reliability_summary_day
                (profile_id, dt, service_domain, avg_missing_rate, max_missing_rate, avg_duplicate_ratio, max_duplicate_ratio,
                 max_ordering_gap_score, total_ordering_violations, avg_event_delay_ms, max_event_delay_ms, stream_risk_score,
                 primary_stream_issue, note)
                SELECT
                    profile_id,
                    DATE(metric_minute) AS dt,
                    service_domain,
                    AVG(missing_rate),
                    MAX(missing_rate),
                    AVG(duplicate_ratio),
                    MAX(duplicate_ratio),
                    MAX(ordering_gap_score),
                    SUM(CASE WHEN ordering_gap_score >= 5 THEN 1 ELSE 0 END),
                    AVG(avg_event_delay_ms),
                    MAX(avg_event_delay_ms),
                    GREATEST(
                        LEAST(MAX(missing_rate) / 0.10, 1.0) * 85,
                        LEAST(MAX(duplicate_ratio) / 0.20, 1.0) * 70,
                        LEAST(MAX(ordering_gap_score) / 20.0, 1.0) * 55,
                        LEAST(GREATEST(MAX(avg_event_delay_ms) - 1000, 0) / 10000.0, 1.0) * 35
                    ) AS stream_risk_score,
                    CASE
                        WHEN LEAST(MAX(missing_rate) / 0.10, 1.0) * 85 >= GREATEST(
                            LEAST(MAX(duplicate_ratio) / 0.20, 1.0) * 70,
                            LEAST(MAX(ordering_gap_score) / 20.0, 1.0) * 55,
                            LEAST(GREATEST(MAX(avg_event_delay_ms) - 1000, 0) / 10000.0, 1.0) * 35
                        ) AND MAX(missing_rate) >= 0.01 THEN 'missing'
                        WHEN LEAST(MAX(duplicate_ratio) / 0.20, 1.0) * 70 >= GREATEST(
                            LEAST(MAX(missing_rate) / 0.10, 1.0) * 85,
                            LEAST(MAX(ordering_gap_score) / 20.0, 1.0) * 55,
                            LEAST(GREATEST(MAX(avg_event_delay_ms) - 1000, 0) / 10000.0, 1.0) * 35
                        ) AND MAX(duplicate_ratio) >= 0.02 THEN 'duplicate'
                        WHEN LEAST(MAX(ordering_gap_score) / 20.0, 1.0) * 55 >= GREATEST(
                            LEAST(MAX(missing_rate) / 0.10, 1.0) * 85,
                            LEAST(MAX(duplicate_ratio) / 0.20, 1.0) * 70,
                            LEAST(GREATEST(MAX(avg_event_delay_ms) - 1000, 0) / 10000.0, 1.0) * 35
                        ) AND MAX(ordering_gap_score) >= 5 THEN 'ordering'
                        WHEN MAX(avg_event_delay_ms) >= 1500 THEN 'delay'
                        ELSE NULL
                    END AS primary_stream_issue,
                    'tuned daily risk: issue determined by strongest normalized component' AS note
                FROM stream_reliability_summary_minute
                WHERE profile_id=%s AND DATE(metric_minute) BETWEEN %s AND %s
                GROUP BY profile_id, DATE(metric_minute), service_domain
            """, (args.profile_id, args.dt_from, args.dt_to))

        conn.commit()
        print("[stream_reliability_aggregator_v5] done")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
