from __future__ import annotations

import argparse
import pymysql


ACTION_MAP = {
    "stream_missing": (
        "consumer_check",
        "high",
        "Check consumer health, offsets, and dropped-message path."
    ),
    "stream_duplicate": (
        "dedup_fix",
        "medium",
        "Review retry/resend path and apply deduplication policy."
    ),
    "stream_ordering_issue": (
        "ordering_fix",
        "high",
        "Review partition ordering assumptions and sequence handling."
    ),
    "stream_latency_spike": (
        "lag_investigate",
        "high",
        "Investigate consumer lag, backlog, and processing delay."
    ),
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


def derive_priority(score: float, default_priority: str) -> str:
    if score >= 8:
        return "high"
    if score >= 3:
        return "medium"
    return default_priority


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
            # 같은 날짜/원인(root_cause)별로 1건만 만들기
            cur.execute(
                """
                SELECT
                    profile_id,
                    dt,
                    primary_stream_issue,
                    MAX(stream_risk_score) AS max_stream_risk_score
                FROM stream_risk_signal_day
                WHERE profile_id=%s
                  AND dt BETWEEN %s AND %s
                  AND stream_risk_score > 0
                GROUP BY profile_id, dt, primary_stream_issue
                ORDER BY dt, primary_stream_issue
                """,
                (args.profile_id, args.dt_from, args.dt_to),
            )
            rows = cur.fetchall()

            for r in rows:
                cause_type = str(r["primary_stream_issue"] or "stream_issue")
                score = float(r["max_stream_risk_score"] or 0)

                action_type, default_priority, action_desc = ACTION_MAP.get(
                    cause_type,
                    ("stream_review", "medium", "Review stream reliability issue."),
                )
                priority = derive_priority(score, default_priority)
                confidence = min(1.0, max(0.0, score / 10.0))

                cur.execute(
                    """
                    INSERT INTO data_reliability_action_day
                    (
                        dt,
                        metric_nm,
                        root_cause,
                        action_type,
                        priority,
                        confidence,
                        profile_id,
                        action_priority,
                        action_source,
                        action_desc
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        action_type = VALUES(action_type),
                        priority = VALUES(priority),
                        confidence = VALUES(confidence),
                        profile_id = VALUES(profile_id),
                        action_priority = VALUES(action_priority),
                        action_source = VALUES(action_source),
                        action_desc = VALUES(action_desc)
                    """,
                    (
                        r["dt"],
                        "stream_risk_score",
                        cause_type,
                        action_type,
                        priority,
                        confidence,
                        r["profile_id"],
                        priority,
                        "stream",
                        action_desc,
                    ),
                )

        conn.commit()
        print("[stream_to_action_bridge] done")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
