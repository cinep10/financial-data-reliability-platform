#!/usr/bin/env python3
import argparse
import os
import pymysql


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.getenv("DB_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", "3306")))
    ap.add_argument("--user", default=os.getenv("DB_USER", "nethru"))
    ap.add_argument("--password", default=os.getenv("DB_PASSWORD", "nethru1234"))
    ap.add_argument("--db", default=os.getenv("DB_NAME", "weblog"))
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            if args.truncate:
                cur.execute(
                    "DELETE FROM data_reliability_action_day WHERE dt BETWEEN %s AND %s",
                    (args.dt_from, args.dt_to),
                )

            cur.execute(
                """
                SELECT rc.dt, rc.cause_type, rc.cause_code, rc.confidence,
                       COALESCE(rc.related_metric, 'ALL') AS related_metric
                FROM data_risk_root_cause_day rc
                WHERE rc.profile_id=%s
                  AND rc.dt BETWEEN %s AND %s
                ORDER BY rc.dt, rc.cause_rank
                """,
                (args.profile_id, args.dt_from, args.dt_to),
            )
            rows = cur.fetchall()

            for rc in rows:
                cause_type = rc["cause_type"]
                cause_code = rc["cause_code"]
                metric = rc["related_metric"]
                confidence = rc["confidence"]

                if cause_type in (
                    "auth_success_gap",
                    "card_funnel_break_at_submit",
                    "loan_funnel_break_at_submit",
                    "transfer_funnel_break_at_confirm",
                ):
                    action_type = "funnel_fix"
                    priority = "high" if cause_code == "fail" else "medium"
                    recommended_fix = f"Review funnel step around {metric}, completion logging, and UX drop causes."
                elif cause_type == "funnel_distortion":
                    action_type = "monitor_funnel"
                    priority = "medium"
                    recommended_fix = "Monitor conversion trend and review threshold sensitivity."
                elif cause_type == "scenario_linked_drift":
                    action_type = "scenario_validate"
                    priority = "medium"
                    recommended_fix = "Check campaign/weather/system_issue scenario context before treating as anomaly."
                elif cause_type == "traffic_mix_shift":
                    action_type = "traffic_investigate"
                    priority = "medium"
                    recommended_fix = "Inspect traffic source mix, campaign impact, and audience composition."
                elif cause_type == "metric_drift":
                    action_type = "anomaly_investigate"
                    priority = "medium"
                    recommended_fix = "Inspect metric-specific distribution shift and compare with baseline."
                elif cause_type == "mapping_gap":
                    action_type = "mapping_fix"
                    priority = "high" if cause_code == "fail" else "medium"
                    recommended_fix = "Review event mapping coverage and unmapped paths."
                else:
                    action_type = "manual_review"
                    priority = "low"
                    recommended_fix = "Manual review required."

                cur.execute(
                    """
                    REPLACE INTO data_reliability_action_day
                    (dt, metric_nm, root_cause, action_type, priority, confidence, recommended_fix)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (rc["dt"], metric, cause_type, action_type, priority, confidence, recommended_fix),
                )

            conn.commit()
            print(f"[OK] action engine completed: profile_id={args.profile_id}, rows={len(rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
