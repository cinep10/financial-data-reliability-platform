#!/usr/bin/env python3
import argparse
import os
import pymysql


def table_exists(cur, table_name):
    cur.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name = %s
        """,
        (table_name,),
    )
    return int(cur.fetchone()["cnt"]) > 0


def get_columns(cur, table_name):
    cur.execute(f"DESC {table_name}")
    return [r["Field"] for r in cur.fetchall()]


def pick_source_table(cur):
    if table_exists(cur, "data_risk_root_cause_day"):
        return "data_risk_root_cause_day"
    if table_exists(cur, "root_cause_result"):
        return "root_cause_result"
    raise SystemExit("No root cause source table found")


def build_insert(cur, target_table):
    cols = get_columns(cur, target_table)
    insert_cols = [c for c in ["profile_id", "dt", "metric_nm", "root_cause", "action_type", "priority", "confidence", "recommended_fix"] if c in cols]
    ph = ", ".join(["%s"] * len(insert_cols))
    sql = f"REPLACE INTO {target_table} ({', '.join(insert_cols)}) VALUES ({ph})"
    return insert_cols, sql


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
            target_table = "data_reliability_action_day"
            if not table_exists(cur, target_table):
                raise SystemExit("Target table data_reliability_action_day not found")
            source_table = pick_source_table(cur)
            target_cols = get_columns(cur, target_table)

            if args.truncate:
                if "profile_id" in target_cols:
                    cur.execute(
                        f"DELETE FROM {target_table} WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                        (args.profile_id, args.dt_from, args.dt_to),
                    )
                else:
                    cur.execute(
                        f"DELETE FROM {target_table} WHERE dt BETWEEN %s AND %s",
                        (args.dt_from, args.dt_to),
                    )

            if source_table == "data_risk_root_cause_day":
                cur.execute(
                    """
                    SELECT profile_id, dt, cause_type, cause_code, confidence,
                           COALESCE(related_metric, 'ALL') AS related_metric
                    FROM data_risk_root_cause_day
                    WHERE profile_id=%s
                      AND dt BETWEEN %s AND %s
                    ORDER BY dt, cause_rank
                    """,
                    (args.profile_id, args.dt_from, args.dt_to),
                )
            else:
                cur.execute(
                    """
                    SELECT profile_id, dt,
                           COALESCE(cause_type, root_cause_type, 'unknown') AS cause_type,
                           COALESCE(cause_code, severity, 'info') AS cause_code,
                           COALESCE(confidence, contribution_score, 0.5) AS confidence,
                           COALESCE(related_metric, metric_name, 'ALL') AS related_metric
                    FROM root_cause_result
                    WHERE profile_id=%s
                      AND dt BETWEEN %s AND %s
                    ORDER BY dt
                    """,
                    (args.profile_id, args.dt_from, args.dt_to),
                )
            rows = cur.fetchall()

            insert_cols, insert_sql = build_insert(cur, target_table)

            for rc in rows:
                cause_type = rc["cause_type"]
                cause_code = str(rc.get("cause_code") or "")
                metric = rc["related_metric"]
                confidence = rc.get("confidence")

                if cause_type in (
                    "auth_success_gap",
                    "card_funnel_break_at_submit",
                    "loan_funnel_break_at_submit",
                    "transfer_funnel_break_at_confirm",
                ):
                    action_type = "funnel_fix"
                    priority = "high" if cause_code in ("fail", "high") else "medium"
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
                    priority = "high" if cause_code in ("fail", "high") else "medium"
                    recommended_fix = "Review event mapping coverage and unmapped paths."
                else:
                    action_type = "manual_review"
                    priority = "low"
                    recommended_fix = "Manual review required."

                value_map = {
                    "profile_id": rc.get("profile_id"),
                    "dt": rc.get("dt"),
                    "metric_nm": metric,
                    "root_cause": cause_type,
                    "action_type": action_type,
                    "priority": priority,
                    "confidence": confidence,
                    "recommended_fix": recommended_fix,
                }
                cur.execute(insert_sql, tuple(value_map[c] for c in insert_cols))

            conn.commit()
            print(f"[OK] action engine completed: profile_id={args.profile_id}, rows={len(rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
