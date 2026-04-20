#!/usr/bin/env python3
from __future__ import annotations
import argparse
import pymysql

def connect_mysql(args):
    return pymysql.connect(host=args.db_host, port=args.db_port, user=args.db_user, password=args.db_pass,
                           database=args.db_name, charset="utf8mb4", autocommit=False,
                           cursorclass=pymysql.cursors.DictCursor)

def clamp01(v):
    return 0.0 if v < 0 else (1.0 if v > 1 else v)

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
                DELETE FROM data_risk_root_cause_day
                WHERE profile_id=%s AND dt BETWEEN %s AND %s AND cause_source='stream'
            """, (args.profile_id, args.dt_from, args.dt_to))

            cur.execute("""
                SELECT profile_id, dt, service_domain, missing_rate, duplicate_ratio, ordering_gap_score,
                       avg_event_delay_ms, stream_risk_score, primary_stream_issue
                FROM stream_risk_signal_day
                WHERE profile_id=%s AND dt BETWEEN %s AND %s
                ORDER BY dt, service_domain
            """, (args.profile_id, args.dt_from, args.dt_to))
            rows = cur.fetchall()
            inserted = 0

            for r in rows:
                causes = []
                if float(r.get("missing_rate") or 0) >= 0.03:
                    causes.append(("stream_missing", f"stream_missing_{r['service_domain']}", clamp01(float(r["missing_rate"]) * 5), f"Missing rate high in {r['service_domain']}: {float(r['missing_rate']):.4f}"))
                if float(r.get("duplicate_ratio") or 0) >= 0.01:
                    causes.append(("stream_duplicate", f"stream_duplicate_{r['service_domain']}", clamp01(float(r["duplicate_ratio"]) * 5), f"Duplicate ratio high in {r['service_domain']}: {float(r['duplicate_ratio']):.4f}"))
                if float(r.get("ordering_gap_score") or 0) > 0:
                    causes.append(("stream_ordering", f"stream_ordering_{r['service_domain']}", clamp01(float(r["ordering_gap_score"]) / 10.0), f"Ordering gap in {r['service_domain']}: ordering_gap_score={float(r['ordering_gap_score']):.4f}"))
                if float(r.get("avg_event_delay_ms") or 0) >= 1000:
                    causes.append(("stream_delay", f"stream_delay_{r['service_domain']}", clamp01(float(r["avg_event_delay_ms"]) / 30000.0), f"Delay high in {r['service_domain']}: avg_event_delay_ms={float(r['avg_event_delay_ms']):.2f}"))

                if not causes and r.get("primary_stream_issue"):
                    issue = r["primary_stream_issue"]
                    causes.append((f"stream_{issue}", f"stream_{issue}_{r['service_domain']}", 0.5, f"Primary stream issue={issue} in {r['service_domain']}"))

                causes.sort(key=lambda x: x[2], reverse=True)
                for rank, c in enumerate(causes[:4], start=1):
                    cur.execute("""
                        INSERT INTO data_risk_root_cause_day
                        (
                            profile_id, dt, cause_rank, cause_type, cause_code, confidence,
                            driver_source, related_metric, observed_value, baseline_value,
                            detail, run_id, cause_score, cause_source, cause_desc
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                            cause_type = VALUES(cause_type),
                            cause_code = VALUES(cause_code),
                            confidence = VALUES(confidence),
                            driver_source = VALUES(driver_source),
                            related_metric = VALUES(related_metric),
                            observed_value = VALUES(observed_value),
                            baseline_value = VALUES(baseline_value),
                            detail = VALUES(detail),
                            run_id = VALUES(run_id),
                            cause_score = VALUES(cause_score),
                            cause_source = VALUES(cause_source),
                            cause_desc = VALUES(cause_desc)
                    """, (
                        r["profile_id"], r["dt"], rank, c[0], c[1], c[2],
                        "stream", "stream_risk_score", float(r.get("stream_risk_score") or 0), 0.0,
                        c[3], f"stream_bridge_v4_{r['profile_id']}_{r['dt']}", float(r.get("stream_risk_score") or 0), "stream", c[3]
                    ))
                    inserted += 1
        conn.commit()
        print(f"[stream_to_root_cause_bridge_v4] done inserted={inserted}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
