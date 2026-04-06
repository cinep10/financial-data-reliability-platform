#!/usr/bin/env python3
import argparse
import os
from decimal import Decimal

import pymysql


FUNNEL_PAIRS = [
    ("auth_attempt_count", "auth_success_count", "auth"),
    ("card_apply_start_count", "card_apply_submit_count", "card"),
    ("loan_apply_start_count", "loan_apply_submit_count", "loan"),
    ("transfer_step1_count", "transfer_confirm_count", "transfer"),
]


def severity_from_rate(rate: Decimal, fail_rate: Decimal, warn_rate: Decimal, high_rate: Decimal):
    if rate < fail_rate:
        return "fail"
    if rate < warn_rate:
        return "warn"
    if rate > high_rate:
        return "warn"
    return None


def dictfetchall(cur):
    rows = cur.fetchall()
    out = []
    for row in rows:
        if isinstance(row, dict):
            out.append(row)
        else:
            out.append(dict(row))
    return out


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
    ap.add_argument("--funnel-fail-rate", type=Decimal, default=Decimal("0.15"))
    ap.add_argument("--funnel-warn-rate", type=Decimal, default=Decimal("0.25"))
    ap.add_argument("--funnel-high-rate", type=Decimal, default=Decimal("0.95"))
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
                    """
                    DELETE FROM risk_signal_link_day
                    WHERE profile_id=%s AND dt BETWEEN %s AND %s
                    """,
                    (args.profile_id, args.dt_from, args.dt_to),
                )
                cur.execute(
                    """
                    DELETE FROM data_risk_root_cause_day
                    WHERE profile_id=%s AND dt BETWEEN %s AND %s
                    """,
                    (args.profile_id, args.dt_from, args.dt_to),
                )
                conn.commit()

            cur.execute(
                """
                SELECT DISTINCT dt
                FROM metric_value_day
                WHERE profile_id=%s
                  AND dt BETWEEN %s AND %s
                ORDER BY dt
                """,
                (args.profile_id, args.dt_from, args.dt_to),
            )
            dts = [r["dt"] for r in dictfetchall(cur)]

            for dt in dts:
                run_id = f"rootcause-{args.profile_id}-{dt}"

                cur.execute(
                    """
                    SELECT metric_name, metric_value
                    FROM metric_value_day
                    WHERE profile_id=%s AND dt=%s
                    """,
                    (args.profile_id, dt),
                )
                metric_rows = dictfetchall(cur)
                metrics = {
                    r["metric_name"]: Decimal(str(r["metric_value"] or 0))
                    for r in metric_rows
                }

                cur.execute(
                    """
                    SELECT metric_name, drift_status, severity, drift_score
                    FROM metric_drift_result_r
                    WHERE profile_id=%s AND dt=%s
                    """,
                    (args.profile_id, dt),
                )
                drift_rows = dictfetchall(cur)

                cur.execute(
                    """
                    SELECT total_rules, pass_count, warn_count, fail_count
                    FROM validation_summary_day
                    WHERE profile_id=%s AND dt=%s
                    ORDER BY validation_run_id DESC
                    LIMIT 1
                    """,
                    (args.profile_id, dt),
                )
                validation = cur.fetchone() or {}

                cur.execute(
                    """
                    SELECT mapping_coverage
                    FROM mapping_coverage_day
                    WHERE dt=%s
                    ORDER BY dt DESC
                    LIMIT 1
                    """,
                    (dt,),
                )
                mc = cur.fetchone() or {}
                mapping_coverage = Decimal(str(mc.get("mapping_coverage") or 1))

                contribution_rows = []
                root_cause_candidates = []

                for start_metric, end_metric, group_name in FUNNEL_PAIRS:
                    start_val = Decimal(str(metrics.get(start_metric, 0)))
                    end_val = Decimal(str(metrics.get(end_metric, 0)))

                    if start_val <= 0:
                        continue

                    rate = end_val / start_val if start_val > 0 else Decimal("0")
                    sev = severity_from_rate(
                        rate, args.funnel_fail_rate, args.funnel_warn_rate, args.funnel_high_rate
                    )

                    contribution_rows.append(
                        (
                            args.profile_id, dt, "funnel", f"{group_name}_conversion",
                            int((rate * 100).quantize(Decimal("1"))),
                            str((Decimal("1") - min(rate, Decimal("1"))) * Decimal("100")),
                            "medium" if sev == "warn" else ("high" if sev == "fail" else "low"),
                            f"start={start_metric}, end={end_metric}, rate={rate:.4f}",
                            run_id,
                        )
                    )

                    if sev:
                        confidence = Decimal("0.90") if sev == "fail" else Decimal("0.70")
                        root_cause_candidates.append(
                            {
                                "cause_type": "funnel_distortion",
                                "cause_code": sev,
                                "related_metric": end_metric,
                                "confidence": confidence,
                                "detail": f"{group_name} conversion abnormal: {end_val}/{start_val}={rate:.4f}",
                            }
                        )

                if mapping_coverage < Decimal("0.80"):
                    root_cause_candidates.append(
                        {
                            "cause_type": "mapping_gap",
                            "cause_code": "warn",
                            "related_metric": "mapping_coverage",
                            "confidence": Decimal("0.70"),
                            "detail": f"mapping_coverage={mapping_coverage:.4f}",
                        }
                    )
                    contribution_rows.append(
                        (
                            args.profile_id, dt, "mapping", "mapping_coverage",
                            int((mapping_coverage * 100).quantize(Decimal("1"))),
                            str((Decimal("1") - mapping_coverage) * Decimal("100")),
                            "high" if mapping_coverage < Decimal("0.50") else "medium",
                            f"mapping_coverage={mapping_coverage:.4f}",
                            run_id,
                        )
                    )

                warn_count = int(validation.get("warn_count") or 0)
                fail_count = int(validation.get("fail_count") or 0)
                total_rules = int(validation.get("total_rules") or 0)
                if fail_count > 0 or warn_count > 0:
                    val_weight = Decimal(fail_count * 2 + warn_count)
                    contribution_rows.append(
                        (
                            args.profile_id, dt, "validation", "validation_summary",
                            fail_count + warn_count,
                            str(val_weight),
                            "high" if fail_count > 0 else "medium",
                            f"fail={fail_count}, warn={warn_count}, total_rules={total_rules}",
                            run_id,
                        )
                    )

                drift_seen = set()
                for row in drift_rows:
                    metric_name = row["metric_name"]
                    drift_status = (row.get("drift_status") or "").lower()
                    severity = (row.get("severity") or "").lower()
                    drift_score = Decimal(str(row.get("drift_score") or 0))

                    if metric_name in drift_seen:
                        continue
                    drift_seen.add(metric_name)

                    if drift_status in ("alert", "warn") or severity in ("high", "medium"):
                        contribution_rows.append(
                            (
                                args.profile_id, dt, "drift", metric_name,
                                1,
                                str(drift_score if drift_score > 0 else Decimal("1")),
                                "high" if drift_status == "alert" or severity == "high" else "medium",
                                f"drift_status={drift_status}, severity={severity}, drift_score={drift_score}",
                                run_id,
                            )
                        )

                        if "conversion" in metric_name or metric_name.endswith("_submit_count") or metric_name.endswith("_success_count"):
                            cause_type = "funnel_distortion"
                        else:
                            cause_type = "metric_drift"

                        root_cause_candidates.append(
                            {
                                "cause_type": cause_type,
                                "cause_code": drift_status if drift_status in ("alert", "warn") else "alert",
                                "related_metric": metric_name,
                                "confidence": Decimal("0.75") if cause_type == "metric_drift" else Decimal("0.80"),
                                "detail": f"drift metric={metric_name}, status={drift_status}, severity={severity}, score={drift_score}",
                            }
                        )

                dedup = {}
                for item in root_cause_candidates:
                    key = (item["cause_type"], item["related_metric"], item["cause_code"])
                    if key not in dedup or item["confidence"] > dedup[key]["confidence"]:
                        dedup[key] = item

                ordered = sorted(
                    dedup.values(),
                    key=lambda x: (Decimal(str(x["confidence"])), x["cause_type"], x["related_metric"]),
                    reverse=True,
                )[:5]

                for rank, item in enumerate(ordered, start=1):
                    cur.execute(
                        """
                        INSERT INTO data_risk_root_cause_day
                        (profile_id, dt, cause_rank, cause_type, cause_code, confidence,
                         driver_source, related_metric, observed_value, baseline_value, detail, run_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            args.profile_id,
                            dt,
                            rank,
                            item["cause_type"],
                            item["cause_code"],
                            str(item["confidence"]),
                            "derived",
                            item["related_metric"],
                            None,
                            None,
                            item["detail"],
                            run_id,
                        ),
                    )

                seen_links = set()
                for row in contribution_rows:
                    key = (row[2], row[3])
                    if key in seen_links:
                        continue
                    seen_links.add(key)
                    cur.execute(
                        """
                        INSERT INTO risk_signal_link_day
                        (profile_id, dt, signal_group, signal_name, signal_count,
                         weighted_contribution, severity, detail, run_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        row,
                    )

                print(f"[OK] root cause completed: profile_id={args.profile_id}, dt={dt}, causes={len(ordered)}, links={len(seen_links)}")

            conn.commit()

    finally:
        conn.close()


if __name__ == "__main__":
    main()
