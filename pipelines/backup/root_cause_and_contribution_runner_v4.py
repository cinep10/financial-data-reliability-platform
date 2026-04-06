#!/usr/bin/env python3
import argparse
import os
from collections import defaultdict
from decimal import Decimal

import pymysql


FUNNEL_PAIRS = [
    ("auth_attempt_count", "auth_success_count", "auth", "auth_success_rate"),
    ("card_apply_start_count", "card_apply_submit_count", "card", "card_funnel_conversion"),
    ("loan_apply_start_count", "loan_apply_submit_count", "loan", "loan_funnel_conversion"),
    ("transfer_step1_count", "transfer_confirm_count", "transfer", "submit_capture_rate"),
]


def d(val) -> Decimal:
    try:
        return Decimal(str(val or 0))
    except Exception:
        return Decimal("0")


def severity_from_rate(rate: Decimal, fail_rate: Decimal, warn_rate: Decimal, high_rate: Decimal):
    if rate < fail_rate:
        return "fail"
    if rate < warn_rate:
        return "warn"
    if rate > high_rate:
        return "warn"
    return None


def fetchall_dict(cur):
    rows = cur.fetchall()
    return list(rows) if rows else []


def fetch_metric_map(cur, profile_id: str, dt: str):
    cur.execute(
        """
        SELECT metric_name, metric_value
        FROM metric_value_day
        WHERE profile_id=%s AND dt=%s
        """,
        (profile_id, dt),
    )
    rows = fetchall_dict(cur)
    return {r["metric_name"]: d(r["metric_value"]) for r in rows}


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
                    "DELETE FROM risk_signal_link_day WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                    (args.profile_id, args.dt_from, args.dt_to),
                )
                cur.execute(
                    "DELETE FROM data_risk_root_cause_day WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                    (args.profile_id, args.dt_from, args.dt_to),
                )
                conn.commit()

            cur.execute(
                """
                SELECT DISTINCT dt
                FROM metric_value_day
                WHERE profile_id=%s AND dt BETWEEN %s AND %s
                ORDER BY dt
                """,
                (args.profile_id, args.dt_from, args.dt_to),
            )
            dates = [str(r["dt"]) for r in fetchall_dict(cur)]

            for dt in dates:
                run_id = f"rootcause-{args.profile_id}-{dt}"
                metrics = fetch_metric_map(cur, args.profile_id, dt)

                # validation summary
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
                fail_count = int(validation.get("fail_count") or 0)
                warn_count = int(validation.get("warn_count") or 0)
                total_rules = int(validation.get("total_rules") or 0)

                # mapping coverage
                cur.execute(
                    """
                    SELECT mapping_coverage
                    FROM mapping_coverage_day
                    WHERE dt=%s
                    LIMIT 1
                    """,
                    (dt,),
                )
                mc = cur.fetchone() or {}
                mapping_coverage = d(mc.get("mapping_coverage") or 1)

                # drift
                cur.execute(
                    """
                    SELECT metric_name, drift_status, severity, drift_score
                    FROM metric_drift_result_r
                    WHERE profile_id=%s AND dt=%s
                    """,
                    (args.profile_id, dt),
                )
                drift_rows = fetchall_dict(cur)

                root_cause_candidates = []
                contribution_rows = []

                # funnel rules
                for start_metric, end_metric, group_name, rate_metric_name in FUNNEL_PAIRS:
                    start_val = d(metrics.get(start_metric))
                    end_val = d(metrics.get(end_metric))
                    if start_val <= 0:
                        continue

                    rate = end_val / start_val
                    sev = severity_from_rate(
                        rate, args.funnel_fail_rate, args.funnel_warn_rate, args.funnel_high_rate
                    )

                    contribution_rows.append(
                        (
                            args.profile_id,
                            dt,
                            "funnel",
                            rate_metric_name,
                            int((rate * 100).quantize(Decimal("1"))),
                            str((Decimal("1") - min(rate, Decimal("1"))) * Decimal("100")),
                            "high" if sev == "fail" else ("medium" if sev == "warn" else "low"),
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
                                "detail": f"{group_name} rate abnormal: {end_val}/{start_val}={rate:.4f}",
                            }
                        )

                # mapping
                if mapping_coverage < Decimal("0.95"):
                    code = "fail" if mapping_coverage < Decimal("0.80") else "warn"
                    conf = Decimal("0.90") if code == "fail" else Decimal("0.70")
                    root_cause_candidates.append(
                        {
                            "cause_type": "mapping_gap",
                            "cause_code": code,
                            "related_metric": "mapping_coverage",
                            "confidence": conf,
                            "detail": f"mapping_coverage={mapping_coverage:.4f}",
                        }
                    )
                    contribution_rows.append(
                        (
                            args.profile_id,
                            dt,
                            "mapping",
                            "mapping_coverage",
                            int((mapping_coverage * 100).quantize(Decimal("1"))),
                            str((Decimal("1") - mapping_coverage) * Decimal("100")),
                            "high" if code == "fail" else "medium",
                            run_id,
                        )
                    )

                # validation
                if fail_count > 0 or warn_count > 0:
                    validation_weight = Decimal(fail_count * 2 + warn_count)
                    contribution_rows.append(
                        (
                            args.profile_id,
                            dt,
                            "validation",
                            "validation_summary",
                            fail_count + warn_count,
                            str(validation_weight),
                            "high" if fail_count > 0 else "medium",
                            run_id,
                        )
                    )

                # drift rules
                seen_drift_metric = set()
                for row in drift_rows:
                    metric_name = row["metric_name"]
                    if metric_name in seen_drift_metric:
                        continue
                    seen_drift_metric.add(metric_name)

                    drift_status = (row.get("drift_status") or "").lower()
                    severity = (row.get("severity") or "").lower()
                    drift_score = d(row.get("drift_score"))

                    is_alert = drift_status in ("alert", "warn") or severity in ("high", "medium")
                    if not is_alert:
                        continue

                    contribution_rows.append(
                        (
                            args.profile_id,
                            dt,
                            "drift",
                            metric_name,
                            1,
                            str(drift_score if drift_score > 0 else Decimal("1")),
                            "high" if drift_status == "alert" or severity == "high" else "medium",
                            run_id,
                        )
                    )

                    if (
                        "conversion" in metric_name
                        or metric_name.endswith("_submit_count")
                        or metric_name.endswith("_success_count")
                        or metric_name in ("submit_capture_rate", "success_outcome_capture_rate")
                    ):
                        cause_type = "funnel_distortion"
                        confidence = Decimal("0.80")
                    else:
                        cause_type = "metric_drift"
                        confidence = Decimal("0.75")

                    root_cause_candidates.append(
                        {
                            "cause_type": cause_type,
                            "cause_code": drift_status if drift_status in ("alert", "warn") else "alert",
                            "related_metric": metric_name,
                            "confidence": confidence,
                            "detail": f"drift metric={metric_name}, status={drift_status}, severity={severity}, score={drift_score}",
                        }
                    )

                # dedupe root causes
                dedup = {}
                for item in root_cause_candidates:
                    key = (item["cause_type"], item["related_metric"], item["cause_code"])
                    if key not in dedup or item["confidence"] > dedup[key]["confidence"]:
                        dedup[key] = item

                ordered = sorted(
                    dedup.values(),
                    key=lambda x: (d(x["confidence"]), x["cause_type"], x["related_metric"]),
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

                # dedupe contributions / signal links
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
                         weighted_contribution, severity, run_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        row,
                    )

                print(
                    f"[OK] root cause completed: profile_id={args.profile_id}, dt={dt}, "
                    f"causes={len(ordered)}, links={len(seen_links)}"
                )

            conn.commit()

    finally:
        conn.close()


if __name__ == "__main__":
    main()
