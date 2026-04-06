#!/usr/bin/env python3
import argparse
import os
from decimal import Decimal
import pymysql

FUNNEL_PAIRS = [
    ("auth_attempt_count", "auth_success_count", "auth", "auth_success_rate", "auth_success_gap"),
    ("card_apply_start_count", "card_apply_submit_count", "card", "card_funnel_conversion", "card_funnel_break_at_submit"),
    ("loan_apply_start_count", "loan_apply_submit_count", "loan", "loan_funnel_conversion", "loan_funnel_break_at_submit"),
    ("transfer_step1_count", "transfer_confirm_count", "transfer", "submit_capture_rate", "transfer_funnel_break_at_confirm"),
]

def d(val):
    try:
        return Decimal(str(val or 0))
    except Exception:
        return Decimal("0")

def severity_from_rate(rate, fail_rate, warn_rate, high_rate):
    if rate < fail_rate:
        return "fail"
    if rate < warn_rate:
        return "warn"
    if rate > high_rate:
        return "warn"
    return None

def table_exists(cur, table_name):
    cur.execute("SHOW TABLES LIKE %s", (table_name,))
    return cur.fetchone() is not None

def table_columns(cur, table_name):
    cur.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
        (table_name,),
    )
    return {r["COLUMN_NAME"] for r in cur.fetchall()}

def fetch_metric_map(cur, profile_id, dt):
    cur.execute("SELECT metric_name, metric_value FROM metric_value_day WHERE profile_id=%s AND dt=%s", (profile_id, dt))
    return {r["metric_name"]: d(r["metric_value"]) for r in cur.fetchall()}

def fetch_scenario_context(cur, profile_id, dt):
    if not table_exists(cur, "scenario_experiment_result_day"):
        return []
    cols = table_columns(cur, "scenario_experiment_result_day")
    where = []
    params = []
    if "profile_id" in cols:
        where.append("profile_id=%s")
        params.append(profile_id)
    if "dt" in cols:
        where.append("dt=%s")
        params.append(dt)
    elif "scenario_date" in cols:
        where.append("scenario_date=%s")
        params.append(dt)
    if not where:
        return []
    preferred_label_cols = ["scenario_name", "scenario_type", "scenario_key", "experiment_name", "scenario_label"]
    preferred_status_cols = ["scenario_value", "status", "severity", "scenario_status", "label"]
    label_col = next((c for c in preferred_label_cols if c in cols), None)
    status_col = next((c for c in preferred_status_cols if c in cols), None)
    selected = ["*"] if not label_col else [label_col]
    if status_col and status_col not in selected:
        selected.append(status_col)
    sql = f"SELECT {', '.join(selected)} FROM scenario_experiment_result_day WHERE " + " AND ".join(where)
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    return [(str(r.get(label_col) or "scenario") if label_col else "scenario", str(r.get(status_col) or "") if status_col else "") for r in rows]

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

    conn = pymysql.connect(host=args.host, port=args.port, user=args.user, password=args.password, database=args.db, autocommit=False, cursorclass=pymysql.cursors.DictCursor)
    try:
        with conn.cursor() as cur:
            if args.truncate:
                cur.execute("DELETE FROM risk_signal_link_day WHERE profile_id=%s AND dt BETWEEN %s AND %s", (args.profile_id, args.dt_from, args.dt_to))
                cur.execute("DELETE FROM data_risk_root_cause_day WHERE profile_id=%s AND dt BETWEEN %s AND %s", (args.profile_id, args.dt_from, args.dt_to))
                conn.commit()

            cur.execute("SELECT DISTINCT dt FROM metric_value_day WHERE profile_id=%s AND dt BETWEEN %s AND %s ORDER BY dt", (args.profile_id, args.dt_from, args.dt_to))
            dates = [str(r["dt"]) for r in cur.fetchall()]

            for dt in dates:
                run_id = f"rootcause-{args.profile_id}-{dt}"
                metrics = fetch_metric_map(cur, args.profile_id, dt)

                cur.execute("SELECT total_rules, warn_count, fail_count FROM validation_summary_day WHERE profile_id=%s AND dt=%s ORDER BY validation_run_id DESC LIMIT 1", (args.profile_id, dt))
                validation = cur.fetchone() or {}
                total_rules = max(int(validation.get("total_rules") or 0), 1)
                fail_count = int(validation.get("fail_count") or 0)
                warn_count = int(validation.get("warn_count") or 0)

                cur.execute("SELECT mapping_coverage FROM mapping_coverage_day WHERE dt=%s LIMIT 1", (dt,))
                mc = cur.fetchone() or {}
                mapping_coverage = d(mc.get("mapping_coverage") or 1)

                cur.execute("""
                    SELECT metric_name, drift_status, severity, drift_score,
                           COALESCE(observed_value,0) AS observed_value,
                           COALESCE(baseline_value,0) AS baseline_value
                    FROM metric_drift_result_r
                    WHERE profile_id=%s AND dt=%s
                """, (args.profile_id, dt))
                drift_rows = cur.fetchall()
                scenarios = fetch_scenario_context(cur, args.profile_id, dt)

                root_cause_candidates = []
                contribution_rows = []

                for start_metric, end_metric, group_name, rate_metric_name, cause_type in FUNNEL_PAIRS:
                    start_val = d(metrics.get(start_metric))
                    end_val = d(metrics.get(end_metric))
                    if start_val <= 0:
                        continue
                    rate = end_val / start_val
                    sev = severity_from_rate(rate, args.funnel_fail_rate, args.funnel_warn_rate, args.funnel_high_rate)
                    contribution_rows.append((args.profile_id, dt, "funnel", rate_metric_name, int((rate * 100).quantize(Decimal("1"))), str((Decimal("1") - min(rate, Decimal("1"))) * Decimal("100")), "high" if sev == "fail" else ("medium" if sev == "warn" else "low"), f"{group_name} rate={rate:.4f}", run_id))
                    if sev:
                        confidence = Decimal("0.92") if sev == "fail" else Decimal("0.72")
                        root_cause_candidates.append({"cause_type": cause_type, "cause_code": sev, "related_metric": end_metric, "confidence": confidence, "driver_source": "funnel", "observed_value": end_val, "baseline_value": start_val, "detail": f"{group_name} conversion abnormal: {end_val}/{start_val}={rate:.4f}"})

                if mapping_coverage < Decimal("0.95"):
                    code = "fail" if mapping_coverage < Decimal("0.80") else "warn"
                    confidence = Decimal("0.90") if code == "fail" else Decimal("0.70")
                    root_cause_candidates.append({"cause_type": "mapping_gap", "cause_code": code, "related_metric": "mapping_coverage", "confidence": confidence, "driver_source": "mapping", "observed_value": mapping_coverage, "baseline_value": Decimal("1.0"), "detail": f"mapping_coverage={mapping_coverage:.4f}"})
                    contribution_rows.append((args.profile_id, dt, "mapping", "mapping_coverage", int((mapping_coverage * 100).quantize(Decimal("1"))), str((Decimal("1") - mapping_coverage) * Decimal("100")), "high" if code == "fail" else "medium", f"mapping_coverage={mapping_coverage:.4f}", run_id))

                if fail_count > 0 or warn_count > 0:
                    contribution_rows.append((args.profile_id, dt, "validation", "validation_summary", fail_count + warn_count, str(Decimal(fail_count * 2 + warn_count) / Decimal(total_rules)), "high" if fail_count > 0 else "medium", f"fail={fail_count}, warn={warn_count}, total={total_rules}", run_id))

                seen_drift = set()
                for row in drift_rows:
                    metric_name = row["metric_name"]
                    if metric_name in seen_drift:
                        continue
                    seen_drift.add(metric_name)
                    drift_status = (row.get("drift_status") or "").lower()
                    sev = (row.get("severity") or "").lower()
                    drift_score = d(row.get("drift_score"))
                    observed = d(row.get("observed_value"))
                    baseline = d(row.get("baseline_value"))
                    if not (drift_status in ("warn", "alert") or sev in ("high", "medium")):
                        continue
                    contribution_rows.append((args.profile_id, dt, "drift", metric_name, 1, str(drift_score if drift_score > 0 else Decimal("1")), "high" if drift_status == "alert" or sev == "high" else "medium", f"drift_status={drift_status}, score={drift_score}", run_id))
                    scenario_bias = ""
                    if scenarios:
                        labels = ",".join([s[0] for s in scenarios[:3]])
                        scenario_bias = f"; scenarios={labels}"
                    if metric_name in ("page_view_count", "raw_event_count", "daily_active_users", "collector_event_count"):
                        cause_type = "traffic_mix_shift"
                        confidence = Decimal("0.78")
                    elif ("conversion" in metric_name or metric_name.endswith("_submit_count") or metric_name.endswith("_success_count") or metric_name in ("submit_capture_rate", "success_outcome_capture_rate")):
                        cause_type = "funnel_distortion"
                        confidence = Decimal("0.82")
                    else:
                        cause_type = "metric_drift"
                        confidence = Decimal("0.74")
                    if scenarios and cause_type == "traffic_mix_shift":
                        cause_type = "scenario_linked_drift"
                        confidence = Decimal("0.84")
                    root_cause_candidates.append({"cause_type": cause_type, "cause_code": drift_status if drift_status in ("warn", "alert") else "alert", "related_metric": metric_name, "confidence": confidence, "driver_source": "drift", "observed_value": observed, "baseline_value": baseline, "detail": f"drift metric={metric_name}, score={drift_score}{scenario_bias}"})

                for label, status in scenarios:
                    contribution_rows.append((args.profile_id, dt, "scenario", label, 1, "1.000000", "medium", f"scenario_status={status}", run_id))

                dedup = {}
                for item in root_cause_candidates:
                    key = (item["cause_type"], item["related_metric"], item["cause_code"])
                    if key not in dedup or d(item["confidence"]) > d(dedup[key]["confidence"]):
                        dedup[key] = item

                ordered = sorted(dedup.values(), key=lambda x: (d(x["confidence"]), x["cause_type"], x["related_metric"]), reverse=True)[:5]

                for rank, item in enumerate(ordered, start=1):
                    cur.execute("""
                        INSERT INTO data_risk_root_cause_day
                        (profile_id, dt, cause_rank, cause_type, cause_code, confidence,
                         driver_source, related_metric, observed_value, baseline_value, detail, run_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (args.profile_id, dt, rank, item["cause_type"], item["cause_code"], str(item["confidence"]), item["driver_source"], item["related_metric"], str(item["observed_value"]) if item["observed_value"] is not None else None, str(item["baseline_value"]) if item["baseline_value"] is not None else None, item["detail"], run_id))

                seen_links = set()
                for row in contribution_rows:
                    key = (row[2], row[3])
                    if key in seen_links:
                        continue
                    seen_links.add(key)
                    cur.execute("""
                        INSERT INTO risk_signal_link_day
                        (profile_id, dt, signal_group, signal_name, signal_count,
                         weighted_contribution, severity, note, run_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, row)

                print(f"[OK] root cause completed: profile_id={args.profile_id}, dt={dt}, causes={len(ordered)}, links={len(seen_links)}")
            conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
