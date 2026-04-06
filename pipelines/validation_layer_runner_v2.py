from __future__ import annotations

import argparse
from collections import defaultdict
from decimal import Decimal
from typing import Any, Dict, Iterable, Optional

import pymysql


def connect_mysql(host: str, port: int, user: str, password: str, db: str):
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


class ValidationRunner:
    """
    Validation runner for metric_value_hh / metric_value_day.

    Design goals:
    - work directly on MySQL
    - be tolerant of schema evolution in the metric layer
    - use direct counts when available
    - fall back to derived counts from rate * denominator when needed
    """

    def __init__(self, conn, profile_id: str, dt_from: str, dt_to: str, truncate: bool = False):
        self.conn = conn
        self.profile_id = profile_id
        self.dt_from = dt_from
        self.dt_to = dt_to
        self.truncate = truncate
        self.run_id: Optional[int] = None

    def _d(self, v: Any) -> Decimal:
        if v is None:
            return Decimal("0")
        return Decimal(str(v))

    def start_run(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO validation_run (profile_id, dt_from, dt_to, started_at, status, note)
                VALUES (%s, %s, %s, NOW(), 'running', 'validation layer runner v2')
                """,
                (self.profile_id, self.dt_from, self.dt_to),
            )
            self.run_id = cur.lastrowid
        self.conn.commit()

    def finish_run(self, status: str, note: str | None = None):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE validation_run
                SET finished_at = NOW(), status = %s, note = COALESCE(%s, note)
                WHERE validation_run_id = %s
                """,
                (status, note, self.run_id),
            )
        self.conn.commit()

    def truncate_targets(self):
        if not self.truncate:
            return
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM validation_result WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                (self.profile_id, self.dt_from, self.dt_to),
            )
            cur.execute(
                "DELETE FROM validation_summary_day WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                (self.profile_id, self.dt_from, self.dt_to),
            )
        self.conn.commit()

    def fetch_metric_hh(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    profile_id, dt, hh, metric_name, metric_group, source_layer,
                    metric_value, numerator_value, denominator_value, run_id, note
                FROM metric_value_hh
                WHERE profile_id=%s
                  AND dt BETWEEN %s AND %s
                ORDER BY dt, hh, metric_group, metric_name, source_layer
                """,
                (self.profile_id, self.dt_from, self.dt_to),
            )
            return cur.fetchall()

    def build_index(self, rows):
        idx: Dict[tuple, dict] = {}
        for r in rows:
            idx[(r["dt"], r["hh"], r["metric_name"], r["source_layer"])] = r
        return idx

    def get_metric(self, idx, dt, hh, metric_name, source_layers: Iterable[str] | None = None) -> Decimal:
        if source_layers:
            for layer in source_layers:
                rec = idx.get((dt, hh, metric_name, layer))
                if rec is not None:
                    return self._d(rec.get("metric_value"))
        else:
            for (k_dt, k_hh, k_metric, _), rec in idx.items():
                if k_dt == dt and k_hh == hh and k_metric == metric_name:
                    return self._d(rec.get("metric_value"))
        return Decimal("0")

    def get_record(self, idx, dt, hh, metric_name, source_layers: Iterable[str] | None = None):
        if source_layers:
            for layer in source_layers:
                rec = idx.get((dt, hh, metric_name, layer))
                if rec is not None:
                    return rec
        else:
            for (k_dt, k_hh, k_metric, _), rec in idx.items():
                if k_dt == dt and k_hh == hh and k_metric == metric_name:
                    return rec
        return None

    def get_rate_or_count(self, idx, dt, hh, count_metric: str, rate_metric: str, denominator_metric: str):
        """
        Prefer direct count metric.
        If not available, derive from rate * denominator.
        Returns (count_value, derivation_note).
        """
        count_val = self.get_metric(idx, dt, hh, count_metric)
        if count_val != 0:
            return count_val, f"direct:{count_metric}"

        count_rec = self.get_record(idx, dt, hh, count_metric)
        if count_rec is not None:
            return self._d(count_rec.get("metric_value")), f"direct:{count_metric}"

        rate_val = self.get_metric(idx, dt, hh, rate_metric)
        denom_val = self.get_metric(idx, dt, hh, denominator_metric)
        return rate_val * denom_val, f"derived:{rate_metric}*{denominator_metric}"

    def add_result(
        self,
        out,
        dt,
        hh,
        rule_name,
        rule_group,
        metric_name,
        layer_left,
        layer_right,
        observed,
        expected,
        status,
        severity,
        note,
    ):
        observed = self._d(observed)
        expected = self._d(expected)
        diff = observed - expected
        diff_ratio = None
        if expected != 0:
            diff_ratio = diff / expected
        out.append(
            (
                self.run_id,
                self.profile_id,
                dt,
                hh,
                rule_name,
                rule_group,
                metric_name,
                layer_left,
                layer_right,
                observed,
                expected,
                diff,
                diff_ratio,
                status,
                severity,
                note,
            )
        )

    def run_validation(self):
        rows = self.fetch_metric_hh()
        idx = self.build_index(rows)
        results = []

        dt_hhs = sorted(set((r["dt"], r["hh"]) for r in rows))

        for dt, hh in dt_hhs:
            raw_count = self.get_metric(idx, dt, hh, "raw_event_count", ["raw", "staging", "source"])
            collector_count = self.get_metric(idx, dt, hh, "collector_event_count", ["collector", "metric", "derived"])
            page_view_count = self.get_metric(idx, dt, hh, "page_view_count", ["collector", "metric", "derived"])

            auth_attempt = self.get_metric(idx, dt, hh, "auth_attempt_count", ["collector", "metric", "derived"])
            auth_success_rate = self.get_metric(idx, dt, hh, "auth_success_rate", ["collector", "metric", "derived"])
            auth_fail_rate = self.get_metric(idx, dt, hh, "auth_fail_rate", ["collector", "metric", "derived"])
            auth_success_cnt, auth_success_note = self.get_rate_or_count(
                idx, dt, hh, "auth_success_count", "auth_success_rate", "auth_attempt_count"
            )
            auth_fail_cnt, auth_fail_note = self.get_rate_or_count(
                idx, dt, hh, "auth_fail_count", "auth_fail_rate", "auth_attempt_count"
            )

            otp_request = self.get_metric(idx, dt, hh, "otp_request_count", ["collector", "metric", "derived"])
            risk_login_count = self.get_metric(idx, dt, hh, "risk_login_count", ["collector", "metric", "derived"])

            loan_view = self.get_metric(idx, dt, hh, "loan_view_count", ["collector", "metric", "derived"])
            loan_start = self.get_metric(idx, dt, hh, "loan_apply_start_count", ["collector", "metric", "derived"])
            loan_submit = self.get_metric(idx, dt, hh, "loan_apply_submit_count", ["collector", "metric", "derived"])

            card_start = self.get_metric(idx, dt, hh, "card_apply_start_count", ["collector", "metric", "derived"])
            card_submit_rate = self.get_metric(idx, dt, hh, "card_apply_submit_rate", ["collector", "metric", "derived"])
            card_submit_cnt, card_submit_note = self.get_rate_or_count(
                idx, dt, hh, "card_apply_submit_count", "card_apply_submit_rate", "card_apply_start_count"
            )

            missing_rate = self.get_metric(idx, dt, hh, "estimated_missing_rate", ["control", "collector", "metric", "derived"])
            dau = self.get_metric(idx, dt, hh, "daily_active_users", ["collector", "metric", "derived"])
            login_success_count = self.get_metric(idx, dt, hh, "login_success_count", ["collector", "metric", "derived"])

            # cross-system validation
            if raw_count != 0 or collector_count != 0:
                self.add_result(
                    results,
                    dt,
                    hh,
                    "raw_ge_collector",
                    "cross_system",
                    "collector_event_count",
                    "raw",
                    "collector",
                    collector_count,
                    raw_count,
                    "pass" if raw_count >= collector_count else "fail",
                    "high" if raw_count < collector_count else "info",
                    "raw_event_count >= collector_event_count",
                )

            if collector_count != 0 or page_view_count != 0:
                self.add_result(
                    results,
                    dt,
                    hh,
                    "collector_ge_metric_pv",
                    "cross_system",
                    "page_view_count",
                    "collector",
                    "metric",
                    page_view_count,
                    collector_count,
                    "pass" if collector_count >= page_view_count else "fail",
                    "high" if collector_count < page_view_count else "info",
                    "collector_event_count >= page_view_count",
                )

            # funnel validation
            self.add_result(
                results,
                dt,
                hh,
                "auth_success_le_attempt",
                "funnel",
                "auth_success_count",
                "collector",
                "collector",
                auth_success_cnt,
                auth_attempt,
                "pass" if auth_success_cnt <= auth_attempt else "fail",
                "high" if auth_success_cnt > auth_attempt else "info",
                f"auth_success_count <= auth_attempt_count ({auth_success_note})",
            )

            self.add_result(
                results,
                dt,
                hh,
                "auth_fail_le_attempt",
                "funnel",
                "auth_fail_count",
                "collector",
                "collector",
                auth_fail_cnt,
                auth_attempt,
                "pass" if auth_fail_cnt <= auth_attempt else "fail",
                "high" if auth_fail_cnt > auth_attempt else "info",
                f"auth_fail_count <= auth_attempt_count ({auth_fail_note})",
            )

            self.add_result(
                results,
                dt,
                hh,
                "loan_start_le_view",
                "funnel",
                "loan_apply_start_count",
                "collector",
                "collector",
                loan_start,
                loan_view,
                "pass" if loan_start <= loan_view else "fail",
                "high" if loan_start > loan_view else "info",
                "loan_apply_start_count <= loan_view_count",
            )

            self.add_result(
                results,
                dt,
                hh,
                "loan_submit_le_start",
                "funnel",
                "loan_apply_submit_count",
                "collector",
                "collector",
                loan_submit,
                loan_start,
                "pass" if loan_submit <= loan_start else "fail",
                "high" if loan_submit > loan_start else "info",
                "loan_apply_submit_count <= loan_apply_start_count",
            )

            self.add_result(
                results,
                dt,
                hh,
                "card_submit_le_start",
                "funnel",
                "card_apply_submit_count",
                "collector",
                "collector",
                card_submit_cnt,
                card_start,
                "pass" if card_submit_cnt <= card_start else "fail",
                "high" if card_submit_cnt > card_start else "info",
                f"card_apply_submit_count <= card_apply_start_count ({card_submit_note})",
            )

            if login_success_count != 0 or auth_attempt != 0:
                self.add_result(
                    results,
                    dt,
                    hh,
                    "auth_attempt_le_login_success",
                    "funnel",
                    "auth_attempt_count",
                    "collector",
                    "collector",
                    auth_attempt,
                    login_success_count,
                    "pass" if auth_attempt <= login_success_count else "warn",
                    "medium" if auth_attempt > login_success_count else "info",
                    "auth_attempt_count <= login_success_count",
                )

            # ratio bounds
            for rule_name, metric_name, value, layer in [
                ("auth_success_rate_bounds", "auth_success_rate", auth_success_rate, "collector"),
                ("auth_fail_rate_bounds", "auth_fail_rate", auth_fail_rate, "collector"),
                ("card_submit_rate_bounds", "card_apply_submit_rate", card_submit_rate, "collector"),
                ("missing_rate_bounds", "estimated_missing_rate", missing_rate, "control"),
            ]:
                # if metric absent entirely, skip noisy fail
                rec = self.get_record(idx, dt, hh, metric_name)
                if rec is None and value == 0:
                    continue
                ok = Decimal("0") <= self._d(value) <= Decimal("1")
                self.add_result(
                    results,
                    dt,
                    hh,
                    rule_name,
                    "ratio",
                    metric_name,
                    layer,
                    None,
                    value,
                    1,
                    "pass" if ok else "fail",
                    "medium" if not ok else "info",
                    "ratio must be within [0,1]",
                )

            # mapping quality / integrity
            suspicious_auth = auth_attempt > 0 and auth_success_cnt == 0 and auth_fail_cnt == 0
            self.add_result(
                results,
                dt,
                hh,
                "auth_attempt_needs_outcome",
                "mapping_quality",
                "auth_attempt_count",
                "collector",
                "collector",
                auth_attempt,
                auth_success_cnt + auth_fail_cnt,
                "warn" if suspicious_auth else "pass",
                "medium" if suspicious_auth else "info",
                "auth_attempt exists but success/fail both zero",
            )

            suspicious_loan = loan_start > 0 and loan_submit == 0
            self.add_result(
                results,
                dt,
                hh,
                "loan_start_needs_submit",
                "mapping_quality",
                "loan_apply_start_count",
                "collector",
                "collector",
                loan_start,
                loan_submit,
                "warn" if suspicious_loan else "pass",
                "medium" if suspicious_loan else "info",
                "loan_apply_start exists but submit zero",
            )

            suspicious_card = card_start > 0 and card_submit_cnt == 0
            self.add_result(
                results,
                dt,
                hh,
                "card_start_needs_submit",
                "mapping_quality",
                "card_apply_start_count",
                "collector",
                "collector",
                card_start,
                card_submit_cnt,
                "warn" if suspicious_card else "pass",
                "medium" if suspicious_card else "info",
                "card_apply_start exists but submit zero",
            )

            suspicious_otp = otp_request > 0 and auth_attempt == 0
            self.add_result(
                results,
                dt,
                hh,
                "otp_needs_auth_attempt",
                "mapping_quality",
                "otp_request_count",
                "collector",
                "collector",
                otp_request,
                auth_attempt,
                "warn" if suspicious_otp else "pass",
                "medium" if suspicious_otp else "info",
                "otp_request exists but auth_attempt zero",
            )

            if risk_login_count != 0 and auth_attempt != 0:
                self.add_result(
                    results,
                    dt,
                    hh,
                    "risk_login_le_auth_attempt",
                    "mapping_quality",
                    "risk_login_count",
                    "collector",
                    "collector",
                    risk_login_count,
                    auth_attempt,
                    "pass" if risk_login_count <= auth_attempt else "warn",
                    "medium" if risk_login_count > auth_attempt else "info",
                    "risk_login_count should not exceed auth_attempt_count",
                )

            if dau != 0 and login_success_count != 0:
                self.add_result(
                    results,
                    dt,
                    hh,
                    "login_success_reasonable_vs_dau",
                    "sanity",
                    "login_success_count",
                    "collector",
                    "collector",
                    login_success_count,
                    dau,
                    "pass" if login_success_count <= (dau * Decimal("5")) else "warn",
                    "low" if login_success_count > (dau * Decimal("5")) else "info",
                    "soft sanity check: login_success_count unusually high vs DAU",
                )

        # completeness per day per metric
        metric_names = defaultdict(set)
        for r in rows:
            metric_names[(r["dt"], r["metric_name"], r["source_layer"])].add(r["hh"])

        for (dt, metric_name, source_layer), hh_set in sorted(metric_names.items()):
            count_hh = len(hh_set)
            self.add_result(
                results,
                dt,
                None,
                "hh_metric_completeness",
                "completeness",
                metric_name,
                source_layer,
                None,
                count_hh,
                24,
                "pass" if count_hh == 24 else "warn",
                "medium" if count_hh != 24 else "info",
                "hourly metric completeness check",
            )

        return results

    def persist_results(self, results):
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO validation_result
                (validation_run_id, profile_id, dt, hh, rule_name, rule_group, metric_name,
                 layer_left, layer_right, observed_value, expected_value, diff_value, diff_ratio,
                 validation_status, severity, note)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                results,
            )

            cur.execute(
                """
                SELECT
                  dt,
                  COUNT(*) AS total_rules,
                  SUM(CASE WHEN validation_status='pass' THEN 1 ELSE 0 END) AS pass_count,
                  SUM(CASE WHEN validation_status='warn' THEN 1 ELSE 0 END) AS warn_count,
                  SUM(CASE WHEN validation_status='fail' THEN 1 ELSE 0 END) AS fail_count,
                  MAX(CASE severity
                        WHEN 'high' THEN 4
                        WHEN 'medium' THEN 3
                        WHEN 'low' THEN 2
                        WHEN 'info' THEN 1
                        ELSE 0 END) AS sev_rank
                FROM validation_result
                WHERE validation_run_id=%s
                GROUP BY dt
                """,
                (self.run_id,),
            )
            summary = cur.fetchall()

            sev_map = {4: "high", 3: "medium", 2: "low", 1: "info", 0: None}
            cur.executemany(
                """
                INSERT INTO validation_summary_day
                (profile_id, dt, validation_run_id, total_rules, pass_count, warn_count, fail_count, highest_severity, note)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  total_rules=VALUES(total_rules),
                  pass_count=VALUES(pass_count),
                  warn_count=VALUES(warn_count),
                  fail_count=VALUES(fail_count),
                  highest_severity=VALUES(highest_severity),
                  note=VALUES(note)
                """,
                [
                    (
                        self.profile_id,
                        r["dt"],
                        self.run_id,
                        r["total_rules"],
                        r["pass_count"],
                        r["warn_count"],
                        r["fail_count"],
                        sev_map.get(r["sev_rank"]),
                        "validation summary by day",
                    )
                    for r in summary
                ],
            )
        self.conn.commit()

    def run(self):
        self.start_run()
        try:
            self.truncate_targets()
            results = self.run_validation()
            self.persist_results(results)
            self.finish_run("success", f"validation results={len(results)}")
            print(f"[validation] profile={self.profile_id} results={len(results)} run_id={self.run_id}")
        except Exception as e:
            self.conn.rollback()
            self.finish_run("failed", str(e)[:255])
            raise


def main():
    ap = argparse.ArgumentParser(description="Validation Layer runner v2")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    conn = connect_mysql(args.host, args.port, args.user, args.password, args.db)
    try:
        ValidationRunner(conn, args.profile_id, args.dt_from, args.dt_to, args.truncate).run()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
