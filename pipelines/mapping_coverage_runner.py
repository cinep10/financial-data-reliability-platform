from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterable

import pymysql


def daterange(start: str, end: str) -> Iterable[str]:
    cur = datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end, "%Y-%m-%d").date()
    while cur <= end_dt:
        yield cur.isoformat()
        cur += timedelta(days=1)


def connect_mysql(args):
    return pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_metric_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS metric_value_hh (
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          hh TINYINT NOT NULL,
          metric_name VARCHAR(100) NOT NULL,
          metric_group VARCHAR(50) NOT NULL,
          source_layer VARCHAR(50) NOT NULL,
          metric_value DECIMAL(18,6) NOT NULL,
          numerator_value DECIMAL(18,6) NULL,
          denominator_value DECIMAL(18,6) NULL,
          run_id VARCHAR(64) NULL,
          note VARCHAR(255) NULL,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (profile_id, dt, hh, metric_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS metric_value_day (
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          metric_name VARCHAR(100) NOT NULL,
          metric_group VARCHAR(50) NOT NULL,
          source_layer VARCHAR(50) NOT NULL,
          metric_value DECIMAL(18,6) NOT NULL,
          numerator_value DECIMAL(18,6) NULL,
          denominator_value DECIMAL(18,6) NULL,
          run_id VARCHAR(64) NULL,
          note VARCHAR(255) NULL,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (profile_id, dt, metric_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def q(v: float) -> str:
    return str(Decimal(str(v)).quantize(Decimal("0.000001")))


def safe_ratio(num: float, den: float) -> float:
    if den == 0:
        return 0.0
    return num / den


def upsert_metric_hh(cur, row):
    cur.execute(
        """
        INSERT INTO metric_value_hh
        (profile_id, dt, hh, metric_name, metric_group, source_layer,
         metric_value, numerator_value, denominator_value, run_id, note)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          metric_group=VALUES(metric_group),
          source_layer=VALUES(source_layer),
          metric_value=VALUES(metric_value),
          numerator_value=VALUES(numerator_value),
          denominator_value=VALUES(denominator_value),
          run_id=VALUES(run_id),
          note=VALUES(note)
        """,
        row,
    )


def upsert_metric_day(cur, row):
    cur.execute(
        """
        INSERT INTO metric_value_day
        (profile_id, dt, metric_name, metric_group, source_layer,
         metric_value, numerator_value, denominator_value, run_id, note)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          metric_group=VALUES(metric_group),
          source_layer=VALUES(source_layer),
          metric_value=VALUES(metric_value),
          numerator_value=VALUES(numerator_value),
          denominator_value=VALUES(denominator_value),
          run_id=VALUES(run_id),
          note=VALUES(note)
        """,
        row,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Mapping coverage runner")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--date")
    ap.add_argument("--dt-from")
    ap.add_argument("--dt-to")
    args = ap.parse_args()

    if args.date:
        dates = [args.date]
    else:
        if not args.dt_from or not args.dt_to:
            raise SystemExit("Provide --date or --dt-from/--dt-to")
        dates = list(daterange(args.dt_from, args.dt_to))

    run_id = f"mappingcov_{args.profile_id}_{dates[0].replace('-', '')}_{dates[-1].replace('-', '')}"

    conn = connect_mysql(args)
    try:
        with conn.cursor() as cur:
            ensure_metric_tables(cur)

            for dt in dates:
                cur.execute(
                    """
                    SELECT hh, metric_name, metric_value
                    FROM metric_value_hh
                    WHERE profile_id=%s
                      AND dt=%s
                      AND metric_name IN (
                        'auth_attempt_count','auth_success_count','auth_fail_count',
                        'loan_apply_start_count','loan_apply_submit_count',
                        'card_apply_start_count','card_apply_submit_count'
                      )
                    ORDER BY hh
                    """,
                    (args.profile_id, dt),
                )
                rows = cur.fetchall()

                hh_map = {}
                for r in rows:
                    hh_map.setdefault(r["hh"], {})[r["metric_name"]] = float(r["metric_value"] or 0)

                for hh, vals in hh_map.items():
                    auth_attempt = vals.get("auth_attempt_count", 0.0)
                    auth_success = vals.get("auth_success_count", 0.0)
                    auth_fail = vals.get("auth_fail_count", 0.0)
                    loan_start = vals.get("loan_apply_start_count", 0.0)
                    loan_submit = vals.get("loan_apply_submit_count", 0.0)
                    card_start = vals.get("card_apply_start_count", 0.0)
                    card_submit = vals.get("card_apply_submit_count", 0.0)

                    metrics = [
                        (
                            "mapping_coverage_auth",
                            safe_ratio(auth_success + auth_fail, auth_attempt),
                            auth_success + auth_fail,
                            auth_attempt,
                            "auth coverage = (success + fail) / attempt",
                        ),
                        (
                            "success_outcome_capture_rate",
                            safe_ratio(auth_success, auth_attempt),
                            auth_success,
                            auth_attempt,
                            "auth success capture = success / attempt",
                        ),
                        (
                            "mapping_coverage_loan",
                            safe_ratio(loan_submit, loan_start),
                            loan_submit,
                            loan_start,
                            "loan submit coverage = submit / start",
                        ),
                        (
                            "mapping_coverage_card",
                            safe_ratio(card_submit, card_start),
                            card_submit,
                            card_start,
                            "card submit coverage = submit / start",
                        ),
                        (
                            "submit_capture_rate",
                            safe_ratio(loan_submit + card_submit, loan_start + card_start),
                            loan_submit + card_submit,
                            loan_start + card_start,
                            "overall submit capture = (loan_submit + card_submit) / (loan_start + card_start)",
                        ),
                    ]

                    for metric_name, value, num, den, note in metrics:
                        upsert_metric_hh(
                            cur,
                            (
                                args.profile_id,
                                dt,
                                hh,
                                metric_name,
                                "mapping_quality",
                                "control",
                                q(value),
                                q(num),
                                q(den),
                                run_id,
                                note,
                            ),
                        )

                cur.execute(
                    """
                    SELECT metric_name, metric_value
                    FROM metric_value_day
                    WHERE profile_id=%s
                      AND dt=%s
                      AND metric_name IN (
                        'auth_attempt_count','auth_success_count','auth_fail_count',
                        'loan_apply_start_count','loan_apply_submit_count',
                        'card_apply_start_count','card_apply_submit_count'
                      )
                    """,
                    (args.profile_id, dt),
                )
                drows = cur.fetchall()
                day_vals = {r["metric_name"]: float(r["metric_value"] or 0) for r in drows}

                auth_attempt = day_vals.get("auth_attempt_count", 0.0)
                auth_success = day_vals.get("auth_success_count", 0.0)
                auth_fail = day_vals.get("auth_fail_count", 0.0)
                loan_start = day_vals.get("loan_apply_start_count", 0.0)
                loan_submit = day_vals.get("loan_apply_submit_count", 0.0)
                card_start = day_vals.get("card_apply_start_count", 0.0)
                card_submit = day_vals.get("card_apply_submit_count", 0.0)

                day_metrics = [
                    (
                        "mapping_coverage_auth",
                        safe_ratio(auth_success + auth_fail, auth_attempt),
                        auth_success + auth_fail,
                        auth_attempt,
                        "auth coverage = (success + fail) / attempt",
                    ),
                    (
                        "success_outcome_capture_rate",
                        safe_ratio(auth_success, auth_attempt),
                        auth_success,
                        auth_attempt,
                        "auth success capture = success / attempt",
                    ),
                    (
                        "mapping_coverage_loan",
                        safe_ratio(loan_submit, loan_start),
                        loan_submit,
                        loan_start,
                        "loan submit coverage = submit / start",
                    ),
                    (
                        "mapping_coverage_card",
                        safe_ratio(card_submit, card_start),
                        card_submit,
                        card_start,
                        "card submit coverage = submit / start",
                    ),
                    (
                        "submit_capture_rate",
                        safe_ratio(loan_submit + card_submit, loan_start + card_start),
                        loan_submit + card_submit,
                        loan_start + card_start,
                        "overall submit capture = (loan_submit + card_submit) / (loan_start + card_start)",
                    ),
                ]

                for metric_name, value, num, den, note in day_metrics:
                    upsert_metric_day(
                        cur,
                        (
                            args.profile_id,
                            dt,
                            metric_name,
                            "mapping_quality",
                            "control",
                            q(value),
                            q(num),
                            q(den),
                            run_id,
                            note,
                        ),
                    )

        conn.commit()
        print(f"[OK] mapping coverage completed: run_id={run_id}, dates={len(dates)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
