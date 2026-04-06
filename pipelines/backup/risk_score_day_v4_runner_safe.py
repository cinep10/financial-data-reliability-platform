#!/usr/bin/env python3
import argparse
import os
from decimal import Decimal
import pymysql


def d(v):
    try:
        return Decimal(str(v if v is not None else 0))
    except Exception:
        return Decimal("0")


def clamp01(x):
    return max(Decimal("0"), min(Decimal("1"), x))


def avg(vals):
    vals = [d(v) for v in vals if v is not None]
    return sum(vals) / Decimal(str(len(vals))) if vals else Decimal("0")


def table_exists(cur, table_name: str) -> bool:
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


def get_columns(cur, table_name: str):
    if not table_exists(cur, table_name):
        return []
    cur.execute(f"DESC {table_name}")
    return [r["Field"] for r in cur.fetchall()]


def choose(columns, *names):
    for n in names:
        if n in columns:
            return n
    return None


def get_max_metric(cur, table_name: str, value_col: str | None, profile_id: str, dt: str) -> Decimal:
    if not value_col:
        return Decimal("0")
    cur.execute(
        f"SELECT COALESCE(MAX({value_col}), 0) AS s FROM {table_name} WHERE profile_id=%s AND dt=%s",
        (profile_id, dt),
    )
    row = cur.fetchone() or {}
    return d(row.get("s"))


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

with conn:
    with conn.cursor() as cur:
        time_cols = get_columns(cur, "metric_time_anomaly_day")
        corr_cols = get_columns(cur, "metric_correlation_anomaly_day")

        time_score_col = choose(time_cols, "anomaly_score", "score", "time_anomaly_score")
        corr_score_col = choose(corr_cols, "anomaly_score", "score", "correlation_score")

        if args.truncate:
            cur.execute(
                "DELETE FROM data_risk_score_day_v3 WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                (args.profile_id, args.dt_from, args.dt_to),
            )

        cur.execute(
            """
            SELECT DISTINCT dt
            FROM metric_value_day
            WHERE profile_id=%s AND dt BETWEEN %s AND %s
            ORDER BY dt
            """,
            (args.profile_id, args.dt_from, args.dt_to),
        )
        dates = [str(r["dt"]) for r in cur.fetchall()]

        for dt in dates:
            cur.execute(
                """
                SELECT total_rules, warn_count, fail_count
                FROM validation_summary_day
                WHERE profile_id=%s AND dt=%s
                ORDER BY validation_run_id DESC
                LIMIT 1
                """,
                (args.profile_id, dt),
            )
            vr = cur.fetchone() or {}
            total = max(int(vr.get("total_rules") or 0), 1)
            validation = clamp01(
                (d(vr.get("fail_count")) * Decimal("2") + d(vr.get("warn_count"))) / Decimal(str(total))
            )

            cur.execute(
                "SELECT mapping_coverage FROM mapping_coverage_day WHERE dt=%s LIMIT 1",
                (dt,),
            )
            mc = cur.fetchone() or {}
            mapping = clamp01(Decimal("1") - d(mc.get("mapping_coverage") if mc.get("mapping_coverage") is not None else 1))

            cur.execute(
                """
                SELECT metric_name, drift_score
                FROM metric_drift_result_r
                WHERE profile_id=%s AND dt=%s
                """,
                (args.profile_id, dt),
            )
            rows = cur.fetchall()

            drift, funnel, auth, anomaly = [], [], [], []
            for r in rows:
                s = abs(d(r.get("drift_score")))
                n = (r.get("metric_name") or "").lower()
                drift.append(s)

                if (
                    "conversion" in n
                    or n.endswith("_submit_count")
                    or n.endswith("_submit_rate")
                    or n.endswith("_start_count")
                    or n in ("submit_capture_rate", "success_outcome_capture_rate")
                ):
                    funnel.append(s)

                if n.startswith("auth_") or "login" in n or "otp" in n or n == "risk_login_count":
                    auth.append(s)

                if (
                    "conversion" in n
                    or n.endswith("_submit_count")
                    or n.endswith("_success_count")
                    or n.endswith("_fail_count")
                    or n in ("submit_capture_rate", "success_outcome_capture_rate")
                ):
                    anomaly.append(s)

            drift_s = clamp01(avg(drift))
            funnel_s = clamp01(avg(funnel))
            auth_s = clamp01(avg(auth))
            anomaly_s = clamp01(avg(anomaly))

            time_s = clamp01(get_max_metric(cur, "metric_time_anomaly_day", time_score_col, args.profile_id, dt))
            corr_s = clamp01(get_max_metric(cur, "metric_correlation_anomaly_day", corr_score_col, args.profile_id, dt))
            structural = clamp01(avg([time_s, corr_s]))

            cur.execute(
                """
                SELECT scenario_name
                FROM scenario_experiment_run
                WHERE profile_id=%s AND dt_from<=%s AND dt_to>=%s
                ORDER BY scenario_run_id DESC
                LIMIT 1
                """,
                (args.profile_id, dt, dt),
            )
            sr = cur.fetchone() or {}
            sn = sr.get("scenario_name")

            wv, wd, wa, wm, wf, wu, ws = (
                Decimal("0.20"),
                Decimal("0.18"),
                Decimal("0.12"),
                Decimal("0.10"),
                Decimal("0.18"),
                Decimal("0.12"),
                Decimal("0.10"),
            )
            boost = Decimal("0")

            if sn == "funnel_break":
                wf, wu, wd, boost = Decimal("0.32"), Decimal("0.08"), Decimal("0.16"), Decimal("0.08")
            elif sn == "auth_failure":
                wu, wf, wd, boost = Decimal("0.28"), Decimal("0.10"), Decimal("0.18"), Decimal("0.08")
            elif sn == "mixed_incident":
                wv, wd, wa, wm, wf, wu, ws, boost = (
                    Decimal("0.18"),
                    Decimal("0.16"),
                    Decimal("0.16"),
                    Decimal("0.10"),
                    Decimal("0.18"),
                    Decimal("0.12"),
                    Decimal("0.10"),
                    Decimal("0.12"),
                )

            final = clamp01(
                validation * wv
                + drift_s * wd
                + anomaly_s * wa
                + mapping * wm
                + funnel_s * wf
                + auth_s * wu
                + structural * ws
                + boost
            )
            grade = "high" if final >= Decimal("0.55") else ("medium" if final >= Decimal("0.30") else "low")

            cur.execute(
                """
                REPLACE INTO data_risk_score_day_v3
                (profile_id, dt, metric_nm, validation_score, drift_score, anomaly_score,
                 mapping_score, final_risk_score, risk_grade)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    args.profile_id,
                    dt,
                    "ALL_V4",
                    str(validation),
                    str(drift_s),
                    str(clamp01((anomaly_s + funnel_s + auth_s + structural) / Decimal("4"))),
                    str(mapping),
                    str(final),
                    grade,
                ),
            )

            print(
                f"[OK] risk v4 completed: profile_id={args.profile_id}, dt={dt}, "
                f"score={final:.4f}, grade={grade}, scenario={sn or 'baseline'}, "
                f"time_col={time_score_col or 'N/A'}, corr_col={corr_score_col or 'N/A'}"
            )

        conn.commit()
