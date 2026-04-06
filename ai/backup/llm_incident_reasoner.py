#!/usr/bin/env python3
import argparse
import json
import os
from datetime import datetime, date
from decimal import Decimal

import pymysql

try:
    from openai import OpenAI
except Exception:
    OpenAI = None


PROMPT_VERSION = "ai_incident_reasoner_v2"


def db_conn(args):
    return pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
    )


def json_safe(obj):
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [json_safe(v) for v in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj


def fetch_rows(cur, sql, params):
    cur.execute(sql, params)
    return cur.fetchall()


def fetch_context(cur, profile_id, dt):
    cur.execute(
        """
        SELECT
            profile_id,
            dt,
            MAX(final_risk_score) AS risk_score,
            MAX(risk_grade) AS risk_grade
        FROM data_risk_score_day_v3
        WHERE profile_id=%s AND dt=%s
        GROUP BY profile_id, dt
        """,
        (profile_id, dt),
    )
    risk = cur.fetchone() or {}

    cur.execute(
        """
        SELECT
            predicted_risk_status,
            prob_alert,
            prob_warning,
            prob_normal,
            actual_risk_status,
            actual_risk_score,
            model_name,
            model_version
        FROM ml_prediction_result
        WHERE profile_id=%s AND dt=%s
        ORDER BY model_version DESC
        LIMIT 1
        """,
        (profile_id, dt),
    )
    pred = cur.fetchone() or {}

    root_rows = []
    try:
        root_rows = fetch_rows(
            cur,
            """
            SELECT
                cause_rank,
                cause_type,
                cause_code,
                confidence,
                related_metric
            FROM data_risk_root_cause_day
            WHERE profile_id=%s AND dt=%s
            ORDER BY cause_rank
            LIMIT 5
            """,
            (profile_id, dt),
        )
    except Exception:
        pass

    if not root_rows:
        try:
            root_rows = fetch_rows(
                cur,
                """
                SELECT
                    1 AS cause_rank,
                    COALESCE(cause_type, root_cause_type) AS cause_type,
                    COALESCE(cause_code, severity) AS cause_code,
                    COALESCE(confidence, contribution_score, 0.5) AS confidence,
                    COALESCE(related_metric, metric_name, 'ALL') AS related_metric
                FROM root_cause_result
                WHERE profile_id=%s AND dt=%s
                LIMIT 5
                """,
                (profile_id, dt),
            )
        except Exception:
            pass

    drift_rows = fetch_rows(
        cur,
        """
        SELECT
            metric_name,
            drift_status,
            drift_score
        FROM metric_drift_result_r
        WHERE profile_id=%s
          AND dt=%s
          AND drift_status IN ('alert','warn')
        ORDER BY drift_score DESC
        LIMIT 8
        """,
        (profile_id, dt),
    )

    time_rows = []
    try:
        time_rows = fetch_rows(
            cur,
            """
            SELECT
                metric_name,
                anomaly_status,
                zscore_7d
            FROM metric_time_anomaly_day
            WHERE profile_id=%s
              AND dt=%s
              AND anomaly_status IN ('alert','warn')
            ORDER BY ABS(zscore_7d) DESC
            LIMIT 5
            """,
            (profile_id, dt),
        )
    except Exception:
        pass

    corr_rows = []
    try:
        corr_rows = fetch_rows(
            cur,
            """
            SELECT
                pair_name,
                anomaly_status,
                ratio_diff_pct
            FROM metric_correlation_anomaly_day
            WHERE profile_id=%s
              AND dt=%s
              AND anomaly_status IN ('alert','warn')
            ORDER BY ABS(ratio_diff_pct) DESC
            LIMIT 5
            """,
            (profile_id, dt),
        )
    except Exception:
        pass

    action_rows = []
    try:
        action_rows = fetch_rows(
            cur,
            """
            SELECT
                metric_nm,
                root_cause,
                action_type,
                priority,
                recommended_fix
            FROM data_reliability_action_day
            WHERE profile_id=%s
              AND dt=%s
            ORDER BY priority DESC
            LIMIT 8
            """,
            (profile_id, dt),
        )
    except Exception:
        pass

    scenario_rows = []
    try:
        scenario_rows = fetch_rows(
            cur,
            """
            SELECT
                scenario_name,
                risk_score_v3,
                predicted_alert_prob,
                predicted_label,
                root_cause_top1
            FROM scenario_experiment_result_day
            WHERE profile_id=%s
              AND dt=%s
            ORDER BY scenario_run_id DESC
            LIMIT 3
            """,
            (profile_id, dt),
        )
    except Exception:
        pass

    return {
        "risk": risk,
        "prediction": pred,
        "root_causes": root_rows,
        "drift": drift_rows,
        "time_anomaly": time_rows,
        "correlation_anomaly": corr_rows,
        "actions": action_rows,
        "scenario_result": scenario_rows,
    }


def build_prompt(profile_id, dt, context):
    safe_context = json_safe(context)
    return f"""
You are an expert data reliability incident analyst.

Task:
Summarize the incident for one service-day in strict JSON.

Service profile_id: {profile_id}
Date: {dt}

Input context:
{json.dumps(safe_context, ensure_ascii=False, indent=2)}

Return JSON with keys:
incident_title,
incident_level,
executive_summary,
technical_summary,
business_impact,
recommended_actions,
confidence_score

Rules:
- incident_level must be one of: normal, warning, alert
- recommended_actions must be an array of 3 to 5 short strings
- confidence_score must be a number from 0 to 1
- Use root cause and drift evidence directly
- Be concise and operational
""".strip()


def llm_call(prompt_text):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required")

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

    if OpenAI is None:
        raise RuntimeError("openai package is not installed. Run: pip install openai")

    client = OpenAI(api_key=api_key)
    resp = client.responses.create(
        model=model,
        input=prompt_text,
        temperature=0.2,
    )
    text = getattr(resp, "output_text", None) or str(resp)
    return model, text


def parse_json(text, fallback_level="warning"):
    try:
        data = json.loads(text)
        return {
            "incident_title": data.get("incident_title", "Data reliability incident"),
            "incident_level": data.get("incident_level", fallback_level),
            "executive_summary": data.get("executive_summary", ""),
            "technical_summary": data.get("technical_summary", ""),
            "business_impact": data.get("business_impact", ""),
            "recommended_actions": data.get("recommended_actions", []),
            "confidence_score": float(data.get("confidence_score", 0.7)),
        }
    except Exception:
        return {
            "incident_title": "Data reliability incident",
            "incident_level": fallback_level,
            "executive_summary": text[:500],
            "technical_summary": text[:1000],
            "business_impact": "",
            "recommended_actions": [],
            "confidence_score": 0.5,
        }


def upsert_summary(cur, profile_id, dt, run_id, model, context, parsed, prompt_text, response_text):
    risk_score = context.get("risk", {}).get("risk_score")
    pred = context.get("prediction", {})

    cur.execute(
        """
        REPLACE INTO ai_incident_summary_day
        (
            profile_id,
            dt,
            run_id,
            risk_score,
            actual_risk_status,
            predicted_risk_status,
            predicted_alert_prob,
            incident_title,
            incident_level,
            executive_summary,
            technical_summary,
            business_impact,
            recommended_actions,
            confidence_score,
            llm_model,
            prompt_version
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            profile_id,
            dt,
            run_id,
            risk_score,
            pred.get("actual_risk_status"),
            pred.get("predicted_risk_status"),
            pred.get("prob_alert"),
            parsed["incident_title"],
            parsed["incident_level"],
            parsed["executive_summary"],
            parsed["technical_summary"],
            parsed["business_impact"],
            json.dumps(parsed["recommended_actions"], ensure_ascii=False),
            parsed["confidence_score"],
            model,
            PROMPT_VERSION,
        ),
    )

    cur.execute(
        """
        REPLACE INTO ai_prompt_log
        (
            run_id,
            profile_id,
            dt,
            prompt_version,
            llm_model,
            prompt_text,
            response_text
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        (run_id, profile_id, dt, PROMPT_VERSION, model, prompt_text, response_text),
    )


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
    args = ap.parse_args()

    conn = db_conn(args)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT dt
                FROM ml_prediction_result
                WHERE profile_id=%s
                  AND dt BETWEEN %s AND %s
                ORDER BY dt
                """,
                (args.profile_id, args.dt_from, args.dt_to),
            )
            dates = [str(r["dt"]) for r in cur.fetchall()]

            for dt in dates:
                context = fetch_context(cur, args.profile_id, dt)
                prompt_text = build_prompt(args.profile_id, dt, context)
                model, response_text = llm_call(prompt_text)

                fallback_level = context.get("prediction", {}).get("predicted_risk_status", "warning")
                parsed = parse_json(response_text, fallback_level=fallback_level)

                run_id = f"ai_summary_{args.profile_id}_{dt.replace('-', '')}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

                upsert_summary(
                    cur=cur,
                    profile_id=args.profile_id,
                    dt=dt,
                    run_id=run_id,
                    model=model,
                    context=context,
                    parsed=parsed,
                    prompt_text=prompt_text,
                    response_text=response_text,
                )

                print(
                    f"[OK] ai incident summary completed: "
                    f"profile_id={args.profile_id}, dt={dt}, level={parsed['incident_level']}"
                )

        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
