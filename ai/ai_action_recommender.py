#!/usr/bin/env python3
import argparse
import json
import os
from datetime import date, datetime
from decimal import Decimal

import pymysql

try:
    from openai import OpenAI
except Exception:
    OpenAI = None


PROMPT_VERSION = "ai_action_recommender_v3"


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


def table_columns(cur, table_name):
    cur.execute(
        """
        SELECT COLUMN_NAME
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
        ORDER BY ORDINAL_POSITION
        """,
        (table_name,),
    )
    return [r["COLUMN_NAME"] for r in cur.fetchall()]


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
    row = cur.fetchone() or {}
    return int(row.get("cnt", 0)) > 0


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


def fetch_summary(cur, profile_id, dt):
    cur.execute(
        """
        SELECT *
        FROM ai_incident_summary_day
        WHERE profile_id=%s AND dt=%s
        """,
        (profile_id, dt),
    )
    return cur.fetchone() or {}


def fetch_actions(cur, profile_id, dt):
    if not table_exists(cur, "data_reliability_action_day"):
        return []

    cols = set(table_columns(cur, "data_reliability_action_day"))

    select_parts = ["dt"]
    select_parts.append("profile_id" if "profile_id" in cols else "NULL AS profile_id")

    if "metric_nm" in cols:
        select_parts.append("metric_nm")
    elif "metric_name" in cols:
        select_parts.append("metric_name AS metric_nm")
    else:
        select_parts.append("NULL AS metric_nm")

    if "root_cause" in cols:
        select_parts.append("root_cause")
    elif "cause_type" in cols:
        select_parts.append("cause_type AS root_cause")
    else:
        select_parts.append("NULL AS root_cause")

    select_parts.append("action_type" if "action_type" in cols else "NULL AS action_type")
    select_parts.append("priority" if "priority" in cols else "NULL AS priority")

    if "recommended_fix" in cols:
        select_parts.append("recommended_fix")
    elif "action_detail" in cols:
        select_parts.append("action_detail AS recommended_fix")
    else:
        select_parts.append("NULL AS recommended_fix")

    sql = f"SELECT {', '.join(select_parts)} FROM data_reliability_action_day WHERE dt=%s"
    params = [dt]

    if "profile_id" in cols:
        sql += " AND profile_id=%s"
        params.append(profile_id)

    if "priority" in cols:
        sql += """
        ORDER BY CASE priority
            WHEN 'high' THEN 3
            WHEN 'medium' THEN 2
            WHEN 'low' THEN 1
            ELSE 0
        END DESC
        """
    else:
        sql += " ORDER BY dt DESC"

    sql += " LIMIT 10"
    cur.execute(sql, params)
    return cur.fetchall()


def build_prompt(profile_id, dt, summary, actions):
    safe_summary = json_safe(summary)
    safe_actions = json_safe(actions)
    return f"""
You are an SRE and data reliability action planner.

Service profile_id: {profile_id}
Date: {dt}

Incident summary:
{json.dumps(safe_summary, ensure_ascii=False, indent=2)}

Existing action candidates:
{json.dumps(safe_actions, ensure_ascii=False, indent=2)}

Return strict JSON array with 3 to 5 objects.
Each object must include:
action_rank,
action_type,
action_title,
action_detail,
owner_hint,
priority,
evidence
""".strip()


def parse_actions(text):
    try:
        arr = json.loads(text)
        if isinstance(arr, list):
            return arr
    except Exception:
        pass

    return [{
        "action_rank": 1,
        "action_type": "manual_review",
        "action_title": "Review incident manually",
        "action_detail": text[:500],
        "owner_hint": "data-platform",
        "priority": "medium",
        "evidence": "fallback parse",
    }]


def fallback_actions(summary, actions):
    items = []
    rank = 1

    for a in actions[:3]:
        metric_nm = a.get("metric_nm") or "key metric"
        root_cause = a.get("root_cause") or "root cause candidate"
        recommended_fix = a.get("recommended_fix") or "Review related pipeline, metric, and recent changes"

        items.append({
            "action_rank": rank,
            "action_type": a.get("action_type") or "manual_review",
            "action_title": f"Check {metric_nm}",
            "action_detail": recommended_fix,
            "owner_hint": "data-platform",
            "priority": a.get("priority") or "medium",
            "evidence": root_cause,
        })
        rank += 1

    if not items:
        items = [
            {
                "action_rank": 1,
                "action_type": "manual_review",
                "action_title": "Review incident context",
                "action_detail": summary.get("technical_summary") or "Review technical summary and root cause evidence",
                "owner_hint": "data-platform",
                "priority": "medium",
                "evidence": "ai incident summary",
            },
            {
                "action_rank": 2,
                "action_type": "pipeline_check",
                "action_title": "Validate upstream ingestion and mapping",
                "action_detail": "Check collection, parsing, mapping coverage, schema changes, and recent deployment changes",
                "owner_hint": "data-platform",
                "priority": "medium",
                "evidence": "default fallback action",
            },
            {
                "action_rank": 3,
                "action_type": "metric_review",
                "action_title": "Inspect high-risk metrics and drift signals",
                "action_detail": "Review top drift metrics, funnel distortion, and structural anomaly evidence",
                "owner_hint": "data-platform",
                "priority": "medium",
                "evidence": "drift / anomaly fallback",
            },
        ]

    return items[:5]


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
                FROM ai_incident_summary_day
                WHERE profile_id=%s AND dt BETWEEN %s AND %s
                ORDER BY dt
                """,
                (args.profile_id, args.dt_from, args.dt_to),
            )
            dates = [str(r["dt"]) for r in cur.fetchall()]

            for dt in dates:
                summary = fetch_summary(cur, args.profile_id, dt)
                actions = fetch_actions(cur, args.profile_id, dt)
                prompt_text = build_prompt(args.profile_id, dt, summary, actions)

                try:
                    model, response_text = llm_call(prompt_text)
                    items = parse_actions(response_text)
                except Exception as e:
                    model = "fallback_rule_based"
                    response_text = f"[FALLBACK] {type(e).__name__}: {str(e)}"
                    items = fallback_actions(summary, actions)

                cur.execute(
                    "DELETE FROM ai_recommended_action_day WHERE profile_id=%s AND dt=%s",
                    (args.profile_id, dt),
                )

                for idx, item in enumerate(items, start=1):
                    cur.execute(
                        """
                        REPLACE INTO ai_recommended_action_day
                        (
                            profile_id, dt, action_rank, action_type, action_title,
                            action_detail, owner_hint, priority, evidence
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            args.profile_id,
                            dt,
                            int(item.get("action_rank", idx)),
                            item.get("action_type"),
                            item.get("action_title"),
                            item.get("action_detail"),
                            item.get("owner_hint"),
                            item.get("priority"),
                            item.get("evidence"),
                        ),
                    )

                print(
                    f"[OK] ai action recommendation completed: "
                    f"profile_id={args.profile_id}, dt={dt}, actions={len(items)}, model={model}"
                )

        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
