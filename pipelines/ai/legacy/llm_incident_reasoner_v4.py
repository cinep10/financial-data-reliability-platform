#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, os
from datetime import datetime, date
from decimal import Decimal
import pymysql
try:
    from openai import OpenAI
except Exception:
    OpenAI = None
PROMPT_VERSION='ai_incident_reasoner_v4'
def db_conn(a):
    return pymysql.connect(host=a.host, port=a.port, user=a.user, password=a.password, database=a.db, autocommit=False, cursorclass=pymysql.cursors.DictCursor, charset='utf8mb4')
def json_safe(obj):
    if isinstance(obj, dict): return {k: json_safe(v) for k,v in obj.items()}
    if isinstance(obj, list): return [json_safe(v) for v in obj]
    if isinstance(obj, tuple): return [json_safe(v) for v in obj]
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, (datetime, date)): return obj.isoformat()
    return obj
def fetch_rows(cur, sql, params): cur.execute(sql, params); return cur.fetchall()
def pick_dates(cur, profile_id, dt_from, dt_to):
    cur.execute("""SELECT DISTINCT dt FROM (
      SELECT dt FROM ml_prediction_result WHERE profile_id=%s AND dt BETWEEN %s AND %s
      UNION SELECT dt FROM data_risk_score_day_v3 WHERE profile_id=%s AND dt BETWEEN %s AND %s
    ) x ORDER BY dt""", (profile_id,dt_from,dt_to,profile_id,dt_from,dt_to))
    return [str(r['dt']) for r in cur.fetchall()]
def fetch_context(cur, profile_id, dt):
    cur.execute("""SELECT profile_id, dt, MAX(final_risk_score) AS risk_score, MAX(risk_grade) AS risk_grade,
                   MAX(stream_risk_score) AS stream_risk_score, MAX(stream_primary_issue) AS stream_primary_issue, MAX(stream_status) AS stream_status
                   FROM data_risk_score_day_v3 WHERE profile_id=%s AND dt=%s GROUP BY profile_id,dt""", (profile_id,dt))
    risk=cur.fetchone() or {}
    cur.execute("""SELECT predicted_risk_status, prob_alert, prob_warning, prob_normal, actual_risk_status, actual_risk_score, model_name, model_version
                   FROM ml_prediction_result WHERE profile_id=%s AND dt=%s ORDER BY model_version DESC LIMIT 1""", (profile_id,dt))
    pred=cur.fetchone() or {}
    context={'risk':risk,'prediction':pred}
    for k,sql in {
      'root_causes':"SELECT cause_rank, cause_type, cause_code, confidence, related_metric, cause_source FROM data_risk_root_cause_day WHERE profile_id=%s AND dt=%s ORDER BY cause_rank LIMIT 8",
      'drift':"SELECT metric_name, drift_status, drift_score FROM metric_drift_result_r WHERE profile_id=%s AND dt=%s AND drift_status IN ('alert','warn') ORDER BY drift_score DESC LIMIT 8",
      'stream_signal':"SELECT service_domain, missing_rate, duplicate_ratio, ordering_gap_score, avg_event_delay_ms, stream_risk_score FROM stream_risk_signal_day WHERE profile_id=%s AND dt=%s ORDER BY stream_risk_score DESC LIMIT 8",
      'actions':"SELECT metric_nm, root_cause, action_type, priority, recommended_fix FROM data_reliability_action_day WHERE profile_id=%s AND dt=%s ORDER BY FIELD(priority,'high','medium','low') LIMIT 8",
      'scenario_result':"SELECT scenario_name, risk_score_v3, predicted_alert_prob, predicted_label, root_cause_top1 FROM scenario_experiment_result_day WHERE profile_id=%s AND dt=%s ORDER BY scenario_run_id DESC LIMIT 3",
    }.items():
        try: context[k]=fetch_rows(cur,sql,(profile_id,dt))
        except Exception: context[k]=[]
    return context
def build_prompt(profile_id, dt, context):
    return f"""You are an expert data reliability incident analyst.
Summarize the incident for one service-day in strict JSON.
Service profile_id: {profile_id}
Date: {dt}
Input context:
{json.dumps(json_safe(context), ensure_ascii=False, indent=2)}
Return JSON with keys:
incident_title, incident_level, executive_summary, technical_summary, business_impact, recommended_actions, confidence_score
Rules: incident_level in [normal,warning,alert], recommended_actions 3 to 5 short strings, confidence_score from 0 to 1, use root cause, drift, and stream evidence directly.""".strip()
def llm_call(prompt_text):
    api_key=os.getenv('OPENAI_API_KEY')
    if not api_key: raise RuntimeError('OPENAI_API_KEY is required')
    if OpenAI is None: raise RuntimeError('openai package is not installed')
    model=os.getenv('OPENAI_MODEL','gpt-4.1-mini'); client=OpenAI(api_key=api_key)
    resp=client.responses.create(model=model, input=prompt_text, temperature=0.2)
    return model, getattr(resp,'output_text',None) or str(resp)
def parse_json(text, fallback_level='warning'):
    try:
        data=json.loads(text)
        return {'incident_title':data.get('incident_title','Data reliability incident'),'incident_level':data.get('incident_level',fallback_level),'executive_summary':data.get('executive_summary',''),'technical_summary':data.get('technical_summary',''),'business_impact':data.get('business_impact',''),'recommended_actions':data.get('recommended_actions',[]),'confidence_score':float(data.get('confidence_score',0.7))}
    except Exception:
        return {'incident_title':'Data reliability incident','incident_level':fallback_level,'executive_summary':text[:500],'technical_summary':text[:1000],'business_impact':'','recommended_actions':[],'confidence_score':0.5}
def fallback_reason(context):
    pred=context.get('prediction',{}); risk=context.get('risk',{})
    causes=[str(r.get('cause_type') or 'unknown') for r in context.get('root_causes',[])[:3]]
    drifts=[str(r.get('metric_name') or 'unknown_metric') for r in context.get('drift',[])[:3]]
    streams=[str(r.get('service_domain') or 'unknown_domain') for r in context.get('stream_signal',[])[:3]]
    incident_level=pred.get('predicted_risk_status') or ('alert' if float(risk.get('risk_score') or 0)>=0.6 else 'warning')
    score=float(risk.get('risk_score') or 0.0)
    return {'incident_title':(f'{causes[0]} detected' if causes else 'Data reliability incident'),'incident_level':incident_level,'executive_summary':f'Risk score is {score:.3f}. Primary evidence includes root causes={causes[:2]}, drift metrics={drifts[:2]}, stream domains={streams[:2]}.','technical_summary':f'Root causes: {", ".join(causes) if causes else "none"}. Drift metrics: {", ".join(drifts) if drifts else "none"}. Stream signals: {", ".join(streams) if streams else "none"}.','business_impact':('Moderate likelihood of KPI or funnel impact.' if incident_level!='normal' else 'Low immediate business impact.'),'recommended_actions':['Review top root causes and drift metrics','Check stream ingestion and completeness signals','Validate upstream mapping and recent deployment changes'],'confidence_score':(0.70 if incident_level!='normal' else 0.85)}
def upsert_summary(cur, profile_id, dt, run_id, model, context, parsed, prompt_text, response_text):
    risk_score=context.get('risk',{}).get('risk_score'); pred=context.get('prediction',{})
    cur.execute("""REPLACE INTO ai_incident_summary_day
    (profile_id, dt, run_id, risk_score, actual_risk_status, predicted_risk_status, predicted_alert_prob, incident_title, incident_level, executive_summary, technical_summary, business_impact, recommended_actions, confidence_score, llm_model, prompt_version)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (profile_id,dt,run_id,risk_score,pred.get('actual_risk_status'),pred.get('predicted_risk_status'),pred.get('prob_alert'),parsed['incident_title'],parsed['incident_level'],parsed['executive_summary'],parsed['technical_summary'],parsed['business_impact'],json.dumps(parsed['recommended_actions'], ensure_ascii=False),parsed['confidence_score'],model,PROMPT_VERSION))
    cur.execute("""REPLACE INTO ai_prompt_log (run_id, profile_id, dt, prompt_version, llm_model, prompt_text, response_text) VALUES (%s,%s,%s,%s,%s,%s,%s)""", (run_id,profile_id,dt,PROMPT_VERSION,model,prompt_text,response_text))
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--host', default=os.getenv('DB_HOST','127.0.0.1')); ap.add_argument('--port', type=int, default=int(os.getenv('DB_PORT','3306')))
    ap.add_argument('--user', default=os.getenv('DB_USER','nethru')); ap.add_argument('--password', default=os.getenv('DB_PASSWORD','nethru1234')); ap.add_argument('--db', default=os.getenv('DB_NAME','weblog'))
    ap.add_argument('--profile-id', required=True); ap.add_argument('--dt-from', required=True); ap.add_argument('--dt-to', required=True); ap.add_argument('--force-fallback', action='store_true')
    args=ap.parse_args(); conn=db_conn(args)
    try:
        with conn.cursor() as cur:
            for dt in pick_dates(cur,args.profile_id,args.dt_from,args.dt_to):
                context=fetch_context(cur,args.profile_id,dt); prompt_text=build_prompt(args.profile_id,dt,context)
                fallback_level=context.get('prediction',{}).get('predicted_risk_status','warning')
                try:
                    if args.force_fallback: raise RuntimeError('forced fallback')
                    model,response_text=llm_call(prompt_text); parsed=parse_json(response_text, fallback_level=fallback_level)
                except Exception as e:
                    model='fallback_rule_based'; response_text=f'[FALLBACK] {type(e).__name__}: {str(e)}'; parsed=fallback_reason(context)
                run_id=f"ai_summary_{args.profile_id}_{dt.replace('-','')}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
                upsert_summary(cur,args.profile_id,dt,run_id,model,context,parsed,prompt_text,response_text)
                print(f"[OK] ai incident summary completed: profile_id={args.profile_id}, dt={dt}, level={parsed['incident_level']}, model={model}")
        conn.commit()
    finally:
        conn.close()
if __name__=='__main__': main()
