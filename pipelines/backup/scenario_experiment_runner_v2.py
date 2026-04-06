#!/usr/bin/env python3
from __future__ import annotations
import argparse
from datetime import datetime, timedelta
from typing import Iterable
import pymysql
def daterange(start,end):
  cur=datetime.strptime(start,"%Y-%m-%d").date(); end_dt=datetime.strptime(end,"%Y-%m-%d").date()
  while cur<=end_dt: yield cur.isoformat(); cur+=timedelta(days=1)
def conn(args): return pymysql.connect(host=args.host,port=args.port,user=args.user,password=args.password,database=args.db,charset="utf8mb4",autocommit=False,cursorclass=pymysql.cursors.DictCursor)
def addcol(cur,t,c,ddl):
  cur.execute("SELECT COUNT(*) cnt FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=%s AND column_name=%s",(t,c))
  if int(cur.fetchone()["cnt"])==0: cur.execute(f"ALTER TABLE {t} ADD COLUMN {ddl}")
def ensure(cur):
  cur.execute("CREATE TABLE IF NOT EXISTS scenario_experiment_result_day (profile_id VARCHAR(64) NOT NULL, dt DATE NOT NULL, scenario_run_id BIGINT UNSIGNED NOT NULL, scenario_name VARCHAR(100) NOT NULL, scenario_type VARCHAR(50) NOT NULL, risk_score_v2 DECIMAL(20,6) NULL, risk_score_v3 DECIMAL(20,6) NULL, validation_warn_count INT NULL, validation_fail_count INT NULL, drift_alert_count INT NULL, drift_warn_count INT NULL, ml_feature_alert_count INT NULL, ml_feature_warn_count INT NULL, predicted_alert_prob DECIMAL(20,6) NULL, predicted_label VARCHAR(30) NULL, root_cause_top1 VARCHAR(255) NULL, traffic_page_view_count DECIMAL(20,6) NULL, missing_rate DECIMAL(20,6) NULL, note VARCHAR(255) NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (profile_id, dt, scenario_run_id), KEY idx_scenario_result_profile_dt (profile_id, dt), KEY idx_scenario_result_run (scenario_run_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
  addcol(cur,"scenario_experiment_result_day","label_source","label_source VARCHAR(50) NULL"); addcol(cur,"scenario_experiment_result_day","prediction_mode","prediction_mode VARCHAR(30) NULL"); addcol(cur,"scenario_experiment_result_day","scenario_active_flag","scenario_active_flag TINYINT NULL"); addcol(cur,"scenario_experiment_result_day","target_risk_label","target_risk_label INT NULL")
ap=argparse.ArgumentParser(); ap.add_argument("--host", default="127.0.0.1"); ap.add_argument("--port", type=int, default=3306); ap.add_argument("--user", required=True); ap.add_argument("--password", default=""); ap.add_argument("--db", required=True); ap.add_argument("--profile-id", required=True); ap.add_argument("--scenario-run-id", type=int, required=True); ap.add_argument("--dt-from", required=True); ap.add_argument("--dt-to", required=True); ap.add_argument("--truncate", action="store_true")
args=ap.parse_args(); c=conn(args)
with c:
  with c.cursor() as cur:
    ensure(cur)
    cur.execute("SELECT scenario_name,scenario_type,note FROM scenario_experiment_run WHERE scenario_run_id=%s AND profile_id=%s",(args.scenario_run_id,args.profile_id)); meta=cur.fetchone()
    if not meta: raise SystemExit(f"scenario_run_id not found: {args.scenario_run_id}")
    if args.truncate: cur.execute("DELETE FROM scenario_experiment_result_day WHERE profile_id=%s AND dt BETWEEN %s AND %s AND scenario_run_id=%s",(args.profile_id,args.dt_from,args.dt_to,args.scenario_run_id))
    rows=[]
    for dt in daterange(args.dt_from,args.dt_to):
      cur.execute("SELECT MAX(risk_score) risk_score_v2 FROM data_risk_score_day_v2 WHERE profile_id=%s AND dt=%s",(args.profile_id,dt)); r2=cur.fetchone() or {}
      cur.execute("SELECT MAX(final_risk_score) risk_score_v3 FROM data_risk_score_day_v3 WHERE profile_id=%s AND dt=%s",(args.profile_id,dt)); r3=cur.fetchone() or {}
      cur.execute("SELECT SUM(warn_count) validation_warn_count,SUM(fail_count) validation_fail_count FROM validation_summary_day WHERE profile_id=%s AND dt=%s",(args.profile_id,dt)); v=cur.fetchone() or {}
      cur.execute("SELECT SUM(CASE WHEN drift_status='alert' THEN 1 ELSE 0 END) drift_alert_count,SUM(CASE WHEN drift_status='warn' THEN 1 ELSE 0 END) drift_warn_count FROM metric_drift_result_r WHERE profile_id=%s AND dt=%s",(args.profile_id,dt)); dr=cur.fetchone() or {}
      cur.execute("SELECT cause_type,related_metric FROM data_risk_root_cause_day WHERE profile_id=%s AND dt=%s ORDER BY cause_rank ASC LIMIT 1",(args.profile_id,dt)); rc=cur.fetchone() or {}
      cur.execute("SELECT metric_value FROM metric_value_day WHERE profile_id=%s AND dt=%s AND metric_name='page_view_count'",(args.profile_id,dt)); pv=cur.fetchone() or {}
      cur.execute("SELECT metric_value FROM metric_value_day WHERE profile_id=%s AND dt=%s AND metric_name='estimated_missing_rate'",(args.profile_id,dt)); mr=cur.fetchone() or {}
      cur.execute("SELECT prob_alert,predicted_risk_status,note FROM ml_prediction_result WHERE profile_id=%s AND dt=%s ORDER BY created_at DESC LIMIT 1",(args.profile_id,dt)); pred=cur.fetchone() or {}
      cur.execute("SELECT target_risk_label,label_source,scenario_active_flag FROM ml_feature_vector_day WHERE profile_id=%s AND dt=%s LIMIT 1",(args.profile_id,dt)); fv=cur.fetchone() or {}
      top1=(f"{rc.get('cause_type','')} | {rc.get('related_metric','')}".strip(" |") if rc else None)
      pnote=(pred.get("note") or "").lower(); pmode="supervised" if "supervised" in pnote else ("rule_fallback" if "fallback" in pnote else None)
      rows.append((args.profile_id,dt,args.scenario_run_id,meta["scenario_name"],meta["scenario_type"],r2.get("risk_score_v2"),r3.get("risk_score_v3"),v.get("validation_warn_count"),v.get("validation_fail_count"),dr.get("drift_alert_count"),dr.get("drift_warn_count"),None,None,pred.get("prob_alert"),pred.get("predicted_risk_status"),top1,pv.get("metric_value"),mr.get("metric_value"),meta.get("note") or f"scenario_run_id={args.scenario_run_id}",fv.get("label_source"),pmode,fv.get("scenario_active_flag"),fv.get("target_risk_label")))
    cur.executemany("INSERT INTO scenario_experiment_result_day (profile_id,dt,scenario_run_id,scenario_name,scenario_type,risk_score_v2,risk_score_v3,validation_warn_count,validation_fail_count,drift_alert_count,drift_warn_count,ml_feature_alert_count,ml_feature_warn_count,predicted_alert_prob,predicted_label,root_cause_top1,traffic_page_view_count,missing_rate,note,label_source,prediction_mode,scenario_active_flag,target_risk_label) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE scenario_name=VALUES(scenario_name),scenario_type=VALUES(scenario_type),risk_score_v2=VALUES(risk_score_v2),risk_score_v3=VALUES(risk_score_v3),validation_warn_count=VALUES(validation_warn_count),validation_fail_count=VALUES(validation_fail_count),drift_alert_count=VALUES(drift_alert_count),drift_warn_count=VALUES(drift_warn_count),ml_feature_alert_count=VALUES(ml_feature_alert_count),ml_feature_warn_count=VALUES(ml_feature_warn_count),predicted_alert_prob=VALUES(predicted_alert_prob),predicted_label=VALUES(predicted_label),root_cause_top1=VALUES(root_cause_top1),traffic_page_view_count=VALUES(traffic_page_view_count),missing_rate=VALUES(missing_rate),note=VALUES(note),label_source=VALUES(label_source),prediction_mode=VALUES(prediction_mode),scenario_active_flag=VALUES(scenario_active_flag),target_risk_label=VALUES(target_risk_label)",rows)
    c.commit(); print(f"[OK] scenario_experiment_runner_v2 completed: run_id={args.scenario_run_id}, rows={len(rows)}")
