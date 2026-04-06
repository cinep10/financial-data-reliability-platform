#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
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
  cur.execute("CREATE TABLE IF NOT EXISTS scenario_experiment_run (scenario_run_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, profile_id VARCHAR(64) NOT NULL, scenario_name VARCHAR(100) NOT NULL, scenario_type VARCHAR(50) NOT NULL, dt_from DATE NOT NULL, dt_to DATE NOT NULL, parameters_json TEXT NULL, note VARCHAR(255) NULL, started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (scenario_run_id), KEY idx_scenario_run_profile_dt (profile_id, dt_from, dt_to)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
  cur.execute("CREATE TABLE IF NOT EXISTS scenario_metric_change_log (scenario_run_id BIGINT UNSIGNED NOT NULL, profile_id VARCHAR(64) NOT NULL, dt DATE NOT NULL, metric_name VARCHAR(100) NOT NULL, before_value DECIMAL(20,6) NULL, after_value DECIMAL(20,6) NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (scenario_run_id, profile_id, dt, metric_name), KEY idx_scenario_metric_change_dt (profile_id, dt)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
  addcol(cur,"scenario_experiment_run","scenario_severity","scenario_severity VARCHAR(20) NULL")
  addcol(cur,"scenario_metric_change_log","scenario_name","scenario_name VARCHAR(100) NULL")
  addcol(cur,"scenario_metric_change_log","scenario_type","scenario_type VARCHAR(50) NULL")
def plan(name):
  if name=="campaign_spike": return ({"page_view_count":2.0,"daily_active_users":1.7,"raw_event_count":1.8,"collector_event_count":1.8,"new_user_ratio":1.25},"campaign","medium")
  if name=="weather_drop": return ({"page_view_count":0.70,"daily_active_users":0.72,"loan_view_count":0.78,"card_apply_start_count":0.80,"loan_apply_start_count":0.80},"weather","medium")
  if name=="auth_failure": return ({"auth_success_count":0.25,"auth_success_rate":0.35,"login_success_count":0.30,"risk_login_count":2.20,"auth_fail_count":1.80},"system_issue","high")
  if name=="funnel_break": return ({"loan_apply_submit_count":0.15,"card_apply_submit_count":0.20,"card_apply_submit_rate":0.35,"submit_capture_rate":0.25,"success_outcome_capture_rate":0.30},"system_issue","high")
  if name=="mixed_incident": return ({"page_view_count":1.80,"daily_active_users":1.50,"auth_success_count":0.35,"auth_success_rate":0.45,"loan_apply_submit_count":0.25,"card_apply_submit_count":0.30,"estimated_missing_rate":2.20},"mixed","high")
  raise ValueError(name)
ap=argparse.ArgumentParser(); ap.add_argument("--host", default="127.0.0.1"); ap.add_argument("--port", type=int, default=3306); ap.add_argument("--user", required=True); ap.add_argument("--password", default=""); ap.add_argument("--db", required=True); ap.add_argument("--profile-id", required=True); ap.add_argument("--scenario-name", required=True, choices=["campaign_spike","weather_drop","auth_failure","funnel_break","mixed_incident"]); ap.add_argument("--dt-from", required=True); ap.add_argument("--dt-to", required=True); ap.add_argument("--note", default="")
args=ap.parse_args(); changes,stype,severity=plan(args.scenario_name)
c=conn(args)
with c:
  with c.cursor() as cur:
    ensure(cur)
    cur.execute("INSERT INTO scenario_experiment_run (profile_id,scenario_name,scenario_type,dt_from,dt_to,parameters_json,note,scenario_severity) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",(args.profile_id,args.scenario_name,stype,args.dt_from,args.dt_to,json.dumps(changes,ensure_ascii=False),args.note or f'injected {args.scenario_name}',severity))
    run_id=cur.lastrowid
    metrics=list(changes.keys()); ph=", ".join(["%s"]*len(metrics))
    for dt in daterange(args.dt_from,args.dt_to):
      cur.execute(f"SELECT metric_name,metric_value FROM metric_value_day WHERE profile_id=%s AND dt=%s AND metric_name IN ({ph})",(args.profile_id,dt,*metrics))
      for row in cur.fetchall():
        metric=row["metric_name"]; before=row["metric_value"]; after=float(before)*float(changes[metric]) if before is not None else None
        cur.execute("UPDATE metric_value_day SET metric_value=%s WHERE profile_id=%s AND dt=%s AND metric_name=%s",(after,args.profile_id,dt,metric))
        cur.execute("REPLACE INTO scenario_metric_change_log (scenario_run_id,profile_id,dt,metric_name,before_value,after_value,scenario_name,scenario_type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",(run_id,args.profile_id,dt,metric,before,after,args.scenario_name,stype))
    c.commit(); print(f"[OK] scenario injected: run_id={run_id}, scenario={args.scenario_name}, dt_from={args.dt_from}, dt_to={args.dt_to}")
