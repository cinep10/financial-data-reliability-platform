#!/usr/bin/env python3
import argparse, os, pandas as pd, pymysql, joblib
def fetch(args):
  conn=pymysql.connect(host=args.host,port=args.port,user=args.user,password=args.password,database=args.db,cursorclass=pymysql.cursors.DictCursor)
  with conn:
    with conn.cursor() as cur:
      cur.execute("SELECT * FROM ml_feature_vector_day WHERE profile_id=%s AND dt BETWEEN %s AND %s ORDER BY dt",(args.profile_id,args.dt_from,args.dt_to)); return pd.DataFrame(list(cur.fetchall()))
def label_to_status(x): return {0:"normal",1:"warning",2:"alert"}.get(int(x),"normal")
def sf(v,f=0.0):
  try: return float(v)
  except Exception: return f
def sdt(v):
  try:
    x=pd.to_datetime(str(v),errors="coerce"); return None if pd.isna(x) else x.strftime("%Y-%m-%d")
  except Exception: return None
def fallback(row):
  s=sf(row.get("target_risk_score"),0.0)
  return (2,"alert",0.02,0.08,0.90,"fallback: target_risk_score alert") if s>=0.55 else ((1,"warning",0.15,0.70,0.15,"fallback: target_risk_score warning") if s>=0.30 else (0,"normal",0.90,0.08,0.02,"fallback: target_risk_score normal"))
ap=argparse.ArgumentParser()
ap.add_argument("--host", default=os.getenv("DB_HOST","127.0.0.1")); ap.add_argument("--port", type=int, default=int(os.getenv("DB_PORT","3306"))); ap.add_argument("--user", default=os.getenv("DB_USER","nethru")); ap.add_argument("--password", default=os.getenv("DB_PASSWORD","nethru1234")); ap.add_argument("--db", default=os.getenv("DB_NAME","weblog")); ap.add_argument("--profile-id", required=True); ap.add_argument("--dt-from", required=True); ap.add_argument("--dt-to", required=True); ap.add_argument("--model-path", default="ml_risk_model_safe.joblib"); ap.add_argument("--model-version", default="ml_risk_safe_v3"); ap.add_argument("--truncate", action="store_true")
args=ap.parse_args(); bundle=joblib.load(args.model_path); mode=bundle.get("mode","supervised"); pipe=bundle.get("pipeline"); feats=bundle.get("feature_columns",[]); model_name=bundle.get("model_name","ml_risk_model")
df=fetch(args); 
if df.empty: raise SystemExit("No rows found in ml_feature_vector_day")
conn=pymysql.connect(host=args.host,port=args.port,user=args.user,password=args.password,database=args.db,autocommit=False,cursorclass=pymysql.cursors.DictCursor)
with conn:
  with conn.cursor() as cur:
    if args.truncate: cur.execute("DELETE FROM ml_prediction_result WHERE profile_id=%s AND dt BETWEEN %s AND %s AND model_version=%s",(args.profile_id,args.dt_from,args.dt_to,args.model_version))
    ins="REPLACE INTO ml_prediction_result (profile_id,dt,model_name,model_version,predicted_label,predicted_risk_status,prob_normal,prob_warning,prob_alert,actual_risk_status,actual_risk_score,run_id,note) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    df=df.reset_index(drop=True)
    if mode=="supervised" and pipe is not None:
      X=df[feats].apply(pd.to_numeric,errors="coerce").fillna(0.0); probs=pipe.predict_proba(X); preds=pipe.predict(X); inserted=0
      for i,row in df.iterrows():
        dt=sdt(row["dt"]); 
        if dt is None: continue
        p=probs[i]; pn=float(p[0]) if len(p)>0 else 0.0; pw=float(p[1]) if len(p)>1 else 0.0; pa=float(p[2]) if len(p)>2 else 0.0; pl=int(preds[i]); ps=label_to_status(pl)
        cur.execute(ins,(row["profile_id"],dt,model_name,args.model_version,pl,ps,pn,pw,pa,row.get("target_risk_status"),sf(row.get("target_risk_score"),0.0),f"mlpred_{row['profile_id']}_{dt.replace('-','')}_{args.model_version}","supervised prediction")); inserted+=1
      print(f"[INFO] supervised rows inserted: {inserted}")
    else:
      inserted=0
      for _,row in df.iterrows():
        dt=sdt(row["dt"]); 
        if dt is None: continue
        pl,ps,pn,pw,pa,note=fallback(row)
        cur.execute(ins,(row["profile_id"],dt,model_name,args.model_version,pl,ps,pn,pw,pa,row.get("target_risk_status"),sf(row.get("target_risk_score"),0.0),f"mlpred_{row['profile_id']}_{dt.replace('-','')}_{args.model_version}",note)); inserted+=1
      print(f"[INFO] fallback rows inserted: {inserted}")
    conn.commit(); print(f"[OK] prediction completed: mode={mode}, rows={len(df)}")
