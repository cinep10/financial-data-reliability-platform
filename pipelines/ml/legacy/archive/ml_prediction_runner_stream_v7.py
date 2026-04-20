#!/usr/bin/env python3
from __future__ import annotations
import argparse, os, joblib, pandas as pd, pymysql

def safe_float(v, fallback=0.0):
    try: return float(v) if v is not None else fallback
    except Exception: return fallback

def normalize_dt(v):
    try:
        x=pd.to_datetime(v, errors='coerce')
        return None if pd.isna(x) else x.strftime('%Y-%m-%d')
    except Exception: return None

def fallback_predict(row):
    score=safe_float(row.get('target_risk_score'),0.0)
    if score >= 55: return 2,'alert',0.05,0.15,0.80,'fallback: high score'
    if score >= 35: return 1,'warning',0.20,0.60,0.20,'fallback: medium score'
    return 0,'normal',0.85,0.12,0.03,'fallback: low score'

def calibrated_status(prob_normal, prob_warning, prob_alert):
    if prob_alert >= 0.65: return 2,'alert'
    if prob_alert >= 0.35 or prob_warning >= 0.45: return 1,'warning'
    return 0,'normal'

ap=argparse.ArgumentParser()
ap.add_argument('--host', default=os.getenv('DB_HOST','127.0.0.1')); ap.add_argument('--port', type=int, default=int(os.getenv('DB_PORT','3306')))
ap.add_argument('--user', default=os.getenv('DB_USER','nethru')); ap.add_argument('--password', default=os.getenv('DB_PASSWORD','nethru1234')); ap.add_argument('--db', default=os.getenv('DB_NAME','weblog'))
ap.add_argument('--profile-id', required=True); ap.add_argument('--dt-from', required=True); ap.add_argument('--dt-to', required=True)
ap.add_argument('--model-path', default='ml_risk_model_stream_v7.joblib'); ap.add_argument('--model-version', default='ml_risk_stream_v7'); ap.add_argument('--truncate', action='store_true')
args=ap.parse_args()

bundle=joblib.load(args.model_path); mode=bundle.get('mode','rule_fallback'); pipe=bundle.get('pipeline'); feats=bundle.get('feature_columns',[]); model_name=bundle.get('model_name','ml_risk_model_stream')
conn=pymysql.connect(host=args.host, port=args.port, user=args.user, password=args.password, database=args.db, autocommit=False, cursorclass=pymysql.cursors.DictCursor)
try:
    with conn.cursor() as cur:
        cur.execute('SELECT * FROM ml_feature_vector_day WHERE profile_id=%s AND dt BETWEEN %s AND %s ORDER BY dt', (args.profile_id,args.dt_from,args.dt_to)); rows=cur.fetchall()
    df=pd.DataFrame(list(rows))
    if df.empty: raise SystemExit('No rows found in ml_feature_vector_day')
    df['dt_norm']=df['dt'].apply(normalize_dt); df=df[df['dt_norm'].notna()].copy()
    if df.empty: raise SystemExit('No valid dt rows after normalization')
    with conn.cursor() as cur:
        if args.truncate:
            cur.execute('DELETE FROM ml_prediction_result WHERE profile_id=%s AND dt BETWEEN %s AND %s AND model_version=%s', (args.profile_id,args.dt_from,args.dt_to,args.model_version))
        insert_sql='''REPLACE INTO ml_prediction_result (profile_id,dt,model_name,model_version,predicted_label,predicted_risk_status,prob_normal,prob_warning,prob_alert,actual_risk_status,actual_risk_score,run_id,note) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        inserted=0
        if mode == 'supervised' and pipe is not None and feats:
            X=df[feats].copy()
            for c in feats: X[c]=pd.to_numeric(X[c], errors='coerce')
            probs=pipe.predict_proba(X); class_order=list(pipe.named_steps['model'].classes_)
            for i,row in df.iterrows():
                prob_map={0:0.0,1:0.0,2:0.0}
                for cls_idx,cls_val in enumerate(class_order): prob_map[int(cls_val)] = float(probs[i][cls_idx])
                pred_label,pred_status=calibrated_status(prob_map[0],prob_map[1],prob_map[2]); dt=row['dt_norm']
                cur.execute(insert_sql,(row['profile_id'],dt,model_name,args.model_version,pred_label,pred_status,prob_map[0],prob_map[1],prob_map[2],row.get('target_risk_status'),safe_float(row.get('target_risk_score'),0.0),f"mlpred_stream_{row['profile_id']}_{dt.replace('-', '')}_{args.model_version}",'supervised prediction with stream/exogenous features')); inserted += 1
        else:
            for _,row in df.iterrows():
                pred_label,pred_status,prob_normal,prob_warning,prob_alert,note=fallback_predict(row); dt=row['dt_norm']
                cur.execute(insert_sql,(row['profile_id'],dt,model_name,args.model_version,pred_label,pred_status,prob_normal,prob_warning,prob_alert,row.get('target_risk_status'),safe_float(row.get('target_risk_score'),0.0),f"mlpred_stream_{row['profile_id']}_{dt.replace('-', '')}_{args.model_version}",note)); inserted += 1
    conn.commit(); print(f"[OK] ml_prediction_runner_stream_v7 completed: mode={mode}, rows={inserted}")
finally:
    conn.close()
