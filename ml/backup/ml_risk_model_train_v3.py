#!/usr/bin/env python3
import argparse, json, os
from datetime import datetime, UTC
import joblib, numpy as np, pandas as pd, pymysql
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_recall_fscore_support
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
MODEL_NAME="ml_risk_model"
def feat_cols(cols):
    c=["daily_active_users","page_view_count","avg_session_duration","avg_session_duration_sec","avg_session_dura","new_user_ratio","auth_attempt_count","auth_success_count","auth_fail_count","auth_success_rate","auth_fail_rate","otp_request_count","risk_login_count","loan_view_count","loan_apply_start_count","loan_apply_submit_count","card_apply_start_count","card_apply_submit_count","card_apply_submit_rate","collector_event_count","raw_event_count","estimated_missing_rate","validation_fail_count","validation_warn_count","drift_alert_count","drift_warn_count","ml_feature_alert_count","ml_feature_warn_count","scenario_active_flag"]
    return [x for x in c if x in cols]
def map_status(s): return 2 if (s or "").lower()=="alert" else (1 if (s or "").lower()=="warning" else 0)
ap=argparse.ArgumentParser()
ap.add_argument("--host", default=os.getenv("DB_HOST","127.0.0.1")); ap.add_argument("--port", type=int, default=int(os.getenv("DB_PORT","3306")))
ap.add_argument("--user", default=os.getenv("DB_USER","nethru")); ap.add_argument("--password", default=os.getenv("DB_PASSWORD","nethru1234"))
ap.add_argument("--db", default=os.getenv("DB_NAME","weblog")); ap.add_argument("--profile-id", required=True); ap.add_argument("--dt-from", required=True); ap.add_argument("--dt-to", required=True)
ap.add_argument("--model-path", default="ml_risk_model_safe.joblib"); ap.add_argument("--report-path", default="ml_risk_model_report_safe.json"); ap.add_argument("--importance-csv", default="ml_feature_importance_safe.csv"); ap.add_argument("--model-version", default="ml_risk_safe_v3")
args=ap.parse_args()
conn=pymysql.connect(host=args.host,port=args.port,user=args.user,password=args.password,database=args.db,cursorclass=pymysql.cursors.DictCursor)
with conn:
  with conn.cursor() as cur:
    cur.execute("SELECT * FROM ml_feature_vector_day WHERE profile_id=%s AND dt BETWEEN %s AND %s ORDER BY dt",(args.profile_id,args.dt_from,args.dt_to)); df=pd.DataFrame(list(cur.fetchall()))
if df.empty: raise SystemExit("No rows found in ml_feature_vector_day")
feats=feat_cols(df.columns); X=df[feats].apply(pd.to_numeric,errors="coerce").fillna(0.0)
if "target_risk_label" in df.columns: y=pd.to_numeric(df["target_risk_label"],errors="coerce").fillna(0).astype(int); label_def="target_risk_label direct"
elif "target_risk_status" in df.columns: y=df["target_risk_status"].map(map_status).fillna(0).astype(int); label_def="target_risk_status mapped"
else: score=pd.to_numeric(df.get("target_risk_score",0),errors="coerce").fillna(0); y=pd.cut(score,bins=[-1,0.30,0.55,999999],labels=[0,1,2]).astype(int); label_def="target_risk_score thresholds 0.30/0.55"
classes=sorted(set(y.tolist()))
if len(classes)<2:
  bundle={"model_name":MODEL_NAME,"model_version":args.model_version,"feature_columns":feats,"pipeline":None,"trained_at":datetime.now(UTC).isoformat(),"mode":"rule_fallback","single_class_label":int(classes[0])}
  joblib.dump(bundle,args.model_path)
  imp=pd.DataFrame({"feature_name":feats,"coefficient":0.0,"abs_coefficient":0.0,"importance_mean":0.0,"importance_std":0.0,"importance_rank":range(1,len(feats)+1)}); imp.to_csv(args.importance_csv,index=False)
  rep={"model_name":MODEL_NAME,"model_version":args.model_version,"mode":"rule_fallback","single_class_label":int(classes[0]),"feature_columns":feats,"label_definition":label_def,"class_distribution":y.value_counts().to_dict(),"row_count":int(len(df))}
  open(args.report_path,"w",encoding="utf-8").write(json.dumps(rep,ensure_ascii=False,indent=2))
  print(f"[OK] ml_risk_model_train_v3 fallback completed: single_class={classes[0]}, rows={len(df)}"); raise SystemExit(0)
split=max(1,int(len(df)*0.7)); Xtr,Xte=X.iloc[:split],X.iloc[split:]; ytr,yte=y.iloc[:split],y.iloc[split:]
pipe=Pipeline([("imputer",SimpleImputer(strategy="constant",fill_value=0.0)),("scaler",StandardScaler()),("model",LogisticRegression(max_iter=2000,class_weight="balanced",random_state=42))]); pipe.fit(Xtr,ytr)
metrics={}
if len(Xte)>0:
  yp=pipe.predict(Xte); acc=accuracy_score(yte,yp); p,r,f,_=precision_recall_fscore_support(yte,yp,average="weighted",zero_division=0); metrics={"accuracy":float(acc),"precision_weighted":float(p),"recall_weighted":float(r),"f1_weighted":float(f)}
bundle={"model_name":MODEL_NAME,"model_version":args.model_version,"feature_columns":feats,"pipeline":pipe,"trained_at":datetime.now(UTC).isoformat(),"mode":"supervised"}; joblib.dump(bundle,args.model_path)
coef=pipe.named_steps["model"].coef_; abs_mean=np.mean(np.abs(coef),axis=0); imp=pd.DataFrame({"feature_name":feats,"coefficient":np.mean(coef,axis=0),"abs_coefficient":abs_mean,"importance_mean":abs_mean,"importance_std":np.std(np.abs(coef),axis=0)}).sort_values("importance_mean",ascending=False).reset_index(drop=True); imp["importance_rank"]=imp.index+1; imp.to_csv(args.importance_csv,index=False)
rep={"model_name":MODEL_NAME,"model_version":args.model_version,"mode":"supervised","feature_columns":feats,"label_definition":label_def,"class_distribution":y.value_counts().to_dict(),"row_count":int(len(df)),"metrics":metrics}
open(args.report_path,"w",encoding="utf-8").write(json.dumps(rep,ensure_ascii=False,indent=2)); print(f"[OK] ml_risk_model_train_v3 supervised completed: rows={len(df)}, features={len(feats)}")
