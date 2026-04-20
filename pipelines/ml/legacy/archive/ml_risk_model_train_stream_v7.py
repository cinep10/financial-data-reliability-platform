#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, os
from datetime import datetime, UTC
import joblib, numpy as np, pandas as pd, pymysql
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, confusion_matrix
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
MODEL_NAME='ml_risk_model_stream'
def feature_columns(cols):
    wanted=['daily_active_users','page_view_count','avg_session_duration_sec','avg_session_duration','new_user_ratio','auth_attempt_count','auth_success_count','auth_fail_count','auth_success_rate','auth_fail_rate','otp_request_count','risk_login_count','loan_view_count','loan_apply_start_count','loan_apply_submit_count','card_apply_start_count','card_apply_submit_count','card_apply_submit_rate','collector_event_count','raw_event_count','estimated_missing_rate','validation_fail_count','validation_warn_count','drift_alert_count','drift_warn_count','ml_feature_alert_count','ml_feature_warn_count','stream_missing_rate','stream_duplicate_ratio','stream_ordering_gap_score','stream_avg_event_delay_ms','stream_risk_score','stream_signal_count','volume_multiplier','conversion_multiplier','timeout_multiplier','retry_multiplier','campaign_flag']
    return [c for c in wanted if c in cols]
def save_report(path, obj):
    with open(path,'w',encoding='utf-8') as f: json.dump(obj,f,ensure_ascii=False,indent=2)
ap=argparse.ArgumentParser()
ap.add_argument('--host', default=os.getenv('DB_HOST','127.0.0.1')); ap.add_argument('--port', type=int, default=int(os.getenv('DB_PORT','3306')))
ap.add_argument('--user', default=os.getenv('DB_USER','nethru')); ap.add_argument('--password', default=os.getenv('DB_PASSWORD','nethru1234')); ap.add_argument('--db', default=os.getenv('DB_NAME','weblog'))
ap.add_argument('--profile-id', required=True); ap.add_argument('--dt-from', required=True); ap.add_argument('--dt-to', required=True)
ap.add_argument('--model-path', default='ml_risk_model_stream_v7.joblib'); ap.add_argument('--report-path', default='ml_risk_model_stream_report_v7.json'); ap.add_argument('--importance-csv', default='ml_feature_importance_stream_v7.csv'); ap.add_argument('--model-version', default='ml_risk_stream_v7')
args=ap.parse_args()
conn=pymysql.connect(host=args.host, port=args.port, user=args.user, password=args.password, database=args.db, autocommit=False, cursorclass=pymysql.cursors.DictCursor)
try:
    df=pd.read_sql('SELECT * FROM ml_feature_vector_day WHERE profile_id=%s AND dt BETWEEN %s AND %s ORDER BY dt', conn, params=[args.profile_id,args.dt_from,args.dt_to])
finally:
    conn.close()
if df.empty: raise SystemExit('No rows found in ml_feature_vector_day')
feats=feature_columns(df.columns)
if not feats: raise SystemExit('No usable feature columns found')
for c in feats: df[c]=pd.to_numeric(df[c], errors='coerce')
y=pd.to_numeric(df['target_risk_label'], errors='coerce').fillna(0).astype(int); X=df[feats].copy()
class_counts=y.value_counts().sort_index().to_dict(); uniq=sorted(set(y.tolist()))
if len(uniq)<2:
    joblib.dump({'model_name':MODEL_NAME,'model_version':args.model_version,'feature_columns':feats,'mode':'rule_fallback','trained_at':datetime.now(UTC).isoformat()}, args.model_path)
    save_report(args.report_path, {'mode':'rule_fallback','reason':'single_class_only','rows':len(df),'class_counts':class_counts,'feature_columns':feats}); print(f"[OK] ml_risk_model_train_stream_v7 fallback completed: single_class={uniq[0]}, rows={len(df)}"); raise SystemExit(0)
split=max(10,int(len(df)*0.7)); split=min(split,len(df)-1); X_train,X_test=X.iloc[:split],X.iloc[split:]; y_train,y_test=y.iloc[:split],y.iloc[split:]
if len(sorted(set(y_train.tolist())))<2:
    joblib.dump({'model_name':MODEL_NAME,'model_version':args.model_version,'feature_columns':feats,'mode':'rule_fallback','trained_at':datetime.now(UTC).isoformat()}, args.model_path)
    save_report(args.report_path, {'mode':'rule_fallback','reason':'train_split_single_class','rows':len(df),'class_counts':class_counts,'feature_columns':feats}); print(f"[OK] ml_risk_model_train_stream_v7 fallback completed: reason=train_split_single_class, rows={len(df)}"); raise SystemExit(0)
pipe=Pipeline([('imputer',SimpleImputer(strategy='constant',fill_value=0.0)),('scaler',StandardScaler()),('model',LogisticRegression(max_iter=3000,class_weight='balanced',random_state=42))])
pipe.fit(X_train,y_train); y_pred=pipe.predict(X_test)
acc=accuracy_score(y_test,y_pred); precision,recall,f1,_=precision_recall_fscore_support(y_test,y_pred,average='weighted',zero_division=0); cm=confusion_matrix(y_test,y_pred,labels=sorted(set(y.tolist())))
joblib.dump({'model_name':MODEL_NAME,'model_version':args.model_version,'feature_columns':feats,'pipeline':pipe,'trained_at':datetime.now(UTC).isoformat(),'mode':'supervised'}, args.model_path)
model=pipe.named_steps['model']; coef=model.coef_; abs_mean=np.mean(np.abs(coef),axis=0)
imp_df=pd.DataFrame({'feature_name':feats,'coefficient':np.mean(coef,axis=0),'importance_mean':abs_mean}).sort_values('importance_mean',ascending=False).reset_index(drop=True)
imp_df['importance_rank']=imp_df.index+1; imp_df.to_csv(args.importance_csv,index=False)
save_report(args.report_path, {'model_name':MODEL_NAME,'model_version':args.model_version,'mode':'supervised','feature_columns':feats,'rows':len(df),'train_rows':len(X_train),'test_rows':len(X_test),'class_counts':class_counts,'accuracy':acc,'precision_weighted':precision,'recall_weighted':recall,'f1_weighted':f1,'confusion_matrix':cm.tolist(),'top_coefficients':imp_df.head(15).to_dict(orient='records')})
print(f"[OK] ml_risk_model_train_stream_v7 completed: rows={len(df)}, feats={len(feats)}, acc={acc:.4f}")
