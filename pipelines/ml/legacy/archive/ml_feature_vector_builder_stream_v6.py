#!/usr/bin/env python3
from __future__ import annotations
import argparse, os
from decimal import Decimal
import pymysql

def d(v):
    try:
        return Decimal(str(v or 0))
    except Exception:
        return Decimal('0')

def safe_float(v): return float(d(v))
def safe_int(v): return int(v or 0)

def cols(cur, table_name):
    cur.execute(f"DESC {table_name}")
    return [r['Field'] for r in cur.fetchall()]

def choose(columns, *names):
    for n in names:
        if n in columns:
            return n
    return None

def scenario_label(scenario_name, intensity, score, active):
    scenario_name = (scenario_name or '').strip().lower()
    intensity = (intensity or 'baseline').strip().lower()
    if active == 0 or scenario_name in ('', 'baseline', 'none', 'null'):
        if score >= 35: return 1, 'warning', 'score_only_baseline'
        return 0, 'normal', 'baseline_default'
    if scenario_name in ('collector_drop','auth_failure','funnel_break','degraded','weather_drop','mixed_incident'):
        if intensity == 'severe':
            return (2 if score >= 35 else 1), ('alert' if score >= 35 else 'warning'), 'scenario_override'
        if intensity == 'medium':
            return (2 if score >= 55 else 1), ('alert' if score >= 55 else 'warning'), 'scenario_override'
        return 1, 'warning', 'scenario_override'
    if scenario_name in ('campaign_spike','salary_day','tax_season'):
        if score >= 55: return 1, 'warning', 'business_event_adjusted'
        return 0, 'normal', 'business_event_adjusted'
    if score >= 55: return 2, 'alert', 'unknown_scenario_score'
    if score >= 35: return 1, 'warning', 'unknown_scenario_score'
    return 0, 'normal', 'unknown_scenario_score'

ap = argparse.ArgumentParser()
ap.add_argument('--host', default=os.getenv('DB_HOST', '127.0.0.1'))
ap.add_argument('--port', type=int, default=int(os.getenv('DB_PORT', '3306')))
ap.add_argument('--user', default=os.getenv('DB_USER', 'nethru'))
ap.add_argument('--password', default=os.getenv('DB_PASSWORD', 'nethru1234'))
ap.add_argument('--db', default=os.getenv('DB_NAME', 'weblog'))
ap.add_argument('--profile-id', required=True)
ap.add_argument('--dt-from', required=True)
ap.add_argument('--dt-to', required=True)
ap.add_argument('--truncate', action='store_true')
args = ap.parse_args()

conn = pymysql.connect(host=args.host, port=args.port, user=args.user, password=args.password, database=args.db,
                       autocommit=False, cursorclass=pymysql.cursors.DictCursor)
with conn:
    with conn.cursor() as cur:
        mlfv_cols = cols(cur, 'ml_feature_vector_day')
        risk_cols = cols(cur, 'data_risk_score_day_v3')
        avg_col = choose(mlfv_cols, 'avg_session_duration_sec', 'avg_session_duration', 'avg_session_dura')
        risk_score_col = choose(risk_cols, 'final_risk_score', 'risk_score')
        risk_grade_col = choose(risk_cols, 'risk_grade', 'risk_status')
        if not avg_col or not risk_score_col or not risk_grade_col:
            raise SystemExit('Required columns not found')
        if args.truncate:
            cur.execute('DELETE FROM ml_feature_vector_day WHERE profile_id=%s AND dt BETWEEN %s AND %s', (args.profile_id, args.dt_from, args.dt_to))
        cur.execute('''
            SELECT m.profile_id,m.dt,
              MAX(CASE WHEN m.metric_name='daily_active_users' THEN m.metric_value END) AS daily_active_users,
              MAX(CASE WHEN m.metric_name='page_view_count' THEN m.metric_value END) AS page_view_count,
              MAX(CASE WHEN m.metric_name='avg_session_duration_sec' THEN m.metric_value END) AS avg_session_duration_value,
              MAX(CASE WHEN m.metric_name='avg_session_duration' THEN m.metric_value END) AS avg_session_duration_value_alt,
              MAX(CASE WHEN m.metric_name='new_user_ratio' THEN m.metric_value END) AS new_user_ratio,
              MAX(CASE WHEN m.metric_name='auth_attempt_count' THEN m.metric_value END) AS auth_attempt_count,
              MAX(CASE WHEN m.metric_name='auth_success_count' THEN m.metric_value END) AS auth_success_count,
              MAX(CASE WHEN m.metric_name='auth_fail_count' THEN m.metric_value END) AS auth_fail_count,
              MAX(CASE WHEN m.metric_name='auth_success_rate' THEN m.metric_value END) AS auth_success_rate,
              MAX(CASE WHEN m.metric_name='auth_fail_rate' THEN m.metric_value END) AS auth_fail_rate,
              MAX(CASE WHEN m.metric_name='otp_request_count' THEN m.metric_value END) AS otp_request_count,
              MAX(CASE WHEN m.metric_name='risk_login_count' THEN m.metric_value END) AS risk_login_count,
              MAX(CASE WHEN m.metric_name='loan_view_count' THEN m.metric_value END) AS loan_view_count,
              MAX(CASE WHEN m.metric_name='loan_apply_start_count' THEN m.metric_value END) AS loan_apply_start_count,
              MAX(CASE WHEN m.metric_name='loan_apply_submit_count' THEN m.metric_value END) AS loan_apply_submit_count,
              MAX(CASE WHEN m.metric_name='card_apply_start_count' THEN m.metric_value END) AS card_apply_start_count,
              MAX(CASE WHEN m.metric_name='card_apply_submit_count' THEN m.metric_value END) AS card_apply_submit_count,
              MAX(CASE WHEN m.metric_name='card_apply_submit_rate' THEN m.metric_value END) AS card_apply_submit_rate,
              MAX(CASE WHEN m.metric_name='collector_event_count' THEN m.metric_value END) AS collector_event_count,
              MAX(CASE WHEN m.metric_name='raw_event_count' THEN m.metric_value END) AS raw_event_count,
              MAX(CASE WHEN m.metric_name='estimated_missing_rate' THEN m.metric_value END) AS estimated_missing_rate,
              COALESCE(MAX(v.fail_count),0) AS validation_fail_count,
              COALESCE(MAX(v.warn_count),0) AS validation_warn_count,
              COALESCE(MAX(ds.alert_cnt),0) AS drift_alert_count,
              COALESCE(MAX(ds.warn_cnt),0) AS drift_warn_count,
              COALESCE(MAX(ta.alert_cnt),0) + COALESCE(MAX(ca.alert_cnt),0) AS ml_feature_alert_count,
              COALESCE(MAX(ta.warn_cnt),0) + COALESCE(MAX(ca.warn_cnt),0) AS ml_feature_warn_count
            FROM metric_value_day m
            LEFT JOIN (SELECT profile_id,dt,MAX(fail_count) fail_count,MAX(warn_count) warn_count FROM validation_summary_day GROUP BY profile_id,dt) v ON m.profile_id=v.profile_id AND m.dt=v.dt
            LEFT JOIN (SELECT profile_id,dt,SUM(CASE WHEN drift_status='alert' THEN 1 ELSE 0 END) alert_cnt,SUM(CASE WHEN drift_status='warn' THEN 1 ELSE 0 END) warn_cnt FROM metric_drift_result_r GROUP BY profile_id,dt) ds ON m.profile_id=ds.profile_id AND m.dt=ds.dt
            LEFT JOIN (SELECT profile_id,dt,SUM(CASE WHEN anomaly_status='alert' THEN 1 ELSE 0 END) alert_cnt,SUM(CASE WHEN anomaly_status='warn' THEN 1 ELSE 0 END) warn_cnt FROM metric_time_anomaly_day GROUP BY profile_id,dt) ta ON m.profile_id=ta.profile_id AND m.dt=ta.dt
            LEFT JOIN (SELECT profile_id,dt,SUM(CASE WHEN anomaly_status='alert' THEN 1 ELSE 0 END) alert_cnt,SUM(CASE WHEN anomaly_status='warn' THEN 1 ELSE 0 END) warn_cnt FROM metric_correlation_anomaly_day GROUP BY profile_id,dt) ca ON m.profile_id=ca.profile_id AND m.dt=ca.dt
            WHERE m.profile_id=%s AND m.dt BETWEEN %s AND %s GROUP BY m.profile_id,m.dt ORDER BY m.dt''', (args.profile_id, args.dt_from, args.dt_to))
        rows = cur.fetchall()
        cur.execute(f"SELECT profile_id,dt,MAX({risk_score_col}) agg_risk_score, CASE WHEN SUM(CASE WHEN {risk_grade_col}='high' THEN 1 ELSE 0 END) > 0 THEN 'alert' WHEN SUM(CASE WHEN {risk_grade_col}='medium' THEN 1 ELSE 0 END) > 0 THEN 'warning' ELSE 'normal' END AS agg_risk_status FROM data_risk_score_day_v3 WHERE profile_id=%s AND dt BETWEEN %s AND %s GROUP BY profile_id,dt", (args.profile_id, args.dt_from, args.dt_to))
        risk_map = {(str(r['profile_id']), str(r['dt'])): r for r in cur.fetchall()}
        cur.execute('SELECT scenario_run_id,profile_id,scenario_name,scenario_type,scenario_intensity,dt_from,dt_to FROM scenario_experiment_run WHERE profile_id=%s AND dt_to >= %s AND dt_from <= %s ORDER BY scenario_run_id DESC', (args.profile_id, args.dt_from, args.dt_to))
        runs = cur.fetchall()
        cur.execute('SELECT profile_id,dt,AVG(missing_rate) stream_missing_rate,AVG(duplicate_ratio) stream_duplicate_ratio,AVG(ordering_gap_score) stream_ordering_gap_score,AVG(avg_event_delay_ms) stream_avg_event_delay_ms,AVG(stream_risk_score) stream_risk_score,COUNT(*) stream_signal_count FROM stream_risk_signal_day WHERE profile_id=%s AND dt BETWEEN %s AND %s GROUP BY profile_id,dt', (args.profile_id, args.dt_from, args.dt_to))
        stream_map = {(str(r['profile_id']), str(r['dt'])): r for r in cur.fetchall()}
        cur.execute("SELECT profile_id,dt,SUBSTRING_INDEX(GROUP_CONCAT(weather_type ORDER BY hh SEPARATOR ','), ',', 1) weather_type,SUBSTRING_INDEX(GROUP_CONCAT(campaign_flag ORDER BY hh SEPARATOR ','), ',', 1) campaign_flag_text,SUBSTRING_INDEX(GROUP_CONCAT(system_flag ORDER BY hh SEPARATOR ','), ',', 1) system_flag_text,AVG(volume_multiplier) volume_multiplier,AVG(conversion_multiplier) conversion_multiplier,AVG(timeout_multiplier) timeout_multiplier,AVG(retry_multiplier) retry_multiplier FROM exogenous_state_timeline WHERE profile_id=%s AND dt BETWEEN %s AND %s GROUP BY profile_id,dt", (args.profile_id, args.dt_from, args.dt_to))
        exo_map = {(str(r['profile_id']), str(r['dt'])): r for r in cur.fetchall()}
        insert_cols = [c for c in ['profile_id','dt','daily_active_users','page_view_count',avg_col,'new_user_ratio','auth_attempt_count','auth_success_count','auth_fail_count','auth_success_rate','auth_fail_rate','otp_request_count','risk_login_count','loan_view_count','loan_apply_start_count','loan_apply_submit_count','card_apply_start_count','card_apply_submit_count','card_apply_submit_rate','collector_event_count','raw_event_count','estimated_missing_rate','validation_fail_count','validation_warn_count','drift_alert_count','drift_warn_count','ml_feature_alert_count','ml_feature_warn_count','target_risk_status','target_risk_score','run_id','note','target_risk_label','label_source','scenario_active_flag','scenario_name','scenario_type','campaign_flag','stream_missing_rate','stream_duplicate_ratio','stream_ordering_gap_score','stream_avg_event_delay_ms','stream_risk_score','stream_signal_count','weather_type','campaign_flag_text','system_flag_text','volume_multiplier','conversion_multiplier','timeout_multiplier','retry_multiplier'] if c in mlfv_cols]
        placeholders = ', '.join(['%s'] * len(insert_cols))
        insert_sql = f"REPLACE INTO ml_feature_vector_day ({', '.join(insert_cols)}) VALUES ({placeholders})"
        def find_run(dt):
            for r in runs:
                if str(r['dt_from']) <= dt <= str(r['dt_to']): return r
            return None
        for row in rows:
            dt = str(row['dt'])
            avgv = row.get('avg_session_duration_value') or row.get('avg_session_duration_value_alt')
            risk = risk_map.get((str(row['profile_id']), dt), {})
            score = safe_float(risk.get('agg_risk_score', 0))
            sc = find_run(dt); active = 1 if sc else 0
            scenario_name = sc.get('scenario_name') if sc else None
            scenario_type = sc.get('scenario_type') if sc else None
            intensity = sc.get('scenario_intensity') if sc else None
            campaign_flag = 1 if (scenario_name or '').lower() == 'campaign_spike' else 0
            label, status, label_source = scenario_label(scenario_name, intensity, score, active)
            stream = stream_map.get((str(row['profile_id']), dt), {})
            exo = exo_map.get((str(row['profile_id']), dt), {})
            vals = {'profile_id': row['profile_id'],'dt': row['dt'],'daily_active_users': safe_float(row.get('daily_active_users')),'page_view_count': safe_float(row.get('page_view_count')),avg_col: safe_float(avgv),'new_user_ratio': safe_float(row.get('new_user_ratio')),'auth_attempt_count': safe_float(row.get('auth_attempt_count')),'auth_success_count': safe_float(row.get('auth_success_count')),'auth_fail_count': safe_float(row.get('auth_fail_count')),'auth_success_rate': safe_float(row.get('auth_success_rate')),'auth_fail_rate': safe_float(row.get('auth_fail_rate')),'otp_request_count': safe_float(row.get('otp_request_count')),'risk_login_count': safe_float(row.get('risk_login_count')),'loan_view_count': safe_float(row.get('loan_view_count')),'loan_apply_start_count': safe_float(row.get('loan_apply_start_count')),'loan_apply_submit_count': safe_float(row.get('loan_apply_submit_count')),'card_apply_start_count': safe_float(row.get('card_apply_start_count')),'card_apply_submit_count': safe_float(row.get('card_apply_submit_count')),'card_apply_submit_rate': safe_float(row.get('card_apply_submit_rate')),'collector_event_count': safe_float(row.get('collector_event_count')),'raw_event_count': safe_float(row.get('raw_event_count')),'estimated_missing_rate': safe_float(row.get('estimated_missing_rate')),'validation_fail_count': safe_int(row.get('validation_fail_count')),'validation_warn_count': safe_int(row.get('validation_warn_count')),'drift_alert_count': safe_int(row.get('drift_alert_count')),'drift_warn_count': safe_int(row.get('drift_warn_count')),'ml_feature_alert_count': safe_int(row.get('ml_feature_alert_count')),'ml_feature_warn_count': safe_int(row.get('ml_feature_warn_count')),'target_risk_status': status,'target_risk_score': score,'run_id': f"mlfv_stream_{row['profile_id']}_{dt.replace('-', '')}",'note': f"scenario_active={active}; scenario={scenario_name}; intensity={intensity}",'target_risk_label': label,'label_source': label_source,'scenario_active_flag': active,'scenario_name': scenario_name,'scenario_type': scenario_type,'campaign_flag': campaign_flag,'stream_missing_rate': safe_float(stream.get('stream_missing_rate')),'stream_duplicate_ratio': safe_float(stream.get('stream_duplicate_ratio')),'stream_ordering_gap_score': safe_float(stream.get('stream_ordering_gap_score')),'stream_avg_event_delay_ms': safe_float(stream.get('stream_avg_event_delay_ms')),'stream_risk_score': safe_float(stream.get('stream_risk_score')),'stream_signal_count': safe_int(stream.get('stream_signal_count')),'weather_type': exo.get('weather_type'),'campaign_flag_text': exo.get('campaign_flag_text'),'system_flag_text': exo.get('system_flag_text'),'volume_multiplier': safe_float(exo.get('volume_multiplier')),'conversion_multiplier': safe_float(exo.get('conversion_multiplier')),'timeout_multiplier': safe_float(exo.get('timeout_multiplier')),'retry_multiplier': safe_float(exo.get('retry_multiplier'))}
            cur.execute(insert_sql, tuple(vals[c] for c in insert_cols))
    conn.commit()
    print(f"[OK] ml_feature_vector_builder_stream_v6 completed: rows={len(rows)}, avg_col={avg_col}, risk_score_col={risk_score_col}, risk_grade_col={risk_grade_col}")
