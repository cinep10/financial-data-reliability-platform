# 9-2 ML input reliability (range-capable)

포함 파일
- ml_feature_drift_analyzer.py
- risk_score_runner_v3.py
- grafana_ml_feature_reliability_dashboard.json
- grafana_ml_feature_reliability_queries.sql

실행 예시

## 1) ML feature drift 단일 날짜
python3 ml_feature_drift_analyzer.py \
  --host 127.0.0.1 --port 3306 \
  --user nethru --password nethru1234 \
  --db weblog \
  --profile-id finance_bank \
  --date 2026-03-09 \
  --baseline-days 28

## 2) ML feature drift 기간 범위
python3 ml_feature_drift_analyzer.py \
  --host 127.0.0.1 --port 3306 \
  --user nethru --password nethru1234 \
  --db weblog \
  --profile-id finance_bank \
  --dt-from 2026-02-23 \
  --dt-to 2026-03-09 \
  --baseline-days 28 \
  --truncate

## 3) risk score v3 단일 날짜
python3 risk_score_runner_v3.py \
  --host 127.0.0.1 --port 3306 \
  --user nethru --password nethru1234 \
  --db weblog \
  --profile-id finance_bank \
  --date 2026-03-09

## 4) risk score v3 기간 범위
python3 risk_score_runner_v3.py \
  --host 127.0.0.1 --port 3306 \
  --user nethru --password nethru1234 \
  --db weblog \
  --profile-id finance_bank \
  --dt-from 2026-02-23 \
  --dt-to 2026-03-09 \
  --truncate
