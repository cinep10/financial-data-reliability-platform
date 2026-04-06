생성 파일

1. sql/07_reliability_control_tables.sql
- root cause
- risk contribution
- rolling time anomaly
- metric pair correlation anomaly

2. pipelines/root_cause_and_contribution_runner.py
- data_risk_score_day_v3 + validation_result + metric_drift_result_r 기준
- data_risk_root_cause_day 생성
- risk_signal_link_day 생성

3. pipelines/time_pattern_anomaly_runner.py
- metric_value_day 기준 rolling 7일 anomaly 생성
- metric_time_anomaly_day 생성

4. pipelines/correlation_anomaly_runner.py
- metric_value_day 기준 metric pair ratio anomaly 생성
- metric_correlation_anomaly_day 생성

권장 실행 순서

1) 테이블 생성
mysql -u USER -p DB < sql/07_reliability_control_tables.sql

2) root cause / contribution
python3 pipelines/root_cause_and_contribution_runner.py \
  --host 127.0.0.1 --port 3306 \
  --user nethru --password nethru1234 \
  --db weblog \
  --profile-id finance_bank \
  --dt-from 2026-02-23 \
  --dt-to 2026-03-09 \
  --truncate

3) rolling anomaly
python3 pipelines/time_pattern_anomaly_runner.py \
  --host 127.0.0.1 --port 3306 \
  --user nethru --password nethru1234 \
  --db weblog \
  --profile-id finance_bank \
  --dt-from 2026-02-23 \
  --dt-to 2026-03-09 \
  --truncate

4) correlation anomaly
python3 pipelines/correlation_anomaly_runner.py \
  --host 127.0.0.1 --port 3306 \
  --user nethru --password nethru1234 \
  --db weblog \
  --profile-id finance_bank \
  --dt-from 2026-02-23 \
  --dt-to 2026-03-09 \
  --truncate

추천 Grafana 패널
- Root Cause Top 5 by day
- Risk contribution by signal group
- Time anomaly detail
- Correlation anomaly detail
