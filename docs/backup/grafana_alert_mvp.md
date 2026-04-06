# Grafana Alert MVP

## 목적

Risk Score를 기반으로 데이터 품질 이슈를 운영 알림으로 연결한다.

## 추천 Alert Rule

### Rule 1. High Data Risk
- Query: `data_risk_score_day.risk_score`
- Condition: `last() >= 6`
- Severity: critical

### Rule 2. Validation Failure Exists
- Query: `data_risk_score_day.validation_fail_count`
- Condition: `last() > 0`
- Severity: warning

### Rule 3. Drift Alert Exists
- Query: `data_risk_score_day.drift_alert_count`
- Condition: `last() > 0`
- Severity: warning

## MVP 판단
포트폴리오 관점에서는 Slack 연동까지 꼭 갈 필요는 없다.
우선 Grafana Alert Rule 정의와 스크린샷만 있어도 충분히 설명력이 있다.
