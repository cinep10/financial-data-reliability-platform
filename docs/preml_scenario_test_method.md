# Pre-ML 시나리오 테스트 방법

## 목적
ML로 넘어가기 전에 pre-ML 파이프라인이
- 외생변수 기반 시나리오
- batch + stream 영향
- validation / drift / anomaly / risk / root cause / action
까지 설명 가능하게 반응하는지 검증한다.

## 기본 원칙
1. baseline과 scenario를 분리한다.
2. scenario source of truth는 `scenario_plan` / `exogenous_state_timeline`이다.
3. old `scenario_injector.py` 흐름은 fallback/legacy 비교용으로만 둔다.
4. 기본 실험은 `run_unified_scenario_pipeline_v2.sh`와 `run_scenario_matrix_unified.sh`로 수행한다.

## 추천 테스트 절차

### 1. baseline 기간 선계산
- 2026-04-01 ~ 2026-04-30 baseline pre-ML 실행
- metric / validation / drift / risk / root cause 기준선 확보

### 2. unified scenario 주입
시나리오별로 `run_unified_scenario_pipeline_v2.sh` 실행
- weather_drop
- campaign_spike
- auth_failure
- collector_drop
- funnel_break
- degraded
- salary_day
- tax_season

### 3. 결과 검증 포인트
- exogenous_state_timeline 생성 여부
- stream adapter injection 여부
- batch metric adapter 반영 여부
- validation 결과 수와 fail/warn 비율 변화
- drift row 증가 여부
- risk v4 score / grade 변화
- root cause에 scenario 관련 cause 반영 여부
- action engine 추천 액션 변화 여부

### 4. summary 비교
`scenario_experiment_runner.py` 결과로 baseline 대비 scenario 차이 정리

## 권장 운영 모드
- pre-ML 확정 전까지는 `RUN_ML=false`
- scenario matrix는 unified runner 기준으로 수행
- stream 전용 검증은 기존 `run_streaming_kafka_pipeline.sh`로 별도 재검증

## 테스트 산출물
- scenario_plan
- exogenous_state_timeline
- scenario_injection_log
- scenario_adapter_result_log
- validation_result
- metric_drift_result_r
- data_risk_score_day_v3
- data_risk_root_cause_day
- data_reliability_action_day
- scenario_experiment_summary_day
