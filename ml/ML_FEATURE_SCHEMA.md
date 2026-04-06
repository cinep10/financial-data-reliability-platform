# ML Feature Schema (v2)

## 목적
이 문서는 Data Reliability 프로젝트의 ML 레이어에서 사용하는 일별 feature schema를 고정한다.

핵심 원칙:
- feature build, training, prediction, explainability, drift analysis는 동일한 feature schema를 사용한다.
- label은 `data_risk_score_day_v3` 기준으로 생성한다.
- explainability는 prediction 결과와 연결된다.

## Granularity
- 단위: `profile_id + dt` (일별)
- source table: `metric_value_day`, `validation_summary_day`, `metric_drift_result_r`, `mapping_coverage_day`, `data_risk_score_day_v3`, `risk_signal_link_day`, `ml_feature_drift_result`

## Label Rule
- primary label: `target_risk_label`
- 생성 규칙:
  - `target_risk_grade = 'high'` 이면 1
  - 아니면 0
- fallback:
  - `final_risk_score >= 0.70` 이면 1

## Fixed Feature Columns
| feature_name | type | source | 의미 |
|---|---|---|---|
| daily_active_users | numeric | metric_value_day | 일 활성 사용자 수 |
| page_view_count | numeric | metric_value_day | 페이지뷰 수 |
| avg_session_duration_sec | numeric | metric_value_day | 평균 세션 길이 |
| new_user_ratio | numeric | metric_value_day | 신규 유저 비율 |
| auth_attempt_count | numeric | metric_value_day | 인증 시도 수 |
| auth_success_count | numeric | metric_value_day | 인증 성공 수 |
| auth_fail_count | numeric | metric_value_day | 인증 실패 수 |
| auth_success_rate | numeric | metric_value_day | 인증 성공률 |
| auth_fail_rate | numeric | metric_value_day | 인증 실패율 |
| otp_request_count | numeric | metric_value_day | OTP 요청 수 |
| risk_login_count | numeric | metric_value_day | 위험 로그인 수 |
| loan_view_count | numeric | metric_value_day | 대출 조회 수 |
| loan_apply_start_count | numeric | metric_value_day | 대출 신청 시작 수 |
| loan_apply_submit_count | numeric | metric_value_day | 대출 신청 제출 수 |
| loan_funnel_conversion | numeric | metric_value_day | 대출 퍼널 전환율 |
| card_apply_start_count | numeric | metric_value_day | 카드 신청 시작 수 |
| card_apply_submit_count | numeric | metric_value_day | 카드 신청 제출 수 |
| card_apply_submit_rate | numeric | metric_value_day | 카드 제출률 |
| card_funnel_conversion | numeric | metric_value_day | 카드 퍼널 전환율 |
| submit_capture_rate | numeric | metric_value_day | transfer step1 대비 confirm 비율 |
| success_outcome_capture_rate | numeric | metric_value_day | auth attempt 대비 loan submit 비율 |
| collector_event_count | numeric | metric_value_day | collector 이벤트 수 |
| raw_event_count | numeric | metric_value_day | raw 이벤트 수 |
| estimated_missing_rate | numeric | metric_value_day | 추정 missing 비율 |
| mapping_coverage | numeric | mapping_coverage_day | 이벤트 매핑 커버리지 |
| validation_fail_count | int | validation_summary_day | validation fail 수 |
| validation_warn_count | int | validation_summary_day | validation warn 수 |
| drift_alert_count | int | metric_drift_result_r | drift alert 개수 |
| drift_warn_count | int | metric_drift_result_r | drift warn 개수 |
| anomaly_alert_count | int | metric_drift_result_r | conversion/success 계열 alert 개수 |
| anomaly_warn_count | int | metric_drift_result_r | conversion/success 계열 warn 개수 |
| ml_feature_alert_count | int | ml_feature_drift_result | ML feature drift alert 개수 |
| ml_feature_warn_count | int | ml_feature_drift_result | ML feature drift warn 개수 |
| total_signal_count | int | risk_signal_link_day | signal linkage 총 개수 |

## Excluded / Removed
다음은 불필요하거나 중복이라 v2 기본 schema에서 제외 가능:
- raw duplicated labels from old risk tables
- legacy counts that are not stable across train/predict

## Null Handling
- numeric feature: `0`으로 대체
- ratio feature: `0`으로 대체
- count feature: `0`으로 대체
- label source missing:
  - `target_risk_grade` 없으면 `final_risk_score`로 fallback

## Model Metadata Requirements
학습 결과물은 아래를 반드시 기록한다.
- model_version
- feature_schema_version
- feature_columns_requested
- feature_columns_used
- label_rule
- train/test date range
- train/test row counts
- evaluation metrics

## Prediction Output Requirements
`ml_prediction_result`는 최소 아래를 포함한다.
- predicted_label
- predicted_risk_status
- prob_alert
- actual_risk_status
- actual_risk_score
- top_reason_1
- top_reason_2
- top_reason_3
- feature_schema_version
- model_version

## Explainability Rule
- LogisticRegression coefficient + transformed feature contribution 기준 top reason 3개 생성
- DB에 적재된 `ml_feature_importance`는 전역 importance 용도
- prediction row의 `top_reason_*`는 개별 예측 설명 용도

## Feature Drift Integration
- `ml_feature_drift_result`: feature별 drift 상세
- `ml_feature_drift_day`: 일별 summary + top drift feature
- prediction과 key는 `profile_id + dt`
