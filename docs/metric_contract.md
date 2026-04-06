# Metric Contract

이 문서는 Data Reliability Platform에서 사용하는 핵심 metric의 의미를 고정하기 위한 계약 문서다.  
목적은 drift, validation, risk 해석의 기준을 흔들리지 않게 유지하는 것이다.

---

## 1. 공통 원칙

- 모든 metric은 `profile_id + dt (+ hh) + metric_name` 기준으로 식별한다.
- hourly metric은 `metric_value_hh`
- daily metric은 `metric_value_day`
- metric_value는 최종 집계값
- numerator_value, denominator_value는 rate 계열 metric의 분자/분모를 저장한다.
- source_layer는 metric이 계산된 계층을 의미한다.
  - raw
  - collector
  - metric_layer
  - control
  - ml_feature

---

## 2. Traffic / Activity Metrics

### daily_active_users
- 의미: 해당 기간 동안 활동한 고유 사용자 수
- 계산 기준:
  - identity_mode 기준 사용자 키 사용
  - hourly: 해당 시간대 활동 사용자 수
  - daily: 하루 전체 활동 사용자 수
- 주의:
  - uid / pcid / ip 전략에 따라 값이 달라질 수 있음

### page_view_count
- 의미: page view로 해석 가능한 이벤트 수
- 계산 기준:
  - analyzer에서 `is_pageview(...) == True` 인 이벤트 수
- 제외:
  - static resource
  - invalid status / invalid method
- 주의:
  - 클릭/스크롤 이벤트와 혼동 금지

### avg_session_duration_sec
- 의미: 세션 지속시간 평균
- 계산 기준:
  - 동일 identity 기준 세션 timeout 내 이벤트 묶음
- 주의:
  - session 정의 변경 시 직접 비교 불가

### new_user_ratio
- 의미: 신규 사용자 비율
- 계산 기준:
  - target user 중 lookback window 이전에 없던 사용자 비율
- 분자:
  - new_user_count
- 분모:
  - target_user_count

---

## 3. Auth Metrics

### auth_attempt_count
- 의미: 인증 시도 이벤트 수
- 포함:
  - auth_attempt
  - auth_success
  - auth_fail
- 주의:
  - success/fail은 attempt의 하위 outcome으로 해석한다

### auth_success_count
- 의미: 인증 성공 이벤트 수

### auth_fail_count
- 의미: 인증 실패 이벤트 수

### auth_success_rate
- 의미: 인증 성공률
- 공식:
  - auth_success_count / auth_attempt_count

### auth_fail_rate
- 의미: 인증 실패율
- 공식:
  - auth_fail_count / auth_attempt_count

### otp_request_count
- 의미: OTP 요청 이벤트 수

---

## 4. Loan Funnel Metrics

### loan_view_count
- 의미: 대출 상품/대출 관련 페이지 조회 수

### loan_apply_start_count
- 의미: 대출 신청 시작 이벤트 수

### loan_apply_submit_count
- 의미: 대출 신청 제출 완료 이벤트 수

### loan_apply_submit_rate
- 의미: 대출 신청 제출 전환율
- 공식:
  - loan_apply_submit_count / loan_apply_start_count

---

## 5. Card Funnel Metrics

### card_apply_start_count
- 의미: 카드 신청 시작 이벤트 수

### card_apply_submit_count
- 의미: 카드 신청 제출 완료 이벤트 수

### card_apply_submit_rate
- 의미: 카드 신청 제출 전환율
- 공식:
  - card_apply_submit_count / card_apply_start_count

---

## 6. Raw / Quality Metrics

### raw_event_count
- 의미: raw/stage 테이블 기준 이벤트 수

### collector_event_count
- 의미: collector 계층 기준 이벤트 수

### estimated_missing_rate
- 의미: 누락 추정 비율
- 해석:
  - raw → collector → metric 흐름 중 손실 가능성 추정치
- 주의:
  - 정확한 missing count가 아니라 control metric 성격

---

## 7. Mapping Coverage Metrics

### mapping_coverage_auth
- 의미: auth outcome 매핑 커버리지
- 공식:
  - (auth_success_count + auth_fail_count) / auth_attempt_count

### mapping_coverage_loan
- 의미: loan funnel capture coverage
- 공식:
  - loan_apply_submit_count / loan_apply_start_count

### mapping_coverage_card
- 의미: card funnel capture coverage
- 공식:
  - card_apply_submit_count / card_apply_start_count

### success_outcome_capture_rate
- 의미: 성공 outcome이 시도 대비 얼마나 잘 잡히는지

### submit_capture_rate
- 의미: 신청 시작 대비 submit이 얼마나 잘 잡히는지

---

## 8. 운영 원칙

- metric 정의가 바뀌면 drift baseline을 재생성해야 한다.
- funnel stage가 추가되면 validation rule도 같이 수정한다.
- mapping coverage metric은 validation 이전에 계산해 quality control 지표로 사용한다.
