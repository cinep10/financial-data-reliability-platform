# finance_bank 기준 realistic event mapping + metric mapping

아래 내용은 `finance_bank.yaml` 프로필 기준으로 **로그 → event → metric → validation → drift → risk_score** 흐름이 안정되도록 맞춘 매핑 가이드다.

## 1. 설계 목적

핵심은 raw web log에 `event_name` 컬럼이 없어도 다음 정보를 이용해 의미 있는 이벤트로 분류하는 것이다.

- `path`
- `url_norm`
- `query`
- `kv_raw`
- `uid`

즉, analyzer/collector가 아래처럼 semantic event를 추론하도록 맞춘다.

```text
/auth/login              -> auth_attempt
/auth/success            -> auth_success
/auth/fail               -> auth_fail
/auth/otp                -> otp_request
/loan/apply              -> loan_apply_start
/loan/apply/submit       -> loan_apply_submit
/card/apply              -> card_apply_start
/card/apply/submit       -> card_apply_submit
```

## 2. metric group 기준 정리

### user_activity
- daily_active_users
- login_success_count
- new_user_ratio
- page_view_count
- avg_session_duration_sec

### auth_security
- auth_attempt_count
- auth_success_count
- auth_fail_count
- auth_success_rate
- auth_fail_rate
- otp_request_count
- risk_login_count

### financial_service
- loan_view_count
- loan_apply_start_count
- loan_apply_submit_count
- card_apply_start_count
- card_apply_submit_count
- card_apply_submit_rate

### system_operation
- raw_event_count
- collector_event_count
- estimated_missing_rate
- schema_change_count
- batch_delay_sec

## 3. event mapping 원칙

### 인증 계열 우선순위
`auth_success / auth_fail > auth_attempt`

즉 `/auth/login` 이면서 `result=success`가 있으면 `auth_attempt`가 아니라 `auth_success`로 본다.

### 신청 계열 우선순위
`loan_apply_submit > loan_apply_start`
`card_apply_submit > card_apply_start`

즉 submit path나 submit kv가 보이면 submit으로 본다.

### page view
정적 파일(css, js, png 등)은 page view에서 제외한다.

## 4. new_user_ratio 개선 기준

기존 단순 `first_seen = dt` 방식은 baseline이 짧을 때 왜곡이 크다.

개선 기준:
```text
lookback window = 30 days
```

공식:
```text
new_user_ratio = new_users_30d / daily_active_users
```

## 5. analyzer에 바로 반영할 핵심 로직

### infer_event_name 우선순위
1. card_apply_submit
2. card_apply_start
3. loan_apply_submit
4. loan_apply_start
5. auth_success
6. auth_fail
7. otp_request
8. risk_login
9. auth_attempt
10. loan_view
11. generic page view

### hourly metrics
- raw_event_count
- collector_event_count
- page_view_count
- auth_attempt_count
- auth_success_count
- auth_fail_count
- otp_request_count
- risk_login_count
- loan_view_count
- loan_apply_start_count
- loan_apply_submit_count
- card_apply_start_count
- card_apply_submit_count
- auth_success_rate
- auth_fail_rate
- card_apply_submit_rate
- estimated_missing_rate

### daily metrics
- daily_active_users
- login_success_count
- new_user_ratio
- avg_session_duration_sec
- schema_change_count
- batch_delay_sec

## 6. validation과 연결되는 포인트

이 매핑이 잘 들어가면 아래 validation이 의미 있게 동작한다.

- `raw_event_count >= collector_event_count`
- `collector_event_count >= page_view_count`
- `auth_success_count <= auth_attempt_count`
- `auth_fail_count <= auth_attempt_count`
- `loan_apply_submit_count <= loan_apply_start_count`
- `card_apply_submit_count <= card_apply_start_count`
- `otp_request_count > 0 implies auth_attempt_count > 0`

특히 아래는 mapping quality check의 핵심이다.

- auth_attempt는 있는데 auth_success/auth_fail이 계속 0
- loan_apply_start는 있는데 loan_apply_submit이 계속 0
- card_apply_start는 있는데 card_apply_submit이 계속 0

이 패턴이 계속 나오면 실제 이상이 아니라 **event mapping 룰 부족**일 가능성이 높다.

## 7. drift와 연결되는 포인트

mapping이 안정되면 drift는 아래 기준으로 보는 것이 맞다.

### z-score
- auth_success_count
- auth_fail_count
- otp_request_count
- risk_login_count
- estimated_missing_rate

### psi-like
- loan_view_count
- loan_apply_start_count
- loan_apply_submit_count
- card_apply_start_count
- card_apply_submit_count

### funnel drift
- loan_funnel_conversion
- card_funnel_conversion

baseline은 `weekday + hour` 기준 유지가 적절하다.

## 8. 지금 바로 적용 추천

### A. config 반영
- `configs/profiles/finance_bank.yaml`은 트래픽 시뮬레이션 프로필로 유지
- 새 매핑 파일은 `configs/finance_bank_metric_mapping.yaml`로 별도 관리

### B. analyzer 반영
- `infer_event_name()` 함수에 우선순위 규칙 반영
- `card_apply_submit_count` 직접 저장
- `auth_success_count`, `auth_fail_count` 직접 저장

### C. validation 반영
- rate fallback보다 count metric 우선 사용
- mapping quality 규칙 유지

이렇게 가면 현재 구조에서 가장 중요한 안정화 포인트인 **semantic mapping**이 정리된다.
