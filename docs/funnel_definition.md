# Funnel Definition

이 문서는 funnel distortion, correlation anomaly, mapping coverage 해석의 기준이 되는 funnel 구조를 정의한다.

---

## 1. Funnel 설계 원칙

- funnel은 상위 단계 >= 하위 단계 관계를 가진다.
- stage 간 역전은 이상 신호다.
- 단, 샘플링/매핑 누락이 있을 수 있으므로 validation과 mapping coverage를 함께 본다.

---

## 2. Auth Funnel

### Stage
1. auth_attempt
2. auth_success or auth_fail

### 기대 관계
- auth_attempt_count >= auth_success_count
- auth_attempt_count >= auth_fail_count
- auth_success_count + auth_fail_count <= auth_attempt_count

### 주요 이상
- attempt는 있는데 success/fail 둘 다 0
- success가 attempt보다 큼
- fail 비율 급증

### 해석
- mapping gap
- auth logging 누락
- 인증 플로우 변화
- 시스템 지연/오류

---

## 3. Loan Funnel

### Stage
1. loan_view
2. loan_apply_start
3. loan_apply_submit

### 기대 관계
- loan_view_count >= loan_apply_start_count
- loan_apply_start_count >= loan_apply_submit_count

### 주요 이상
- start > view
- submit > start
- submit 거의 0

### 해석
- submit mapping 누락
- start 이벤트 과다 분류
- 상단 funnel 대비 하단 capture 부족

---

## 4. Card Funnel

### Stage
1. card_apply_start
2. card_apply_submit

### 기대 관계
- card_apply_start_count >= card_apply_submit_count

### 주요 이상
- submit > start
- start는 많은데 submit 없음

### 해석
- submit capture 부족
- 이벤트 네이밍/매핑 누락
- downstream 수집 실패

---

## 5. Funnel 관련 coverage 지표

### auth
- mapping_coverage_auth
- success_outcome_capture_rate

### loan
- mapping_coverage_loan
- submit_capture_rate

### card
- mapping_coverage_card
- submit_capture_rate

---

## 6. Funnel distortion 판단 기준

### warn
- coverage < 0.8
- stage ratio 급변
- 상하위 관계 약화

### fail / alert
- 하위 stage > 상위 stage
- submit / success outcome 거의 전부 소실
- baseline 대비 구조 급변

---

## 7. 운영 해석 규칙

- validation warn + low coverage
  → mapping issue 가능성 높음

- drift alert + funnel ratio change
  → behavior change or funnel distortion

- correlation anomaly + low coverage
  → 구조적 logging 문제 가능성 높음
