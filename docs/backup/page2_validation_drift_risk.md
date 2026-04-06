# Validation · Drift · Risk Scoring 구조 상세

## 핵심 설계 원칙

이 프로젝트에서 가장 중요한 설계 원칙은 아래 두 가지입니다.

1. **지표가 먼저 믿을 수 있어야 한다**
2. **이상 탐지는 단일 룰이 아니라 레이어드 통제로 해야 한다**

즉, 순서는 항상 다음과 같습니다.

```text
Raw Log
→ Metric
→ Validation
→ Drift
→ Risk Score
→ Dashboard
```

---

## Validation Layer

Validation Layer는 Drift 이전 단계에서 **지표의 논리적 정합성**을 검증합니다.

### Validation Rule Categories

#### 1. Cross-system validation
시스템 간 수량 관계 검증

예시:

- `raw_event_count >= collector_event_count`
- `collector_event_count >= page_view_count`

#### 2. Funnel validation
업무 흐름 상 앞단 수량보다 뒷단 수량이 많아지지 않는지 검증

예시:

- `auth_success_count <= auth_attempt_count`
- `loan_apply_submit_count <= loan_apply_start_count`
- `card_apply_submit_count <= card_apply_start_count`

#### 3. Ratio bounds
비율값이 [0,1] 범위를 벗어나지 않는지 검증

예시:

- `auth_success_rate`
- `auth_fail_rate`
- `card_apply_submit_rate`
- `estimated_missing_rate`

#### 4. Completeness validation
시간 단위 지표가 누락 없이 생성되었는지 검증

예시:

- 24시간 hourly metric completeness

#### 5. Mapping quality validation
이벤트 매핑이 불완전해 보이는 패턴을 검출

예시:

- auth_attempt 존재하지만 success/fail 모두 0
- loan start 존재하지만 submit 0
- card start 존재하지만 submit 0
- otp_request 존재하지만 auth_attempt 0

---

## Validation 결과 모델

### validation_result
시간 단위 규칙 실행 결과 저장

주요 컬럼 예시:

- profile_id
- dt
- hh
- rule_name
- rule_group
- metric_name
- observed_value
- expected_value
- validation_status
- severity
- note

### validation_summary_day
일 단위 요약 결과 저장

주요 컬럼 예시:

- total_rules
- pass_count
- warn_count
- fail_count
- highest_severity

---

## Drift Detection Layer

Validation 이후에는 **통계적 이상 탐지**를 수행합니다.

### Baseline 설계

초기에는 단순 7일 평균 baseline을 고려했지만, 운영 트래픽은 요일과 시간대 영향을 크게 받기 때문에 다음 방식으로 개선했습니다.

```text
weekday + hour baseline
```

예를 들어 월요일 10시는 과거 월요일 10시들과 비교합니다.

---

## Drift Methods

### 1. Z-score
비율/카운트 기반 지표의 급격한 변화를 탐지

적용 예시:

- auth_success_count
- auth_fail_count
- otp_request_count
- risk_login_count
- estimated_missing_rate

### 2. PSI-like drift
분포 혹은 규모 변화 탐지

적용 예시:

- loan_view_count
- loan_apply_start_count
- loan_apply_submit_count
- card_apply_start_count
- card_apply_submit_count

### 3. Funnel conversion change
전환율 자체의 baseline shift 탐지

적용 예시:

- loan_funnel_conversion
- card_funnel_conversion

---

## Drift 결과 모델

### metric_drift_result
운영용 drift 결과 테이블

### metric_drift_result_r
R 기반 drift 분석 결과 테이블

대표 컬럼:

- profile_id
- dt
- hh
- metric_name
- metric_group
- source_layer
- baseline_value
- observed_value
- drift_score
- drift_method
- drift_status
- severity
- detail
- run_id

---

## Risk Scoring Layer

Validation과 Drift는 서로 다른 성격의 품질 신호입니다.  
운영 관점에서는 이를 하나의 점수로 통합해 보는 것이 더 실용적입니다.

### Risk Score Formula

```text
risk_score =
5 * validation_fail_count
+ 2 * validation_warn_count
+ 3 * drift_alert_count
+ 1 * drift_warn_count
```

### Risk Status

- 0 ~ 2: normal
- 3 ~ 5: warning
- 6 이상: alert

---

## 왜 점수화가 필요한가

운영자는 개별 rule 수백 개를 모두 보지 않습니다.  
대신 “오늘 데이터 상태가 정상인지”를 빠르게 판단할 수 있어야 합니다.

Risk Score Layer를 두면서 얻은 장점:

- Validation / Drift 결과를 단일 운영 지표로 통합
- Alerting 조건을 단순화
- Dashboard와 Incident 대응 기준을 명확히 설정 가능

---

## Monitoring으로 닫히는 구조

Grafana는 단순 시각화 도구가 아니라, 이 아키텍처의 마지막 제어면(Control Surface) 역할을 합니다.

대표 시각화:

- Daily Risk Score
- Validation Summary by Day
- Drift Alert / Warn Count
- Hourly Event Volume
- Auth Funnel
- Business Funnel

즉, 이 프로젝트는 다음 문장으로 요약할 수 있습니다.

> Validation ensures metrics are trustworthy, drift detection identifies abnormal changes, and risk scoring converts those signals into an operational control metric.

---

## 포트폴리오 관점에서의 의미

이 구조는 단순 분석이 아니라 다음 역량을 보여줍니다.

- 데이터 품질 프레임워크 설계
- 운영 지표 semantic modeling
- 통계적 이상 탐지 설계
- 운영 리스크 점수화
- 데이터 관측 가능성(Observability) 설계

그래서 이 프로젝트는 단순 ETL 포트폴리오보다 한 단계 높은 **Data Reliability Architecture 포트폴리오**로 포지셔닝할 수 있습니다.
