# Root Cause → Action Playbook

이 문서는 root cause가 탐지되었을 때 운영자가 어떤 액션을 취해야 하는지 정의한다.

---

## 1. traffic_drop

### 의미
- page_view_count
- daily_active_users
- raw_event_count
- collector_event_count
가 baseline 대비 하락

### 가능한 원인
- 자연 트래픽 감소
- holiday / weekend effect
- campaign 종료
- upstream ingestion loss

### 액션
1. scenario metadata 확인
2. raw_event_count vs collector_event_count 비교
3. missing_rate 확인
4. 동일 기간 baseline 재검토

---

## 2. traffic_spike

### 의미
트래픽 유입 급증

### 가능한 원인
- campaign
- bot / abnormal traffic
- external referral 증가

### 액션
1. ref_host / campaign tag 확인
2. page_type 분포 확인
3. 신규 사용자 비율 확인
4. bot filtering 필요 여부 검토

---

## 3. mapping_gap

### 의미
attempt/start는 있으나 success/submit outcome 부족

### 가능한 원인
- event mapping 미완성
- parser rule 누락
- collector transform 누락

### 액션
1. metric_event_mapping.yaml 점검
2. collector_a / analyzer_b rule 확인
3. raw kv(evt, page_type) 샘플 확인
4. validation rule와 funnel 정의 재검증

---

## 4. funnel_distortion

### 의미
view → start → submit 구조 붕괴

### 가능한 원인
- 실제 사용자 행동 변화
- submit 이벤트 미수집
- 특정 stage 과대 분류

### 액션
1. funnel definition 기준 비교
2. mapping coverage 확인
3. correlation anomaly 확인
4. submit / success 관련 raw event 샘플 점검

---

## 5. quality_shift

### 의미
estimated_missing_rate 상승, raw/collector 차이 확대

### 가능한 원인
- loader 누락
- collector drop
- parse failure
- schema drift

### 액션
1. raw load rowcount 확인
2. collector rowcount 확인
3. parser regex / kv 파싱 확인
4. source log sample 재확인

---

## 6. metric_drift

### 의미
개별 metric 분포 변화

### 가능한 원인
- 자연 seasonal 변화
- product usage 변화
- 이벤트 분류 변경

### 액션
1. traffic / coverage / correlation과 함께 해석
2. 단독 metric인지 군집 변화인지 확인
3. weekday + hour baseline 재검토

---

## 7. 운영 우선순위

### 즉시 확인
- quality_shift
- mapping_gap
- submit capture collapse
- auth outcome missing

### 모니터링 지속
- traffic_spike
- traffic_drop
- mild drift
