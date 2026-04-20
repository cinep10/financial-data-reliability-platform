현재 스키마 기준으로 재작성한 버전입니다.

입력 source:
- stg_wc_log_hit

출력:
- event_log_raw
- 옵션: Kafka publish

포인트:
- 현재 stg_wc_log_hit 스키마에는 pcid, sid, evt, page_type, device_type, latency_ms, accept_lang, cc 컬럼이 없으므로 kv_raw, path, ua 에서 추론합니다.
- 즉, 단순히 테이블명만 바꾸는 것이 아니라 SELECT / payload / 추론 로직까지 같이 바꿔야 합니다.

실행 예시:
python3 pipelines/source_event/event_log_raw_builder_final.py \
  --db-host 127.0.0.1 --db-port 3306 \
  --db-user nethru --db-pass 'nethru1234' \
  --db-name weblog \
  --profile-id finance_bank \
  --dt-from 2026-04-07 --dt-to 2026-04-07 \
  --truncate-target

Kafka까지 같이:
python3 pipelines/source_event/event_log_raw_builder_final.py \
  --db-host 127.0.0.1 --db-port 3306 \
  --db-user nethru --db-pass 'nethru1234' \
  --db-name weblog \
  --profile-id finance_bank \
  --dt-from 2026-04-07 --dt-to 2026-04-07 \
  --truncate-target \
  --publish-kafka \
  --kafka-bootstrap 127.0.0.1:9092
