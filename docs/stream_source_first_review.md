스트림 구조 검토 요약

1. 현재 unified scenario pipeline은 pre-ML 쪽에서 `stream_injection_adapter.py`를 먼저 호출합니다. fileciteturn39file3
2. 동시에 실제 스트리밍 실행은 별도 wrapper에서 `kafka_producer_from_source_events_v3.py -> kafka_consumer_to_stg_event_stream_v2.py -> stream_*` 순서로 돌고 있습니다. fileciteturn39file2turn39file4
3. 이때 producer의 기본 source는 `stg_webserver_log_hit`입니다. baseline matrix도 `STREAM_SOURCE_TABLE` 기본값이 `stg_webserver_log_hit`입니다. fileciteturn38file0turn38file10
4. 그러나 source-first 원칙에 맞는 producer는 이미 따로 있습니다. `kafka_producer_from_event_log_raw.py`는 `event_log_raw`에서 읽어 Kafka payload를 만들고 전송합니다. fileciteturn39file19
5. consumer는 Kafka payload를 `stg_event_stream`에 적재하고, `source_type/path/evt`까지 받도록 설계돼 있으므로 구조 자체는 source-first 흐름과 맞습니다. fileciteturn37file16turn39file17
6. stream anomaly simulator는 `stg_event_stream`에서 missing/duplicate/ordering/latency를 주입합니다. 즉 Kafka 이후, consumer 이후 계층에서 쓰는 것이 맞습니다. fileciteturn37file11

결론

- 스트림 구조의 큰 틀은 맞지만, 현재 wrapper 일부가 `stg_webserver_log_hit -> Kafka`로 가고 있어 source-first 목적과 어긋납니다.
- 따라서 source-first 버전에서는 `event_log_raw -> Kafka -> stg_event_stream -> stream reliability`로 고정하는 것이 맞습니다.
- 시나리오의 스트림 어댑터는 pre-consumer data mutation이 아니라, post-consumer anomaly plan 기록용으로 바꾸는 것이 더 자연스럽습니다.
