핵심 결론

1. 현재 unified scenario pipeline은 `stream_injection_adapter.py`와 `batch_metric_adapter.py`를 pre-ML 재실행 전에 호출합니다. fileciteturn35file10
2. 그런데 `scenario_injector.py`는 실제로 `metric_value_day`를 직접 UPDATE 합니다. 즉 시나리오가 원천로그를 거치지 않고 metric layer를 바로 바꿉니다. fileciteturn36file6
3. 동시에 `analyzer_b_v4.py`는 대상 날짜의 `metric_value_hh`와 `metric_value_day`를 삭제한 뒤 다시 계산합니다. 따라서 direct metric mutation은 source-first 구조에도 맞지 않고, analyzer 재실행 시 덮어써질 수도 있습니다. fileciteturn36file11
4. 실제 로그에서도 batch adapter가 먼저 돌고, 그 뒤 pre-ML이 다시 parse/load/collector/analyzer를 수행합니다. fileciteturn35file7turn35file10

정리 방향

- batch 시나리오는 `scenario -> exogenous_state_timeline -> weblog generator -> stg_webserver_log_hit -> collector -> analyzer -> metric_value_*`
- stream 시나리오는 `scenario -> stream_injection_adapter -> stg_event_stream / stream_*`
- 즉 batch adapter는 metric 직접 업데이트가 아니라 "원천에 영향을 줄 준비가 됨"만 기록해야 함

적용 파일
- pipelines/scenario/batch_metric_adapter_v2.py
- deploy/run_unified_scenario_pipeline_source_first_v1.sh
