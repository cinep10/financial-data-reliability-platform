# ML + Streaming integration

## 단일 시나리오 결과 해석
- pre-ML risk score는 `0.0733`, grade는 `low`였습니다.
- streaming dashboard에서 latest stream risk score는 `39.1`이었고, latest missing rate는 `0.978` 수준이었습니다.
- bridge 이후 recommended stream action은 `consumer_check`로 보였고, root cause bridge에 `stream_missing` 계열이 반영되었습니다.

즉, batch/pre-ML과 stream 결과가 같은 날짜에 함께 적재되는 구조는 확인되었습니다.
다만 이번 테스트에서는 source log 재사용(`RUN_SIMULATION=false`) 때문에 stream 입력 건수는 617건으로 적었고, missing rate가 과도하게 높게 나타났습니다. 이는 현재 비교 기준(expected count)과 stream source 범위가 완전히 맞지 않기 때문으로 보입니다.

## 기존 ML 코드 리뷰 요약
기존 `ml_feature_vector_builder.py`는 주로 아래만 반영합니다.
- metric_value_day
- validation_summary_day
- metric_drift_result_r
- metric_time_anomaly_day
- metric_correlation_anomaly_day
- data_risk_score_day_v3
- scenario_experiment_run

즉, stream 계열과 exogenous_state_timeline 계열 feature는 빠져 있습니다.
기존 `ml_risk_model_train.py`, `ml_prediction_runner.py`도 `ml_feature_vector_day` 안의 batch 중심 feature만 사용합니다.

## 이번 버전에서 추가할 것
### builder
- stream_missing_rate
- stream_duplicate_ratio
- stream_ordering_gap_score
- stream_avg_event_delay_ms
- stream_risk_score
- stream_signal_count
- weather_type
- campaign_flag_text
- system_flag_text
- volume_multiplier
- conversion_multiplier
- timeout_multiplier
- retry_multiplier

### train / prediction
- stream + exogenous columns 포함
- 기존 target_risk_label 사용
- supervised 불가 시 rule_fallback 자동

## 실행 순서
1. 단일 또는 4월 매트릭스로 batch + stream 결과를 먼저 적재합니다.
2. 그 다음 아래 스크립트로 ML을 실행합니다.

```bash
DB_HOST=127.0.0.1 \
DB_PORT=3306 \
DB_USER=nethru \
DB_PASSWORD='nethru1234' \
DB_NAME=weblog \
bash deploy/run_ml_stream_pipeline_v1.sh 2026-04-01 2026-04-30 finance_bank
```

## 산출물
- ml_risk_model_stream_v7.joblib
- ml_risk_model_stream_report_v7.json
- ml_feature_importance_stream_v7.csv
- ml_prediction_result rows
