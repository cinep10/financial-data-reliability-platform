# Unified Scenario / Exogenous Layer Architecture

## 목적
기존 batch metric direct injection과 stream staging direct mutation을 하나의 외생변수 layer로 통합한다.

## 신규 아티팩트
테이블:
- scenario_plan
- exogenous_state_timeline
- scenario_injection_log
- scenario_adapter_result_log

스크립트:
- core/exogenous/scenario_provider.py
- core/exogenous/exogenous_state_builder.py
- pipelines/scenario/source_generator_adapter.py
- pipelines/scenario/stream_injection_adapter.py
- pipelines/scenario/batch_metric_adapter.py
- pipelines/scenario/performance_adapter.py
- pipelines/scenario/availability_adapter.py

실행기:
- deploy/run_unified_scenario_pipeline.sh

## 실행 예시
DB_HOST=127.0.0.1 \
DB_PORT=3306 \
DB_USER=nethru \
DB_PASSWORD='nethru1234' \
DB_NAME=weblog \
RUN_STREAM_ADAPTER=true \
RUN_BATCH_ADAPTER=true \
RUN_PRE_ML=true \
RUN_ML=false \
bash deploy/run_unified_scenario_pipeline.sh weather_drop 2026-04-07 2026-04-07 finance_bank medium
