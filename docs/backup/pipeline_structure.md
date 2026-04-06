# Analyzer + Validation + Drift + Risk 전체 파이프라인 구조 정리

## End-to-End Batch Pipeline

```mermaid
flowchart TD
    A[weblog-sim / raw log] --> B[parse_webserver_log_fast.py]
    B --> C[load_tsv_to_db_v2.py]
    C --> D[stg_webserver_log_hit]
    D --> E[collector_a_v2.py]
    E --> F[stg_wc_log_hit]

    F --> G[analyzer_b_v4.py]
    G --> H[metric_value_hh]
    G --> I[metric_value_day]
    G --> J[stg_ds_metric_hh]
    G --> K[stg_ds_metric_hh_wide]

    H --> L[validation_layer_runner_v2.py]
    I --> L
    L --> M[validation_result]
    L --> N[validation_summary_day]

    H --> O[metric_drift_analysis_db_v7.R]
    I --> O
    O --> P[metric_drift_result_r]
    O --> Q[metric_drift_result]

    N --> R[risk_score_runner_v2.py]
    P --> R
    R --> S[data_risk_score_day]

    S --> T[Grafana]
    N --> T
    P --> T
```

## 핵심 체크 포인트

### Analyzer
- `metric_value_hh` 날짜별 row count
- `collector_event_count >= page_view_count`
- `raw_event_count >= collector_event_count`

### Validation
- fail count
- warn count
- mapping quality rules

### Drift
- warn/alert count by date
- high drift metrics by hour

### Risk
- 일별 risk_score
- warn/alert 전환일
