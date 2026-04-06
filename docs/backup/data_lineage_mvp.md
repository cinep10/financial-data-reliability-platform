# Data Lineage MVP

```mermaid
flowchart LR
    A[stg_wc_log_hit] --> B[metric_value_hh]
    A --> C[metric_value_day]
    B --> D[validation_result]
    B --> E[validation_summary_day]
    B --> F[metric_drift_result]
    B --> G[metric_drift_result_r]
    D --> H[data_risk_score_day]
    E --> H
    F --> H
    G --> H
    H --> I[Grafana Dashboard]
```

## 설명
- `stg_wc_log_hit`: raw/staging log source
- `metric_value_hh`, `metric_value_day`: semantic metric layer
- `validation_*`: logical consistency checks
- `metric_drift_result*`: statistical anomaly signals
- `data_risk_score_day`: operational control metric
- `Grafana`: observability surface
