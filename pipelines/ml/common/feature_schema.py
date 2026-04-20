STREAM_NUMERIC_FEATURES = [
    "missing_rate",
    "duplicate_ratio",
    "ordering_gap_score",
    "avg_event_delay_ms",
    "stream_risk_score",
    "delay_diff",
    "risk_diff",
    "missing_diff",
    "duplicate_diff",
    "delay_ma_7d",
    "risk_ma_7d",
    "delay_std_7d",
    "dayofweek",
    "month_no",
    "is_weekend",
    "missing_x_delay",
    "duplicate_x_ordering",
]

STREAM_CATEGORICAL_FEATURES = [
    "service_domain",
    "prev_issue",
]

STREAM_REQUIRED_COLUMNS = [
    "profile_id",
    "dt",
    *STREAM_NUMERIC_FEATURES,
    *STREAM_CATEGORICAL_FEATURES,
]

STREAM_LABEL_COLUMN = "label"
STREAM_RISK_TARGET_COLUMN = "target_risk_score"
