# Reliability Extension Architecture

## Added objectives
1. Explain why the platform judged the day as risky.
2. Make the link between data change and risk score explicit.
3. Validate the platform under synthetic scenarios.

## Extended flow

Raw Log
-> Parser / Loader
-> stg_webserver_log_hit
-> collector / analyzer
-> metric_value_hh / metric_value_day
-> validation_result / validation_summary_day
-> metric_drift_result_r
-> data_risk_score_day_v3
-> root_cause_analyzer
-> data_risk_root_cause_day / risk_signal_link_day

In parallel:
metric_value_hh
-> ml_feature_drift_result
-> data_risk_score_day_v3
-> ml_feature_vector_day
-> ML model
-> ml_prediction_result
-> ml_feature_importance

## New operational tables
- data_risk_root_cause_day
- risk_signal_link_day
- scenario_experiment_run
- scenario_experiment_result_day

## What each new layer answers
- root cause: why is today risky?
- risk linkage: which signal family contributed most?
- scenario experiment: does the platform react as expected under campaign / holiday / weather / system issue conditions?
