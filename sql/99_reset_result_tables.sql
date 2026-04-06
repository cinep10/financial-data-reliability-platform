SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE scenario_experiment_result_day;
TRUNCATE TABLE scenario_experiment_run;

TRUNCATE TABLE ml_prediction_result;
TRUNCATE TABLE ml_feature_importance;
TRUNCATE TABLE ml_feature_vector_day;
TRUNCATE TABLE ml_feature_drift_result;

TRUNCATE TABLE risk_signal_link_day;
TRUNCATE TABLE data_risk_root_cause_day;
TRUNCATE TABLE data_risk_score_day_v3;
TRUNCATE TABLE data_risk_score_day_v2;
TRUNCATE TABLE data_risk_score_day;

TRUNCATE TABLE metric_correlation_anomaly_day;
TRUNCATE TABLE metric_time_anomaly_day;
TRUNCATE TABLE metric_drift_result_r;
TRUNCATE TABLE metric_drift_result;

TRUNCATE TABLE validation_summary_day;
TRUNCATE TABLE validation_result;
TRUNCATE TABLE metric_validation_result;

SET FOREIGN_KEY_CHECKS = 1;
