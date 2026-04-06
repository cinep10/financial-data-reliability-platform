#!/usr/bin/env bash
set -euo pipefail

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASSWORD="${DB_PASSWORD:-nethru1234}"
DB_NAME="${DB_NAME:-weblog}"

RESET_MODE="${1:-results}"   # results | full | full-with-mapping

mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<SQL
SET FOREIGN_KEY_CHECKS = 0;

-- scenario
TRUNCATE TABLE scenario_experiment_result_day;
TRUNCATE TABLE scenario_metric_change_log;
TRUNCATE TABLE scenario_experiment_run;

-- ml
TRUNCATE TABLE ml_prediction_result;
TRUNCATE TABLE ml_feature_importance;
TRUNCATE TABLE ml_feature_vector_day;
TRUNCATE TABLE ml_feature_drift_result;

-- risk / interpretation / action
TRUNCATE TABLE risk_signal_link_day;
TRUNCATE TABLE data_reliability_action_day;
TRUNCATE TABLE data_risk_root_cause_day;
TRUNCATE TABLE data_risk_score_day_v3;
TRUNCATE TABLE data_risk_score_day_v2;
TRUNCATE TABLE data_risk_score_day;

-- anomaly
TRUNCATE TABLE metric_correlation_anomaly_day;
TRUNCATE TABLE metric_time_anomaly_day;
TRUNCATE TABLE metric_drift_result_r;
TRUNCATE TABLE metric_drift_result;

-- validation
TRUNCATE TABLE validation_summary_day;
TRUNCATE TABLE validation_result;
TRUNCATE TABLE metric_validation_result;

-- mapping support
TRUNCATE TABLE mapping_coverage_day;
SQL

if [ "$RESET_MODE" = "full" ] || [ "$RESET_MODE" = "full-with-mapping" ]; then
mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<SQL
TRUNCATE TABLE metric_value_day;
TRUNCATE TABLE metric_value_hh;

TRUNCATE TABLE stg_ds_metric_hh_wide;
TRUNCATE TABLE stg_ds_metric_hh;
TRUNCATE TABLE stg_ds_metric;
TRUNCATE TABLE stg_hit_common;
TRUNCATE TABLE stg_wc_log_hit;
TRUNCATE TABLE stg_webserver_log_hit;
SQL
fi

if [ "$RESET_MODE" = "full-with-mapping" ]; then
mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<SQL
TRUNCATE TABLE event_mapping;
SQL
fi

mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<SQL
SET FOREIGN_KEY_CHECKS = 1;
SQL

echo "[OK] reset mode: $RESET_MODE"
