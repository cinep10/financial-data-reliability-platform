CREATE TABLE IF NOT EXISTS scenario_experiment_run (
  scenario_run_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  profile_id VARCHAR(64) NOT NULL,
  scenario_name VARCHAR(100) NOT NULL,
  scenario_type VARCHAR(50) NOT NULL,
  dt_from DATE NOT NULL,
  dt_to DATE NOT NULL,
  parameters_json TEXT NULL,
  note VARCHAR(255) NULL,
  started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (scenario_run_id),
  KEY idx_scenario_run_profile_dt (profile_id, dt_from, dt_to)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS scenario_experiment_result_day (
  profile_id VARCHAR(64) NOT NULL,
  dt DATE NOT NULL,
  scenario_run_id BIGINT UNSIGNED NOT NULL,
  scenario_name VARCHAR(100) NOT NULL,
  scenario_type VARCHAR(50) NOT NULL,
  risk_score_v2 DECIMAL(20,6) NULL,
  risk_score_v3 DECIMAL(20,6) NULL,
  validation_warn_count INT NULL,
  validation_fail_count INT NULL,
  drift_alert_count INT NULL,
  drift_warn_count INT NULL,
  ml_feature_alert_count INT NULL,
  ml_feature_warn_count INT NULL,
  predicted_alert_prob DECIMAL(20,6) NULL,
  predicted_label VARCHAR(30) NULL,
  root_cause_top1 VARCHAR(255) NULL,
  traffic_page_view_count DECIMAL(20,6) NULL,
  missing_rate DECIMAL(20,6) NULL,
  note VARCHAR(255) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (profile_id, dt, scenario_run_id),
  KEY idx_scenario_result_profile_dt (profile_id, dt),
  KEY idx_scenario_result_run (scenario_run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
