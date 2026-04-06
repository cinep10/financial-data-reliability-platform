CREATE TABLE IF NOT EXISTS data_risk_root_cause_day (
  profile_id VARCHAR(64) NOT NULL,
  dt DATE NOT NULL,
  cause_rank INT NOT NULL,
  cause_type VARCHAR(50) NOT NULL,
  cause_code VARCHAR(100) NOT NULL,
  confidence DECIMAL(8,4) NOT NULL DEFAULT 0,
  driver_source VARCHAR(50) NOT NULL,
  related_metric VARCHAR(100) NULL,
  observed_value DECIMAL(20,6) NULL,
  baseline_value DECIMAL(20,6) NULL,
  detail VARCHAR(255) NULL,
  run_id VARCHAR(64) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (profile_id, dt, cause_rank)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS risk_signal_link_day (
  profile_id VARCHAR(64) NOT NULL,
  dt DATE NOT NULL,
  signal_group VARCHAR(50) NOT NULL,
  signal_name VARCHAR(100) NOT NULL,
  signal_count INT NOT NULL DEFAULT 0,
  weighted_contribution DECIMAL(20,6) NOT NULL DEFAULT 0,
  severity VARCHAR(20) NULL,
  note VARCHAR(255) NULL,
  run_id VARCHAR(64) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (profile_id, dt, signal_group, signal_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS scenario_experiment_run (
  experiment_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  profile_id VARCHAR(64) NOT NULL,
  scenario_name VARCHAR(100) NOT NULL,
  scenario_type VARCHAR(50) NOT NULL,
  dt_from DATE NOT NULL,
  dt_to DATE NOT NULL,
  shock_strength DECIMAL(8,4) NOT NULL DEFAULT 1.0,
  note VARCHAR(255) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (experiment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS scenario_experiment_result_day (
  experiment_id BIGINT UNSIGNED NOT NULL,
  profile_id VARCHAR(64) NOT NULL,
  scenario_name VARCHAR(100) NOT NULL,
  dt DATE NOT NULL,
  risk_score_v3 DECIMAL(20,6) NULL,
  risk_status_v3 VARCHAR(20) NULL,
  drift_alert_count INT NULL,
  drift_warn_count INT NULL,
  ml_feature_alert_count INT NULL,
  predicted_risk_status VARCHAR(20) NULL,
  pred_alert_prob DECIMAL(20,6) NULL,
  summary_note VARCHAR(255) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (experiment_id, dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
