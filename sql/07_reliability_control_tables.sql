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
  PRIMARY KEY (profile_id, dt, cause_rank),
  KEY idx_rca_profile_dt (profile_id, dt)
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
  PRIMARY KEY (profile_id, dt, signal_group, signal_name),
  KEY idx_risk_link_profile_dt (profile_id, dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS metric_time_anomaly_day (
  profile_id VARCHAR(64) NOT NULL,
  dt DATE NOT NULL,
  metric_name VARCHAR(100) NOT NULL,
  metric_group VARCHAR(50) NULL,
  source_layer VARCHAR(50) NULL,
  observed_value DECIMAL(20,6) NULL,
  rolling_avg_7d DECIMAL(20,6) NULL,
  rolling_std_7d DECIMAL(20,6) NULL,
  zscore_7d DECIMAL(20,6) NULL,
  anomaly_status VARCHAR(20) NOT NULL,
  severity VARCHAR(20) NULL,
  note VARCHAR(255) NULL,
  run_id VARCHAR(64) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (profile_id, dt, metric_name),
  KEY idx_time_anom_profile_dt (profile_id, dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS metric_correlation_anomaly_day (
  profile_id VARCHAR(64) NOT NULL,
  dt DATE NOT NULL,
  pair_name VARCHAR(150) NOT NULL,
  left_metric VARCHAR(100) NOT NULL,
  right_metric VARCHAR(100) NOT NULL,
  baseline_ratio DECIMAL(20,6) NULL,
  observed_ratio DECIMAL(20,6) NULL,
  ratio_diff DECIMAL(20,6) NULL,
  ratio_diff_pct DECIMAL(20,6) NULL,
  anomaly_status VARCHAR(20) NOT NULL,
  severity VARCHAR(20) NULL,
  note VARCHAR(255) NULL,
  run_id VARCHAR(64) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (profile_id, dt, pair_name),
  KEY idx_corr_anom_profile_dt (profile_id, dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
