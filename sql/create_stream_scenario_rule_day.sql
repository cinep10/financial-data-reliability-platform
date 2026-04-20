CREATE TABLE IF NOT EXISTS stream_scenario_rule_day (
  rule_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  profile_id VARCHAR(64) NOT NULL,
  dt DATE NOT NULL,
  hh TINYINT NOT NULL,
  scenario_name VARCHAR(100) NOT NULL,
  intensity VARCHAR(20) NULL,
  target_domains VARCHAR(255) NULL,
  missing_ratio DECIMAL(10,4) NOT NULL DEFAULT 0,
  duplicate_ratio DECIMAL(10,4) NOT NULL DEFAULT 0,
  delay_ms_p50 INT NOT NULL DEFAULT 0,
  delay_ms_p95 INT NOT NULL DEFAULT 0,
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (rule_id),
  KEY idx_profile_dt_hh (profile_id, dt, hh)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
