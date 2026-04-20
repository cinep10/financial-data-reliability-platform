-- =========================================================
-- 005_metric_tables.sql
-- Metric tables for Analyzer B
-- =========================================================

CREATE TABLE IF NOT EXISTS metric_value_hh (
  profile_id VARCHAR(64) NOT NULL,
  dt DATE NOT NULL,
  hh TINYINT NOT NULL,
  metric_name VARCHAR(100) NOT NULL,
  metric_group VARCHAR(50) NOT NULL,
  source_layer VARCHAR(50) NOT NULL,
  metric_value DECIMAL(18,6) NOT NULL,
  numerator_value DECIMAL(18,6) NULL,
  denominator_value DECIMAL(18,6) NULL,
  run_id VARCHAR(64) NULL,
  note VARCHAR(255) NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (profile_id, dt, hh, metric_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS metric_value_day (
  profile_id VARCHAR(64) NOT NULL,
  dt DATE NOT NULL,
  metric_name VARCHAR(100) NOT NULL,
  metric_group VARCHAR(50) NOT NULL,
  source_layer VARCHAR(50) NOT NULL,
  metric_value DECIMAL(18,6) NOT NULL,
  numerator_value DECIMAL(18,6) NULL,
  denominator_value DECIMAL(18,6) NULL,
  run_id VARCHAR(64) NULL,
  note VARCHAR(255) NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (profile_id, dt, metric_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS stg_ds_metric_hh (
  profile_id VARCHAR(64) NOT NULL,
  dt DATE NOT NULL,
  hh TINYINT NOT NULL,
  metric_nm VARCHAR(100) NOT NULL,
  metric_val DECIMAL(18,6) NOT NULL,
  note VARCHAR(255) NULL,
  PRIMARY KEY (profile_id, dt, hh, metric_nm)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS stg_ds_metric_hh_wide (
  profile_id VARCHAR(64) NOT NULL,
  dt DATE NOT NULL,
  hh TINYINT NOT NULL,
  visit DECIMAL(18,6) NOT NULL DEFAULT 0,
  uv DECIMAL(18,6) NOT NULL DEFAULT 0,
  pageview DECIMAL(18,6) NOT NULL DEFAULT 0,
  note VARCHAR(255) NULL,
  PRIMARY KEY (profile_id, dt, hh)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
