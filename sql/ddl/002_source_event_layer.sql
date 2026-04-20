-- =========================================================
-- 002_source_event_layer.sql
-- Canonical source event layer
-- =========================================================

CREATE TABLE IF NOT EXISTS event_log_raw (
  raw_event_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  dt DATE NOT NULL,
  ts DATETIME NOT NULL,

  ip VARCHAR(45) NOT NULL,
  method VARCHAR(10) NOT NULL,
  url_raw TEXT NOT NULL,
  url_full TEXT NOT NULL,
  url_norm TEXT NOT NULL,
  host VARCHAR(255) NULL,
  path VARCHAR(2048) NULL,
  query TEXT NULL,

  status INT NOT NULL,
  bytes BIGINT NULL,
  latency_ms INT NULL,
  ref TEXT NULL,
  ua TEXT NULL,
  kv_raw TEXT NULL,

  uid VARCHAR(128) NULL,
  pcid VARCHAR(128) NULL,
  sid VARCHAR(128) NULL,
  device_type VARCHAR(50) NULL,
  evt VARCHAR(50) NULL,
  accept_lang VARCHAR(100) NULL,
  cc VARCHAR(20) NULL,
  page_type VARCHAR(50) NULL,

  service_domain VARCHAR(50) NULL,
  funnel_stage VARCHAR(50) NULL,
  is_conversion TINYINT(1) NOT NULL DEFAULT 0,

  source_type VARCHAR(50) NOT NULL DEFAULT 'weblog',
  generator_run_id VARCHAR(100) NULL,
  scenario_id VARCHAR(100) NULL,

  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (raw_event_id),
  KEY idx_event_log_raw_dt_ts (dt, ts),
  KEY idx_event_log_raw_domain_dt (service_domain, dt),
  KEY idx_event_log_raw_evt_dt (evt, dt),
  KEY idx_event_log_raw_uid (uid),
  KEY idx_event_log_raw_pcid (pcid),
  KEY idx_event_log_raw_sid (sid),
  KEY idx_event_log_raw_page_type (page_type),
  KEY idx_event_log_raw_conversion (is_conversion)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
