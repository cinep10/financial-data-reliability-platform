-- =========================================================
-- 003_batch_ingestion_layer.sql
-- Collector / batch ingestion layer
-- =========================================================

CREATE TABLE IF NOT EXISTS stg_event_batch (
  batch_ingest_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_event_id BIGINT UNSIGNED NOT NULL,

  dt DATE NOT NULL,
  ts DATETIME NOT NULL,
  event_name VARCHAR(100) NOT NULL,

  service_domain VARCHAR(50) NULL,
  funnel_stage VARCHAR(50) NULL,
  is_conversion TINYINT(1) NOT NULL DEFAULT 0,

  uid VARCHAR(128) NULL,
  pcid VARCHAR(128) NULL,
  sid VARCHAR(128) NULL,
  device_type VARCHAR(50) NULL,
  page_type VARCHAR(50) NULL,

  status INT NULL,
  latency_ms INT NULL,

  batch_dt DATE NOT NULL,
  parse_status VARCHAR(20) DEFAULT 'success',
  load_status VARCHAR(20) DEFAULT 'success',
  replay_source VARCHAR(50) DEFAULT 'event_log_raw',
  collector_rule_version VARCHAR(50) NULL,
  anomaly_tag VARCHAR(50) NULL,

  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (batch_ingest_id),

  KEY idx_stg_event_batch_dt (dt),
  KEY idx_stg_event_batch_batch_dt (batch_dt),
  KEY idx_stg_event_batch_domain_dt (service_domain, dt),
  KEY idx_stg_event_batch_event_name (event_name),
  KEY idx_stg_event_batch_uid (uid),
  KEY idx_stg_event_batch_pcid (pcid),
  KEY idx_stg_event_batch_sid (sid),
  KEY idx_stg_event_batch_raw_event_id (raw_event_id),

  CONSTRAINT fk_stg_event_batch_raw_event
    FOREIGN KEY (raw_event_id) REFERENCES event_log_raw(raw_event_id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS batch_ingestion_quality_day (
  dt DATE NOT NULL,
  profile_id VARCHAR(64) NOT NULL DEFAULT 'default',

  raw_event_count BIGINT NOT NULL DEFAULT 0,
  page_event_candidate_count BIGINT NOT NULL DEFAULT 0,
  collected_event_count BIGINT NOT NULL DEFAULT 0,

  dropped_event_count BIGINT NOT NULL DEFAULT 0,
  duplicate_injected_count BIGINT NOT NULL DEFAULT 0,

  parse_success_count BIGINT NOT NULL DEFAULT 0,
  load_success_count BIGINT NOT NULL DEFAULT 0,

  parse_success_ratio DECIMAL(18,6) NOT NULL DEFAULT 0,
  load_success_ratio DECIMAL(18,6) NOT NULL DEFAULT 0,
  collection_yield_ratio DECIMAL(18,6) NOT NULL DEFAULT 0,

  quality_score DECIMAL(18,6) NULL,
  note VARCHAR(255) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (dt, profile_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
